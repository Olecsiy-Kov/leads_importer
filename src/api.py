from pathlib import Path
import os
import shutil
import tempfile
import zipfile

from fastapi import FastAPI, UploadFile, File, Header, HTTPException
from fastapi.responses import JSONResponse

from parser import parse_file_to_records
from normalizer import normalize_records
from db import (
    create_table_if_not_exists,
    save_records,
    fetch_import_logs,
    fetch_leads,
)

app = FastAPI(title="Leads Importer API")

API_KEY = os.getenv("LEADS_IMPORT_API_KEY", "secret-key")
BASE_DIR = Path(__file__).resolve().parent.parent
MAPPING_FILE = Path(
    os.getenv("COLUMN_MAPPING_FILE", BASE_DIR / "config" / "column_mappings.yaml")
)

MAX_SYNC_ROWS = 50_000
MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024
MAX_ZIP_SIZE_BYTES = 500 * 1024 * 1024
SUPPORTED_EXTENSIONS = {".csv", ".txt", ".tsv", ".xlsx", ".xls", ".xlsm"}


@app.on_event("startup")
def startup():
    create_table_if_not_exists()


def verify_api_key(x_api_key: str | None):
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


def get_upload_size(upload_file: UploadFile) -> int:
    current_pos = upload_file.file.tell()
    upload_file.file.seek(0, 2)
    size = upload_file.file.tell()
    upload_file.file.seek(current_pos)
    return size


def import_records_from_path(file_path: Path, file_name: str) -> dict:
    if not MAPPING_FILE.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Mapping file not found: {MAPPING_FILE}",
        )

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    if not file_path.is_file():
        raise HTTPException(status_code=400, detail=f"Not a file: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Unsupported file type")

    records = parse_file_to_records(file_path, MAPPING_FILE)
    rows_total = len(records)

    if rows_total > MAX_SYNC_ROWS:
        raise HTTPException(
            status_code=409,
            detail=f"File has {rows_total} rows. Async queue is not implemented yet.",
        )

    normalized = normalize_records(records)

    result = save_records(
        records=normalized,
        file_name=file_name,
    )

    return result


def is_safe_zip_member(member_name: str) -> bool:
    member_path = Path(member_name)

    if member_path.is_absolute():
        return False

    if ".." in member_path.parts:
        return False

    return True


def process_folder_import(root_dir: Path) -> dict:
    items = []
    total_rows = 0
    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    found_files = []

    for root, _, files in os.walk(root_dir):
        for file_name in files:
            file_path = Path(root) / file_name

            if file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                found_files.append(file_path)

    found_files.sort()

    for file_path in found_files:
        relative_name = str(file_path.relative_to(root_dir)).replace("\\", "/")

        try:
            result = import_records_from_path(
                file_path=file_path,
                file_name=relative_name,
            )

            total_rows += result["rows_total"]
            total_inserted += result["rows_inserted"]
            total_updated += result["rows_updated"]
            total_skipped += result["rows_skipped"]

            items.append({
                "file": relative_name,
                **result,
            })

        except HTTPException as e:
            items.append({
                "file": relative_name,
                "status": "failed",
                "error": e.detail,
            })
        except Exception as e:
            items.append({
                "file": relative_name,
                "status": "failed",
                "error": str(e),
            })

    return {
        "files_total": len(found_files),
        "rows_total": total_rows,
        "rows_inserted": total_inserted,
        "rows_updated": total_updated,
        "rows_skipped": total_skipped,
        "items": items,
    }


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


@app.post("/api/v1/import/upload")
def import_upload(
    file: UploadFile = File(...),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    verify_api_key(x_api_key)

    if not file.filename:
        raise HTTPException(status_code=400, detail="File name is missing")

    suffix = Path(file.filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Unsupported file type")

    if get_upload_size(file) > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=400,
            detail="File is too large. Maximum allowed size is 100 MB.",
        )

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            file.file.seek(0)
            shutil.copyfileobj(file.file, tmp)
            temp_path = Path(tmp.name)

        result = import_records_from_path(
            file_path=temp_path,
            file_name=file.filename,
        )

        return JSONResponse(status_code=200, content=result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {e}") from e
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)


@app.post("/api/v1/import/upload-zip")
def import_upload_zip(
    archive: UploadFile = File(...),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    verify_api_key(x_api_key)

    if not archive.filename:
        raise HTTPException(status_code=400, detail="Archive file name is missing")

    suffix = Path(archive.filename).suffix.lower()
    if suffix != ".zip":
        raise HTTPException(status_code=400, detail="Only .zip archives are supported")

    if get_upload_size(archive) > MAX_ZIP_SIZE_BYTES:
        raise HTTPException(
            status_code=400,
            detail="Archive is too large. Maximum allowed size is 500 MB.",
        )

    temp_dir = Path(tempfile.mkdtemp(prefix="leads_zip_"))
    archive_path = temp_dir / archive.filename
    extract_dir = temp_dir / "extracted"

    try:
        archive.file.seek(0)
        with open(archive_path, "wb") as out_file:
            shutil.copyfileobj(archive.file, out_file)

        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(archive_path, "r") as zf:
            members = zf.namelist()

            if not members:
                raise HTTPException(status_code=400, detail="ZIP archive is empty")

            for member in members:
                if not is_safe_zip_member(member):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Unsafe ZIP path detected: {member}",
                    )

            zf.extractall(extract_dir)

        result = process_folder_import(extract_dir)

        if result["files_total"] == 0:
            raise HTTPException(
                status_code=400,
                detail="No supported files found in ZIP archive",
            )

        return JSONResponse(status_code=200, content=result)

    except zipfile.BadZipFile as e:
        raise HTTPException(status_code=400, detail=f"Invalid ZIP archive: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ZIP import failed: {e}") from e
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@app.get("/api/v1/import/logs")
def get_import_logs(
    limit: int = 50,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    verify_api_key(x_api_key)
    return {"items": fetch_import_logs(limit=limit)}


@app.get("/api/v1/leads")
def get_leads(
    limit: int = 100,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    verify_api_key(x_api_key)
    return {"items": fetch_leads(limit=limit)}