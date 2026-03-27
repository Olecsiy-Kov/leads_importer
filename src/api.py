from pathlib import Path
import os
import shutil
import tempfile

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

    if not MAPPING_FILE.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Mapping file not found: {MAPPING_FILE}",
        )

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            file.file.seek(0)
            shutil.copyfileobj(file.file, tmp)
            temp_path = Path(tmp.name)

        records = parse_file_to_records(temp_path, MAPPING_FILE)
        rows_total = len(records)

        if rows_total > MAX_SYNC_ROWS:
            raise HTTPException(
                status_code=409,
                detail=f"File has {rows_total} rows. Async queue is not implemented yet.",
            )

        normalized = normalize_records(records)

        result = save_records(
            records=normalized,
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