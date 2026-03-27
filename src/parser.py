import pandas as pd
import csv
import chardet
import yaml
from pathlib import Path
from typing import Any

SUPPORTED_EXCEL_EXTENSIONS = {".xlsx", ".xls", ".xlsm"}
SUPPORTED_CSV_EXTENSIONS = {".csv", ".txt", ".tsv"}


# Detect probable file encoding from raw bytes.
def detect_encoding(path: Path) -> str:
    with open(path, "rb") as f:
        detected = chardet.detect(f.read(10000)).get("encoding")
        print(f"Detected: Encoding {detected}")
        return detected or "utf-8"


# Detect CSV delimiter from file sample.
def detect_delimiter(path: Path, encoding: str, sample_size: int = 5000) -> str:
    with path.open("r", encoding=encoding, errors="ignore", newline="") as f:
        sample = f.read(sample_size)

    try:
        return csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
    except csv.Error:
        if path.suffix.lower() == ".tsv":
            return "\t"
        if ";" in sample and sample.count(";") >= sample.count(","):
            return ";"
        return ","


# Read Excel file into pandas DataFrame.
def read_excel_file(path: Path, sheet_name: int | str = 0) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except Exception as e:
        raise ValueError(f"Failed to read Excel file '{path}': {e}") from e


# Read CSV/TXT/TSV file with encoding fallback.
def read_csv_file(path: Path) -> pd.DataFrame:
    detected = detect_encoding(path)

    encodings_to_try = []
    for enc in [detected, "utf-8", "utf-8-sig", "windows-1251", "latin-1"]:
        if enc and enc.lower() not in {e.lower() for e in encodings_to_try}:
            encodings_to_try.append(enc)

    last_error = None

    for encoding in encodings_to_try:
        try:
            delimiter = detect_delimiter(path, encoding)
            print(f"Trying encoding={encoding}, delimiter={repr(delimiter)}")
            return pd.read_csv(path, sep=delimiter, encoding=encoding)
        except Exception as e:
            last_error = e
            print(f"Failed with encoding={encoding}: {e}")

    raise ValueError(
        f"Failed to read text file '{path}'. Tried encodings: {encodings_to_try}. Last error: {last_error}"
    )


# Read any supported file type and return DataFrame.
def read_file(path: str | Path, sheet_name: int | str = 0) -> pd.DataFrame:
    path = Path(path)

    if not path.exists():
        raise ValueError(f"File not found: {path}")

    suffix = path.suffix.lower()

    if suffix in SUPPORTED_EXCEL_EXTENSIONS:
        return read_excel_file(path, sheet_name=sheet_name)

    if suffix in SUPPORTED_CSV_EXTENSIONS:
        return read_csv_file(path)

    raise ValueError(f"Unsupported file extension: {suffix}")


# Load column mappings from YAML config.
def load_mappings(yml_path: str | Path) -> dict[str, list[str]]:
    with open(yml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# Normalize column name for comparison.
def normalize_name(name: Any) -> str:
    return str(name).strip().lower()


# Match source column name to standard field name.
def match_column_to_standard(column_name: str, mappings: dict[str, list[str]]) -> str | None:
    clean_col = normalize_name(column_name)

    for standard_name, variants in mappings.items():
        for variant in variants:
            if clean_col == normalize_name(variant):
                return standard_name

    for standard_name, variants in mappings.items():
        for variant in variants:
            normalized_variant = normalize_name(variant)
            if normalized_variant in clean_col or clean_col in normalized_variant:
                return standard_name

    return None


# Build rename map from original columns to standard fields.
def build_rename_map(df_columns: list[str], mappings: dict[str, list[str]]) -> dict[str, str]:
    rename_map = {}

    for col in df_columns:
        standard_name = match_column_to_standard(col, mappings)
        if standard_name:
            rename_map[col] = standard_name

    return rename_map


# Parse file into cleaned list of records.
def parse_file_to_records(
    file_path: str | Path,
    mapping_file: str | Path,
    sheet_name: int | str = 0
) -> list[dict[str, Any]]:
    df = read_file(file_path, sheet_name=sheet_name)
    print("Original columns:", df.columns.tolist())

    mappings = load_mappings(mapping_file)
    rename_map = build_rename_map(df.columns.tolist(), mappings)

    print("Rename map:", rename_map)

    unmapped = [col for col in df.columns if col not in rename_map]
    print("Unmapped columns:", unmapped)

    df = df.rename(columns=rename_map)
    df = df[[col for col in df.columns if col in mappings.keys()]]
    df = df.dropna(how="all")

    if "email" not in df.columns:
        raise ValueError("Column 'email' not found in file")

    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df = df[df["email"] != ""]
    df = df.drop_duplicates(subset=["email"], keep="last")

    return df.to_dict("records")