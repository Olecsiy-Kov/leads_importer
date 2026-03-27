import pandas as pd
import csv
import chardet
import yaml
from pathlib import Path

SUPPORTED_EXCEL_EXTENSIONS = {".xlsx", ".xls", ".xlsm"}
SUPPORTED_CSV_EXTENSIONS = {".csv", ".txt", ".tsv"}


def detect_encoding(path: Path) -> str:
    with open(path, "rb") as f:
        detected = chardet.detect(f.read(10000)).get("encoding")
        print(f"Detected: Encoding {detected}")
        return detected or "utf-8"


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


def read_excel_file(path: Path, sheet_name=0) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except Exception as e:
        raise ValueError(f"Failed to read Excel file '{path}': {e}") from e


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


def read_file(path, sheet_name=0) -> pd.DataFrame:
    path = Path(path)

    if not path.exists():
        raise ValueError(f"File not found: {path}")

    suffix = path.suffix.lower()

    if suffix in SUPPORTED_EXCEL_EXTENSIONS:
        return read_excel_file(path, sheet_name=sheet_name)

    if suffix in SUPPORTED_CSV_EXTENSIONS:
        return read_csv_file(path)

    raise ValueError(f"Unsupported file extension: {suffix}")


def load_mappings(yml_path):
    with open(yml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_name(name):
    return str(name).strip().lower()


def match_column_to_standard(column_name, mappings):
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


def build_rename_map(df_columns, mappings):
    rename_map = {}

    for col in df_columns:
        standard_name = match_column_to_standard(col, mappings)
        if standard_name:
            rename_map[col] = standard_name

    return rename_map


def parse_file_to_records(file_path, mapping_file, sheet_name=0):
    df = read_file(file_path, sheet_name=sheet_name)
    print("Original columns:", df.columns.tolist())

    mappings = load_mappings(mapping_file)
    rename_map = build_rename_map(df.columns, mappings)

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