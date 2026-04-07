import csv
from pathlib import Path
from typing import Any

import chardet
import pandas as pd
import yaml

from logger_utils import debug


SUPPORTED_EXCEL_EXTENSIONS = {".xlsx", ".xls", ".xlsm"}
SUPPORTED_CSV_EXTENSIONS = {".csv", ".txt", ".tsv"}


def detect_encoding(path: Path) -> str:
    with open(path, "rb") as f:
        detected = chardet.detect(f.read(10000)).get("encoding")

    encoding = detected or "utf-8"
    debug(f"Detected encoding for {path.name}: {encoding}")
    return encoding


def detect_delimiter(path: Path, encoding: str, sample_size: int = 5000) -> str:
    with path.open("r", encoding=encoding, errors="ignore", newline="") as f:
        sample = f.read(sample_size)

    try:
        delimiter = csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
        debug(f"Detected delimiter for {path.name}: {repr(delimiter)}")
        return delimiter
    except csv.Error:
        if path.suffix.lower() == ".tsv":
            return "\t"

        if ";" in sample and sample.count(";") >= sample.count(","):
            return ";"

        return ","


def read_excel_file(path: Path, sheet_name: int | str = 0) -> pd.DataFrame:
    try:
        raw_df = pd.read_excel(path, sheet_name=sheet_name, header=None)

        header_row = None

        for i, row in raw_df.iterrows():
            row_values = [str(value).strip().lower() for value in row.values]

            if any("email" in value for value in row_values):
                header_row = i
                break

        if header_row is None:
            raise ValueError(f"Could not detect header row in Excel file: {path}")

        debug(f"Detected header row at index {header_row} in {path.name}")

        return pd.read_excel(path, sheet_name=sheet_name, header=header_row)

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
            debug(f"Trying {path.name} with encoding={encoding}, delimiter={repr(delimiter)}")
            return pd.read_csv(path, sep=delimiter, encoding=encoding)
        except Exception as e:
            last_error = e
            debug(f"Failed reading {path.name} with encoding={encoding}: {e}")

    raise ValueError(
        f"Failed to read text file '{path}'. Tried encodings: {encodings_to_try}. Last error: {last_error}"
    )


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


def load_mappings(yml_path: str | Path) -> dict[str, list[str]]:
    with open(yml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_name(name: Any) -> str:
    return str(name).strip().lower()


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


def build_rename_map(df_columns: list[str], mappings: dict[str, list[str]]) -> dict[str, str]:
    rename_map = {}

    for col in df_columns:
        standard_name = match_column_to_standard(col, mappings)
        if standard_name and standard_name not in rename_map.values():
            rename_map[col] = standard_name

    return rename_map


def ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns = []
    seen = {}

    for col in df.columns:
        col_str = str(col)
        if col_str not in seen:
            seen[col_str] = 0
            columns.append(col_str)
        else:
            seen[col_str] += 1
            columns.append(f"{col_str}__dup_{seen[col_str]}")

    df.columns = columns
    return df


def parse_file_to_records(
    file_path: str | Path,
    mapping_file: str | Path,
    sheet_name: int | str = 0,
) -> list[dict[str, Any]]:
    df = read_file(file_path, sheet_name=sheet_name)
    df = ensure_unique_columns(df)

    debug(f"Original columns: {df.columns.tolist()}")

    mappings = load_mappings(mapping_file)
    rename_map = build_rename_map(df.columns.tolist(), mappings)

    debug(f"Rename map: {rename_map}")

    unmapped = [col for col in df.columns if col not in rename_map]
    debug(f"Unmapped columns: {unmapped}")

    df = df.rename(columns=rename_map)
    df = df[[col for col in df.columns if col in mappings.keys()]]
    df = df.dropna(how="all")

    if "email" not in df.columns:
        raise ValueError("Column 'email' not found in file")

    email_series = df["email"]

    if isinstance(email_series, pd.DataFrame):
        email_series = email_series.iloc[:, 0]

    df["email"] = email_series.astype(str).str.strip().str.lower()
    df = df[df["email"] != ""]
    df = df.drop_duplicates(subset=["email"], keep="last")

    return df.to_dict("records")