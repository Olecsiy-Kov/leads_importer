from pathlib import Path
import argparse
import os
import time

import logger_utils
from logger_utils import info, warn, error
from parser import parse_file_to_records
from normalizer import normalize_records
from db import create_table_if_not_exists, save_records


SUPPORTED_EXTENSIONS = {".csv", ".txt", ".tsv", ".xlsx", ".xls", ".xlsm"}


def import_file(input_file: Path, mapping_file: Path) -> dict:
    started = time.time()

    records = parse_file_to_records(input_file, mapping_file)
    info(f"Parsed: {len(records)} rows from {input_file}")

    normalized = normalize_records(records)
    info(f"Normalized: {len(normalized)} rows from {input_file}")

    result = save_records(
        records=normalized,
        file_name=input_file.name,
    )

    duration = round(time.time() - started, 2)

    info(
        f"Done: {input_file} | "
        f"inserted={result['rows_inserted']} "
        f"updated={result['rows_updated']} "
        f"skipped={result['rows_skipped']} "
        f"status={result['status']} "
        f"time={duration}s"
    )

    return result


def import_folder(folder_path: Path, mapping_file: Path):
    found_files = []

    for root, _, files in os.walk(folder_path):
        for file_name in files:
            file_path = Path(root) / file_name

            if file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                found_files.append(file_path)

    found_files.sort()

    info(f"Found {len(found_files)} files")

    processed_files = 0
    failed_files = 0
    total_rows = 0
    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    started = time.time()

    for index, file_path in enumerate(found_files, start=1):
        info(f"\n[{index}/{len(found_files)}] Processing {file_path}")

        try:
            result = import_file(file_path, mapping_file)

            processed_files += 1
            total_rows += result["rows_total"]
            total_inserted += result["rows_inserted"]
            total_updated += result["rows_updated"]
            total_skipped += result["rows_skipped"]

        except Exception as e:
            failed_files += 1
            error(f"Error in {file_path}: {e}")

    duration = round(time.time() - started, 2)

    info(f"Files processed: {processed_files}")
    info(f"Files failed: {failed_files}")
    info(f"Rows total: {total_rows}")
    info(f"Inserted: {total_inserted}")
    info(f"Updated: {total_updated}")
    info(f"Skipped: {total_skipped}")
    info(f"Duration: {duration}s")


def main():
    arg_parser = argparse.ArgumentParser(description="Import leads from file or folder")

    group = arg_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="Path to input file")
    group.add_argument("--folder", help="Path to folder with files")

    arg_parser.add_argument(
        "--mapping",
        default="config/column_mappings.yaml",
        help="Path to mapping yaml file",
    )

    arg_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logs",
    )

    args = arg_parser.parse_args()

    logger_utils.DEBUG = args.debug

    mapping_file = Path(args.mapping)

    if not mapping_file.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")

    create_table_if_not_exists()

    if args.file:
        input_file = Path(args.file)

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        import_file(input_file, mapping_file)

    if args.folder:
        folder_path = Path(args.folder)

        if not folder_path.exists():
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        if not folder_path.is_dir():
            raise NotADirectoryError(f"Not a folder: {folder_path}")

        import_folder(folder_path, mapping_file)


if __name__ == "__main__":
    main()