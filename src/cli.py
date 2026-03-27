from pathlib import Path
import argparse

from parser import parse_file_to_records
from normalizer import normalize_records
from db import create_table_if_not_exists, save_records


def main():
    # Parse CLI arguments
    arg_parser = argparse.ArgumentParser(description="Import leads from file")
    arg_parser.add_argument("--file", required=True, help="Path to input file")
    arg_parser.add_argument(
        "--mapping",
        default="D:/leads_importer/config/column_mappings.yaml",
        help="Path to mapping yaml file",
    )

    args = arg_parser.parse_args()

    input_file = Path(args.file)
    mapping_file = Path(args.mapping)

    # Create tables if they do not exist
    create_table_if_not_exists()

    # Parse raw records from source file
    records = parse_file_to_records(input_file, mapping_file)
    print(f"Parsed: {len(records)} rows")

    # Normalize parsed records
    normalized = normalize_records(records)
    print(f"Normalized: {len(normalized)} rows")

    print("\n--- SAMPLE ---")
    for r in normalized[:5]:
        print(r)

    # Save normalized records to database
    save_records(
        records=normalized,
        file_name=input_file.name,
    )

    print("Done")


if __name__ == "__main__":
    main()