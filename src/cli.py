from pathlib import Path

from parser import parse_file_to_records
from normalizer import normalize_records
from db import create_table_if_not_exists, save_records

input_file = Path("D:/leads_importer/test_leads_40_users.csv")
mapping_file = Path("D:/leads_importer/config/column_mappings.yaml")


def main():
    create_table_if_not_exists()

    records = parse_file_to_records(input_file, mapping_file)
    print(f"Parsed: {len(records)} rows")

    normalized = normalize_records(records)
    print(f"Normalized: {len(normalized)} rows")

    print("\n--- SAMPLE ---")
    for r in normalized[:5]:
        print(r)

    save_records(
        records=normalized,
        file_name=input_file.name,
    )

    print("Done")


if __name__ == "__main__":
    main()