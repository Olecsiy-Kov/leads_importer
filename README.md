# Leads Importer

Simple MVP for importing leads from CSV/XLSX files into PostgreSQL.

## What it does
- reads CSV and Excel files
- maps columns using YAML config
- normalizes lead data
- merges records by email
- saves data and import logs to DB


## Leads Importer

Simple MVP for importing leads from CSV/XLSX files into PostgreSQL.

## What it does
- reads CSV and Excel files
- maps columns using YAML config
- normalizes lead data
- merges records by email
- saves data and import logs to DB

Run locally
```bash
python src/cli.py --file test_leads_40_users.csv