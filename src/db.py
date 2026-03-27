import json
import os
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from merger import get_changed_fields, merge_records

DEFAULT_DSN = "dbname=leads_db user=postgres password=postgres host=127.0.0.1 port=5433"

LEAD_FIELDS = [
    "email",
    "phone",
    "country_iso2",
    "first_name",
    "last_name",
    "city",
    "language",
    "nationality",
    "is_buyer",
    "latest_source",
    "latest_campaign",
    "brevo_id",
    "tags",
    "status",
    "meta_info",
]


def get_connection() -> psycopg.Connection:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN") or DEFAULT_DSN
    return psycopg.connect(dsn, row_factory=dict_row)


def create_table_if_not_exists() -> None:
    queries = [
        """
        CREATE TABLE IF NOT EXISTS leads (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            country_iso2 TEXT,
            first_name TEXT,
            last_name TEXT,
            city TEXT,
            language TEXT,
            nationality TEXT,
            is_buyer BOOLEAN,
            latest_source TEXT,
            latest_campaign TEXT,
            brevo_id TEXT,
            tags TEXT,
            status TEXT,
            meta_info JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS import_logs (
            id SERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            imported_at TIMESTAMPTZ DEFAULT NOW(),
            rows_total INT,
            rows_inserted INT,
            rows_updated INT,
            rows_skipped INT,
            status TEXT,
            error_details JSONB
        )
        """,
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS nationality TEXT",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS is_buyer BOOLEAN",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS brevo_id TEXT",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS meta_info JSONB DEFAULT '{}'::jsonb",
        "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS filename TEXT",
        "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS imported_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS rows_total INT",
        "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS rows_inserted INT",
        "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS rows_updated INT",
        "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS rows_skipped INT",
        "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS status TEXT",
        "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS error_details JSONB",
        "CREATE INDEX IF NOT EXISTS idx_leads_email ON leads (email)",
        "CREATE INDEX IF NOT EXISTS idx_leads_updated_at ON leads (updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_import_logs_imported_at ON import_logs (imported_at)",
        "CREATE INDEX IF NOT EXISTS idx_leads_meta_info_gin ON leads USING GIN (meta_info)",
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for query in queries:
                cur.execute(query)
        conn.commit()


def _prepare_record(record: dict[str, Any]) -> dict[str, Any]:
    data: dict[str, Any] = {}

    for field in LEAD_FIELDS:
        data[field] = record.get(field)

    if isinstance(data.get("tags"), list):
        clean_tags = []
        for tag in data["tags"]:
            tag = str(tag).strip()
            if tag and tag not in clean_tags:
                clean_tags.append(tag)
        data["tags"] = ",".join(clean_tags) if clean_tags else None

    if data.get("meta_info") is None:
        data["meta_info"] = {}

    data["meta_info"] = json.dumps(data["meta_info"])
    return data


def get_existing_record(cur: psycopg.Cursor, email: str) -> dict[str, Any] | None:
    cur.execute("SELECT * FROM leads WHERE email = %s", (email,))
    return cur.fetchone()


def insert_record(cur: psycopg.Cursor, record: dict[str, Any]) -> None:
    data = _prepare_record(record)
    cur.execute(
        """
        INSERT INTO leads (
            email, phone, country_iso2, first_name, last_name, city, language,
            nationality, is_buyer, latest_source, latest_campaign, brevo_id,
            tags, status, meta_info, created_at, updated_at
        )
        VALUES (
            %(email)s, %(phone)s, %(country_iso2)s, %(first_name)s, %(last_name)s,
            %(city)s, %(language)s, %(nationality)s, %(is_buyer)s,
            %(latest_source)s, %(latest_campaign)s, %(brevo_id)s,
            %(tags)s, %(status)s, %(meta_info)s::jsonb, NOW(), NOW()
        )
        """,
        data,
    )


def update_record(cur: psycopg.Cursor, email: str, changed_fields: dict[str, Any]) -> None:
    if not changed_fields:
        return

    data = _prepare_record(changed_fields)
    data["email"] = email

    parts = []
    for field in changed_fields:
        if field == "email":
            continue
        if field == "meta_info":
            parts.append("meta_info = %(meta_info)s::jsonb")
        else:
            parts.append(f"{field} = %({field})s")

    parts.append("updated_at = NOW()")

    cur.execute(
        f"UPDATE leads SET {', '.join(parts)} WHERE email = %(email)s",
        data,
    )


def insert_import_log(
    cur: psycopg.Cursor,
    filename: str,
    rows_total: int,
    rows_inserted: int,
    rows_updated: int,
    rows_skipped: int,
    status: str,
    error_details: Any = None,
) -> int:
    cur.execute(
        """
        INSERT INTO import_logs (
            filename, imported_at, rows_total, rows_inserted,
            rows_updated, rows_skipped, status, error_details
        )
        VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s::jsonb)
        RETURNING id
        """,
        (
            filename,
            rows_total,
            rows_inserted,
            rows_updated,
            rows_skipped,
            status,
            json.dumps(error_details) if error_details is not None else None,
        ),
    )
    return cur.fetchone()["id"]


def fetch_leads(limit: int = 100) -> list[dict[str, Any]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM leads
                ORDER BY updated_at DESC, created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cur.fetchall())


def fetch_import_logs(limit: int = 50) -> list[dict[str, Any]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM import_logs
                ORDER BY imported_at DESC, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cur.fetchall())


def save_records(records: list[dict[str, Any]], file_name: str) -> dict[str, Any]:
    inserted = 0
    updated = 0
    skipped = 0
    errors: list[dict[str, Any]] = []

    imported_at = datetime.now(timezone.utc).isoformat()

    with get_connection() as conn:
        with conn.cursor() as cur:
            for record in records:
                email = record.get("email")

                if not email:
                    skipped += 1
                    errors.append({"error": "missing email", "record": record})
                    continue

                try:
                    existing = get_existing_record(cur, email)

                    # ВАЖЛИВО: merge робимо і для existing, і для нового запису,
                    # щоб meta_info.import_history завжди заповнювався.
                    merged = merge_records(
                        existing_record=existing,
                        new_record=record,
                        filename=file_name,
                        imported_at=imported_at,
                        raw_row=record,
                    )

                    if existing:
                        changed_fields = get_changed_fields(existing, merged)

                        if changed_fields:
                            update_record(cur, email, changed_fields)
                            updated += 1
                    else:
                        insert_record(cur, merged)
                        inserted += 1

                except Exception as exc:
                    skipped += 1
                    errors.append(
                        {
                            "email": email,
                            "error": str(exc),
                        }
                    )

            status = "success"
            if errors and (inserted > 0 or updated > 0):
                status = "partial"
            elif errors and inserted == 0 and updated == 0:
                status = "failed"

            import_log_id = insert_import_log(
                cur=cur,
                filename=file_name,
                rows_total=len(records),
                rows_inserted=inserted,
                rows_updated=updated,
                rows_skipped=skipped,
                status=status,
                error_details=errors if errors else None,
            )

        conn.commit()

    return {
        "import_id": import_log_id,
        "rows_total": len(records),
        "rows_inserted": inserted,
        "rows_updated": updated,
        "rows_skipped": skipped,
        "status": status,
    }