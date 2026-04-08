from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any


PROTECTED_IF_EXISTS_FIELDS = {
    "country_iso2",
    "nationality",
    "city",
    "language",
    "brevo_id",
}

NAME_FIELDS = {"first_name", "last_name"}
NEWER_FILE_FIELDS = {"phone", "latest_source", "latest_campaign"}
BOOLEAN_TRUE_WINS_FIELDS = {"is_buyer"}


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def normalize_text(value: Any) -> Any:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        clean = value.strip().lower()
        if clean in {"true", "1", "yes", "y", "on"}:
            return True
        if clean in {"false", "0", "no", "n", "off"}:
            return False
    return None


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            return None
    return None


def get_existing_record_timestamp(existing_record: dict | None) -> datetime | None:
    existing_record = existing_record or {}

    meta_info = existing_record.get("meta_info") or {}

    last_imported_at = parse_datetime(meta_info.get("last_imported_at"))
    if last_imported_at:
        return last_imported_at

    updated_at = parse_datetime(existing_record.get("updated_at"))
    if updated_at:
        return updated_at

    return parse_datetime(existing_record.get("created_at"))


def is_newer_import(existing_record: dict | None, imported_at: Any) -> bool:
    new_dt = parse_datetime(imported_at)
    if new_dt is None:
        return False

    existing_dt = get_existing_record_timestamp(existing_record)
    if existing_dt is None:
        return True

    return new_dt >= existing_dt


def split_tags(tags: Any) -> set[str]:
    if tags is None:
        return set()

    if isinstance(tags, list):
        values = tags
    else:
        values = str(tags).split(",")

    result = set()
    for tag in values:
        clean = str(tag).strip()
        if clean:
            result.add(clean)
    return result


def merge_tags(old_tags: Any, new_tags: Any) -> str | None:
    merged = split_tags(old_tags) | split_tags(new_tags)
    if not merged:
        return None
    return ",".join(sorted(merged))


def merge_name_field(old_value: Any, new_value: Any) -> Any:
    old_value = normalize_text(old_value)
    new_value = normalize_text(new_value)

    if is_empty(old_value):
        return new_value
    if is_empty(new_value):
        return old_value
    return old_value


def merge_fill_if_empty(old_value: Any, new_value: Any) -> Any:
    old_value = normalize_text(old_value)
    new_value = normalize_text(new_value)

    if is_empty(old_value) and not is_empty(new_value):
        return new_value
    return old_value


def merge_boolean_true_wins(old_value: Any, new_value: Any) -> bool | None:
    old_bool = parse_bool(old_value)
    new_bool = parse_bool(new_value)

    if old_bool is True or new_bool is True:
        return True
    if old_bool is False or new_bool is False:
        return False
    return None


def merge_status(old_value: Any, new_value: Any) -> Any:
    old_value = normalize_text(old_value)
    new_value = normalize_text(new_value)

    if not is_empty(old_value):
        return old_value
    return new_value


def merge_newer_file_field(
    old_value: Any,
    new_value: Any,
    existing_record: dict | None,
    imported_at: Any,
) -> Any:
    old_value = normalize_text(old_value)
    new_value = normalize_text(new_value)

    if is_empty(old_value):
        return new_value
    if is_empty(new_value):
        return old_value
    if is_newer_import(existing_record, imported_at):
        return new_value
    return old_value


def merge_raw_phones(old_meta_info: dict, new_meta_info: dict) -> list[str] | None:
    values = []
    for item in (old_meta_info or {}).get("raw_phones", []):
        if item is not None and str(item).strip():
            values.append(str(item).strip())
    for item in (new_meta_info or {}).get("raw_phones", []):
        if item is not None and str(item).strip():
            values.append(str(item).strip())

    deduped = []
    seen = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            deduped.append(value)

    return deduped or None


def merge_meta_info(
    old_meta_info: dict | None,
    new_meta_info: dict | None,
    filename: str,
    imported_at: Any,
    raw_row: dict | None,
) -> dict:
    meta_info = deepcopy(old_meta_info or {})
    new_meta_info = deepcopy(new_meta_info or {})

    raw_phones = merge_raw_phones(meta_info, new_meta_info)
    if raw_phones:
        meta_info["raw_phones"] = raw_phones
    else:
        meta_info.pop("raw_phones", None)

    meta_info["last_import_file"] = filename
    meta_info["last_imported_at"] = imported_at

    return meta_info


def merge_field(
    field: str,
    old_value: Any,
    new_value: Any,
    existing_record: dict | None,
    imported_at: Any,
) -> Any:
    if field in NAME_FIELDS:
        return merge_name_field(old_value, new_value)

    if field in PROTECTED_IF_EXISTS_FIELDS:
        return merge_fill_if_empty(old_value, new_value)

    if field in BOOLEAN_TRUE_WINS_FIELDS:
        return merge_boolean_true_wins(old_value, new_value)

    if field in NEWER_FILE_FIELDS:
        return merge_newer_file_field(old_value, new_value, existing_record, imported_at)

    if field == "status":
        return merge_status(old_value, new_value)

    if field == "email":
        return normalize_text(new_value) or normalize_text(old_value)

    return normalize_text(new_value) if not is_empty(new_value) else normalize_text(old_value)


def merge_records(
    existing_record: dict | None,
    new_record: dict,
    filename: str,
    imported_at: Any,
    raw_row: dict | None = None,
) -> dict:
    existing_record = existing_record or {}
    new_record = new_record or {}

    merged = dict(existing_record)

    all_fields = set(existing_record.keys()) | set(new_record.keys())
    all_fields.discard("meta_info")
    all_fields.discard("tags")
    all_fields.discard("updated_at")
    all_fields.discard("created_at")
    all_fields.discard("id")

    for field in all_fields:
        merged[field] = merge_field(
            field=field,
            old_value=existing_record.get(field),
            new_value=new_record.get(field),
            existing_record=existing_record,
            imported_at=imported_at,
        )

    merged["tags"] = merge_tags(
        existing_record.get("tags"),
        new_record.get("tags"),
    )

    merged["meta_info"] = merge_meta_info(
        old_meta_info=existing_record.get("meta_info"),
        new_meta_info=new_record.get("meta_info"),
        filename=filename,
        imported_at=imported_at,
        raw_row=raw_row or new_record,
    )

    return merged


def get_changed_fields(old_record: dict | None, new_record: dict, ignore_fields=None) -> dict:
    old_record = old_record or {}
    ignore_fields = set(ignore_fields or [])
    changed = {}

    for key, new_value in new_record.items():
        if key in ignore_fields:
            continue
        if old_record.get(key) != new_value:
            changed[key] = new_value

    return changed