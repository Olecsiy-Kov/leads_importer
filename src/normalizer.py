from email_validator import validate_email, EmailNotValidError
import phonenumbers
import pandas as pd
import pycountry
from phonenumbers import PhoneNumberFormat
from typing import Any


DEMONYM_BY_COUNTRY_CODE = {
    "UA": "Ukrainian",
    "PL": "Polish",
    "DE": "German",
    "FR": "French",
    "IT": "Italian",
    "ES": "Spanish",
    "GB": "British",
    "US": "American",
}


# Normalize and validate email address.
def normalize_email(email: Any) -> str | None:
    if email is None:
        return None

    email = str(email).strip().lower()
    if email == "":
        return None

    try:
        v = validate_email(email, check_deliverability=False)
        return v.email
    except EmailNotValidError:
        return None


# Convert country name to ISO2 country code.
def normalize_country_iso2(country_name: Any) -> str | None:
    try:
        return pycountry.countries.lookup(country_name).alpha_2
    except Exception:
        return None


# Normalize phone number to E.164 format or keep raw value in meta.
def normalize_phone(phone: Any, region: str | None = None) -> tuple[str | None, str | None]:
    if phone is None:
        return None, None

    raw_phone = str(phone).strip()
    if raw_phone == "":
        return None, None

    try:
        parsed = phonenumbers.parse(raw_phone, None)
        if phonenumbers.is_possible_number(parsed):
            return (
                phonenumbers.format_number(parsed, PhoneNumberFormat.E164),
                None,
            )
    except phonenumbers.NumberParseException:
        pass

    if region:
        try:
            parsed = phonenumbers.parse(raw_phone, region)
            if phonenumbers.is_possible_number(parsed):
                return (
                    phonenumbers.format_number(parsed, PhoneNumberFormat.E164),
                    None,
                )
        except phonenumbers.NumberParseException:
            pass

    return None, raw_phone


# Normalize simple text field and remove empty values.
def normalize_simple_text(value: Any) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text == "":
        return None

    return text


# Normalize nationality value using country demonym if possible.
def normalize_nationality(value: Any) -> str | None:
    value = normalize_simple_text(value)
    if value is None:
        return None

    country_code = normalize_country_iso2(value)
    if country_code:
        return DEMONYM_BY_COUNTRY_CODE.get(country_code, value)

    return value


# Normalize city and remove values that duplicate country.
def normalize_city(city: Any, country_iso2: str | None = None) -> str | None:
    city = normalize_simple_text(city)
    if city is None:
        return None

    if country_iso2 and city.strip().upper() == country_iso2.strip().upper():
        return None

    country_code_from_city = normalize_country_iso2(city)
    if country_code_from_city and country_code_from_city == country_iso2:
        return None

    return city


# Normalize one record into unified output format.
def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = {}
    meta_info = {}

    normalized["email"] = normalize_email(record.get("email"))
    normalized["country_iso2"] = normalize_country_iso2(record.get("country_iso2"))

    phone, raw_phone = normalize_phone(
        record.get("phone"),
        region=normalized["country_iso2"],
    )
    normalized["phone"] = phone

    if raw_phone:
        meta_info["raw_phones"] = [raw_phone]

    normalized["first_name"] = normalize_simple_text(record.get("first_name"))
    normalized["last_name"] = normalize_simple_text(record.get("last_name"))
    normalized["language"] = normalize_simple_text(record.get("language"))
    normalized["latest_source"] = normalize_simple_text(record.get("latest_source"))
    normalized["latest_campaign"] = normalize_simple_text(record.get("latest_campaign"))
    normalized["tags"] = normalize_simple_text(record.get("tags"))
    normalized["status"] = normalize_simple_text(record.get("status"))

    normalized["nationality"] = normalize_nationality(record.get("nationality"))

    normalized["city"] = normalize_city(
        record.get("city"),
        normalized["country_iso2"],
    )

    normalized["meta_info"] = meta_info or None

    return normalized


# Normalize list of records and skip invalid emails.
def normalize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []

    for record in records:
        normalized = normalize_record(record)

        if not normalized.get("email"):
            continue

        result.append(normalized)

    return result