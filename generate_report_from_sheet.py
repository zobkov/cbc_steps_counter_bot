#!/usr/bin/env python3
"""Build step-count reports by reading the Google Sheet directly."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, Iterable, List

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from generate_daily_breakdown import build_daily_totals, emit_daily_report
from generate_report import (
    COLUMN_DAY,
    COLUMN_EMAIL,
    COLUMN_STEPS,
    COLUMN_TEAM,
    COLUMN_TIMESTAMP,
    ValidationError,
    aggregate_by_team,
    collect_valid_entries,
    emit_report,
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
DEFAULT_SPREADSHEET_ID = "1Qfvvfow5F4NPiw9UnC7j_IIbY-TJvlyQaNj0wO36KUo"
DEFAULT_SERVICE_ACCOUNT = Path(__file__).with_name("cbc2026-9d97f11665da.json")
REQUIRED_COLUMNS = (
    COLUMN_TIMESTAMP,
    COLUMN_DAY,
    COLUMN_TEAM,
    COLUMN_STEPS,
    COLUMN_EMAIL,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Google Sheet submissions, validate them with the marathon rules, "
            "and emit aggregated reports."
        )
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=DEFAULT_SPREADSHEET_ID,
        help="Google Spreadsheet ID (the long token from the sheet URL).",
    )
    parser.add_argument(
        "--sheet",
        default="bot",
        help="Worksheet/tab name to read from the spreadsheet.",
    )
    parser.add_argument(
        "--range",
        help="Optional A1 range override (defaults to '<sheet>!A:Z').",
    )
    parser.add_argument(
        "--service-account",
        type=Path,
        default=DEFAULT_SERVICE_ACCOUNT,
        help="Path to the service-account JSON key.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Optional path to write the per-team totals as CSV. If omitted, the "
            "summary prints to stdout."
        ),
    )
    parser.add_argument(
        "--daily-output",
        type=Path,
        help=(
            "Optional path to write the per-day breakdown as CSV. If omitted, the "
            "daily table is not produced."
        ),
    )
    parser.add_argument(
        "--env-var",
        default="BOT_TOKEN",
        help="Environment variable that stores the integration token.",
    )
    return parser.parse_args()


def load_token(env_var: str) -> str:
    try:
        from dotenv import load_dotenv as _load_dotenv
    except ImportError:
        _load_dotenv = None

    if _load_dotenv:
        _load_dotenv()

    token = os.getenv(env_var)
    if not token:
        raise SystemExit(
            f"Environment variable '{env_var}' is not set; update your .env file."
        )
    return token


def fetch_sheet_rows(
    spreadsheet_id: str,
    sheet: str,
    value_range: str | None,
    service_account: Path,
) -> List[Dict[str, str]]:
    if not service_account.exists():
        raise SystemExit(f"Service-account key not found: {service_account}")

    creds = Credentials.from_service_account_file(
        str(service_account), scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    range_name = value_range or f"{sheet}!A:Z"
    try:
        response = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
    finally:
        service.close()

    values: List[List[str]] = response.get("values", [])
    if not values:
        return []

    def _normalize(value: str) -> str:
        return " ".join(value.strip().split()).casefold()

    normalized_required = {_normalize(column) for column in REQUIRED_COLUMNS}
    header_row_idx = None
    header: List[str] | None = None
    for idx, raw in enumerate(values):
        cleaned = [cell.strip() for cell in raw]
        normalized = {_normalize(cell) for cell in cleaned}
        if normalized_required.issubset(normalized):
            header_row_idx = idx
            header = cleaned
            break

    if header is None or header_row_idx is None:
        raise ValidationError(
            "Worksheet is missing required columns even after scanning the sheet."
        )

    header_indexes = {_normalize(name): idx for idx, name in enumerate(header)}
    rows: List[Dict[str, str]] = []
    for raw in values[header_row_idx + 1 :]:
        row: Dict[str, str] = {}
        for column in header:
            idx = header_indexes.get(_normalize(column))
            if idx is None:
                continue
            row[column] = raw[idx].strip() if idx < len(raw) else ""
        rows.append(row)

    return rows


def build_entries(rows: Iterable[Dict[str, str]]):
    return collect_valid_entries(rows)


def main() -> None:
    args = parse_args()
    load_token(args.env_var)

    rows = fetch_sheet_rows(
        spreadsheet_id=args.spreadsheet_id,
        sheet=args.sheet,
        value_range=args.range,
        service_account=args.service_account,
    )

    entries = build_entries(rows)
    team_totals = aggregate_by_team(entries)
    emit_report(team_totals, args.output)

    if args.daily_output:
        daily_totals = build_daily_totals(entries)
        emit_daily_report(daily_totals, args.daily_output)


if __name__ == "__main__":
    main()
