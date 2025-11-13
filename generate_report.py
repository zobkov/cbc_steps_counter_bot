#!/usr/bin/env python3
"""Aggregate valid step submissions per team from a Google Forms CSV export."""

from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Iterable, Tuple

# Column names from the exported Google Forms CSV.
COLUMN_TIMESTAMP = "Отметка времени"
COLUMN_DAY = "Отметка дня"
COLUMN_TEAM = "Название команды "
COLUMN_STEPS = "Количество шагов за день"
COLUMN_EMAIL = "Адрес электронной почты"

DATE_FMT = "%d.%m.%Y"
TIMESTAMP_FMT = "%d.%m.%Y %H:%M:%S"


class ValidationError(RuntimeError):
    """Raised when a row cannot be parsed as expected."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a team-level steps summary applying validation rules to a "
            "Google Forms CSV export."
        )
    )
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to the downloaded Google Forms CSV file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Optional path to write the aggregated report as CSV. If omitted, "
            "the report is printed to stdout."
        ),
    )
    parser.add_argument(
        "--env-var",
        default="BOT_TOKEN",
        help="Environment variable name that stores the integration token.",
    )
    return parser.parse_args()


def read_rows(csv_path: Path) -> Iterable[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        missing = {col for col in _required_columns() if col not in reader.fieldnames}
        if missing:
            raise ValidationError(
                "CSV file is missing required columns: " + ", ".join(sorted(missing))
            )
        for row in reader:
            yield row


def _required_columns() -> Tuple[str, ...]:
    return (
        COLUMN_TIMESTAMP,
        COLUMN_DAY,
        COLUMN_TEAM,
        COLUMN_STEPS,
        COLUMN_EMAIL,
    )


def parse_submission(row: Dict[str, str]) -> Tuple[datetime, date, str, int, str]:
    try:
        submitted_at = datetime.strptime(row[COLUMN_TIMESTAMP].strip(), TIMESTAMP_FMT)
        report_date = datetime.strptime(row[COLUMN_DAY].strip(), DATE_FMT).date()
        team = row[COLUMN_TEAM].strip()
        steps = int(row[COLUMN_STEPS].strip())
        email = row[COLUMN_EMAIL].strip().lower()
    except (KeyError, ValueError) as exc:
        raise ValidationError(f"Failed to parse row: {row!r}") from exc

    if not team or not email:
        raise ValidationError(f"Row missing team or email information: {row!r}")

    return submitted_at, report_date, team, steps, email


def is_valid(submitted_at: datetime, report_date: date) -> bool:
    submission_day = submitted_at.date()
    if report_date > submission_day:
        # Ignore future-dated reports relative to when they were submitted.
        return False

    delay = (submission_day - report_date).days
    if delay >= 3:
        # Ignore reports filed three or more days after the activity date.
        return False

    return True


def collect_valid_entries(rows: Iterable[Dict[str, str]]) -> Dict[Tuple[str, date], Tuple[datetime, str, int]]:
    valid_entries: Dict[Tuple[str, date], Tuple[datetime, str, int]] = {}
    for row in rows:
        try:
            submitted_at, report_date, team, steps, email = parse_submission(row)
        except ValidationError:
            continue

        if not is_valid(submitted_at, report_date):
            continue

        key = (email, report_date)
        current = valid_entries.get(key)
        if current is None or submitted_at < current[0]:
            valid_entries[key] = (submitted_at, team, steps)

    return valid_entries


def aggregate_by_team(entries: Dict[Tuple[str, date], Tuple[datetime, str, int]]) -> Dict[str, int]:
    team_totals: Dict[str, int] = defaultdict(int)
    for submitted_at, team, steps in entries.values():
        team_totals[team] += steps
    return team_totals


def emit_report(team_totals: Dict[str, int], output_path: Path | None) -> None:
    rows = sorted(team_totals.items())

    if output_path:
        with output_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["team", "total_steps"])
            writer.writerows(rows)
        print(f"Report saved to {output_path}")
    else:
        print("team,total_steps")
        for team, steps in rows:
            print(f"{team},{steps}")


def main() -> None:
    args = parse_args()

    entries = collect_valid_entries(read_rows(args.csv_path))
    team_totals = aggregate_by_team(entries)

    emit_report(team_totals, args.output)


if __name__ == "__main__":
    main()
