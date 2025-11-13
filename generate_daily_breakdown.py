#!/usr/bin/env python3
"""Generate a day-by-day step breakdown for each team."""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Tuple

from generate_report import DATE_FMT, collect_valid_entries, read_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a day-by-day table of team step totals from a Google Forms "
            "CSV export, applying the same validation rules as the summary report."
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
            "Optional path to write the breakdown as CSV. If omitted, the table is "
            "printed to stdout."
        ),
    )
    return parser.parse_args()


def build_daily_totals(
    entries: Dict[Tuple[str, date], Tuple[datetime, str, int]]
) -> Dict[date, Dict[str, int]]:
    daily_totals: Dict[date, Dict[str, int]] = {}

    for (_, report_date), (_, team, steps) in entries.items():
        team_totals = daily_totals.setdefault(report_date, {})
        team_totals[team] = team_totals.get(team, 0) + steps

    return {day: dict(teams) for day, teams in daily_totals.items()}


def emit_daily_report(
    daily_totals: Dict[date, Dict[str, int]], output_path: Path | None
) -> None:
    teams = sorted({team for totals in daily_totals.values() for team in totals})

    header = ["date", *teams]
    rows = []
    for report_day in sorted(daily_totals):
        totals = daily_totals[report_day]
        row = [report_day.strftime(DATE_FMT)]
        row.extend(str(totals.get(team, 0)) for team in teams)
        rows.append(row)

    if output_path:
        with output_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)
            writer.writerows(rows)
        print(f"Daily breakdown saved to {output_path}")
    else:
        print(",".join(header))
        for row in rows:
            print(",".join(row))


def main() -> None:
    args = parse_args()

    entries = collect_valid_entries(read_rows(args.csv_path))
    daily_totals = build_daily_totals(entries)

    emit_daily_report(daily_totals, args.output)


if __name__ == "__main__":
    main()
