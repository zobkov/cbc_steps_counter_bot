import sys
from datetime import date
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pytest

from generate_daily_breakdown import build_daily_totals, emit_daily_report
from generate_report import (
    ValidationError,
    aggregate_by_team,
    collect_valid_entries,
    read_rows,
)

DATA_DIR = Path(__file__).parent / "data"


def load_entries(file_name: str):
    csv_path = DATA_DIR / file_name
    return collect_valid_entries(read_rows(csv_path))


def test_collect_valid_entries_filters_invalid_and_duplicate_rows():
    entries = load_entries("basic.csv")

    expected_keys = {
        ("alpha@example.com", date(2025, 11, 9)),
        ("beta@example.com", date(2025, 11, 10)),
        ("gamma@example.com", date(2025, 11, 10)),
    }

    assert set(entries.keys()) == expected_keys

    alpha_submission = entries[("alpha@example.com", date(2025, 11, 9))]
    beta_submission = entries[("beta@example.com", date(2025, 11, 10))]
    gamma_submission = entries[("gamma@example.com", date(2025, 11, 10))]

    assert alpha_submission[1] == "Team Alpha"
    assert alpha_submission[2] == 100
    assert beta_submission[1] == "Team Beta"
    assert beta_submission[2] == 400
    assert gamma_submission[1] == "Team Gamma"
    assert gamma_submission[2] == 50


def test_aggregate_by_team_sums_steps_per_team():
    entries = load_entries("basic.csv")
    totals = aggregate_by_team(entries)

    assert totals == {
        "Team Alpha": 100,
        "Team Beta": 400,
        "Team Gamma": 50,
    }


def test_build_daily_totals_returns_matrix():
    entries = load_entries("basic.csv")
    daily_totals = build_daily_totals(entries)

    assert set(daily_totals.keys()) == {date(2025, 11, 9), date(2025, 11, 10)}
    assert daily_totals[date(2025, 11, 9)]["Team Alpha"] == 100
    assert daily_totals[date(2025, 11, 9)].get("Team Beta", 0) == 0
    assert daily_totals[date(2025, 11, 10)]["Team Beta"] == 400
    assert daily_totals[date(2025, 11, 10)]["Team Gamma"] == 50


def test_build_daily_totals_handles_multiple_days():
    entries = load_entries("multi_day.csv")
    daily_totals = build_daily_totals(entries)

    assert daily_totals[date(2025, 11, 12)]["Team Alpha"] == 1000
    assert daily_totals[date(2025, 11, 12)]["Team Beta"] == 500
    assert daily_totals[date(2025, 11, 11)]["Team Gamma"] == 150


def test_emit_daily_report_writes_csv(tmp_path):
    entries = load_entries("basic.csv")
    daily_totals = build_daily_totals(entries)

    output_path = tmp_path / "daily_report.csv"
    emit_daily_report(daily_totals, output_path)

    content = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert content[0] == "date,Team Alpha,Team Beta,Team Gamma"
    assert "09.11.2025,100,0,0" in content
    assert "10.11.2025,0,400,50" in content


def test_missing_required_columns_raises_validation_error(tmp_path):
    csv_path = tmp_path / "invalid.csv"
    csv_path.write_text(
        "Отметка времени,Название команды ,Количество шагов за день,Адрес электронной почты\n"
        "09.11.2025 16:47:51,Team Alpha,100,alpha@example.com\n",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        list(read_rows(csv_path))
