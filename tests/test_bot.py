import sys
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bot import (
    DataSnapshot,
    format_daily_table,
    format_totals_table,
    handle_daybyday,
    handle_leaderboard,
    handle_report,
    handle_today,
    nearest_day_match,
)


class DummyMessage:
    def __init__(self, chat_type: str = "private") -> None:
        self.chat = SimpleNamespace(type=chat_type)
        self.answers: list[str] = []

    async def answer(self, text: str) -> None:
        self.answers.append(text)


class FakeService:
    def __init__(self, snapshot: DataSnapshot) -> None:
        self.snapshot = snapshot
        self.calls = 0

    async def get_snapshot(self, force: bool = False) -> DataSnapshot:
        self.calls += 1
        return self.snapshot


@pytest.fixture
def sample_snapshot() -> DataSnapshot:
    return DataSnapshot(
        entries={},
        team_totals={"Team A": 200, "Team B": 150},
        daily_totals={
            date(2025, 11, 9): {"Team A": 120},
            date(2025, 11, 10): {"Team A": 80, "Team B": 150},
        },
        fetched_at=datetime.utcnow(),
    )


@pytest.mark.asyncio
async def test_leaderboard_private(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage("private")
    service = FakeService(sample_snapshot)

    await handle_leaderboard(message, service)

    assert message.answers
    assert "Team A" in message.answers[0]
    assert service.calls == 1


@pytest.mark.asyncio
async def test_leaderboard_group_rejected(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage("group")
    service = FakeService(sample_snapshot)

    await handle_leaderboard(message, service)

    assert message.answers == ["Please use this command in a private chat with the bot."]
    assert service.calls == 0


@pytest.mark.asyncio
async def test_today_uses_current_date(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage()
    service = FakeService(sample_snapshot)

    await handle_today(message, service, current_date=date(2025, 11, 10))

    assert message.answers
    assert "10.11.2025" in message.answers[0]
    assert "Team B" in message.answers[0]


@pytest.mark.asyncio
async def test_today_no_data(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage()
    service = FakeService(sample_snapshot)

    await handle_today(message, service, current_date=date(2025, 11, 11))

    assert message.answers == ["11.11.2025\nNo submissions for this day."]


@pytest.mark.asyncio
async def test_daybyday_argument_required(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage()
    service = FakeService(sample_snapshot)

    await handle_daybyday(message, service, argument="")

    assert message.answers == ["Usage: /daybyday DD.MM"]


@pytest.mark.asyncio
async def test_daybyday_invalid_date(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage()
    service = FakeService(sample_snapshot)

    await handle_daybyday(message, service, argument="not-a-date")

    assert message.answers == ["Cannot parse date. Use DD.MM format."]


@pytest.mark.asyncio
async def test_daybyday_matching_day(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage()
    service = FakeService(sample_snapshot)

    await handle_daybyday(message, service, argument="09.11", current_date=date(2025, 11, 15))

    assert message.answers
    assert "09.11.2025" in message.answers[0]
    assert "Team A" in message.answers[0]


@pytest.mark.asyncio
async def test_daybyday_no_match(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage()
    service = FakeService(sample_snapshot)

    await handle_daybyday(message, service, argument="08.11", current_date=date(2025, 11, 15))

    assert message.answers == ["No matching day found in the data set."]


@pytest.mark.asyncio
async def test_report_uses_previous_day(sample_snapshot: DataSnapshot) -> None:
    message = DummyMessage("group")
    service = FakeService(sample_snapshot)

    await handle_report(message, service, current_date=date(2025, 11, 11))

    assert message.answers
    assert "Teams — total steps" in message.answers[0]
    assert "10.11.2025" in message.answers[0]


@pytest.mark.asyncio
async def test_report_without_previous_day() -> None:
    snapshot = DataSnapshot(
        entries={},
        team_totals={"Team A": 50},
        daily_totals={},
        fetched_at=datetime.utcnow(),
    )
    message = DummyMessage()
    service = FakeService(snapshot)

    await handle_report(message, service, current_date=date(2025, 11, 10))

    assert "No previous day data available." in message.answers[0]


def test_nearest_day_match_same_day(sample_snapshot: DataSnapshot) -> None:
    target = date(2025, 11, 10)
    matched = nearest_day_match(target, sample_snapshot.daily_totals)
    assert matched == target


def test_nearest_day_match_same_month_day(sample_snapshot: DataSnapshot) -> None:
    target = date(2026, 11, 10)
    matched = nearest_day_match(target, sample_snapshot.daily_totals)
    assert matched == date(2025, 11, 10)


def test_nearest_day_match_missing(sample_snapshot: DataSnapshot) -> None:
    target = date(2025, 12, 1)
    matched = nearest_day_match(target, sample_snapshot.daily_totals)
    assert matched is None


def test_format_helpers() -> None:
    totals_text = format_totals_table({"Team": 100}, "Header")
    assert "Team" in totals_text

    daily_text = format_daily_table(date(2025, 11, 10), {"Team": 100})
    assert "10.11.2025" in daily_text

    empty_daily = format_daily_table(date(2025, 11, 12), None)
    assert "No submissions" in empty_daily
