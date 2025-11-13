#!/usr/bin/env python3
"""Telegram bot that reports marathon step statistics from Google Sheets."""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message

from generate_daily_breakdown import build_daily_totals
from generate_report import aggregate_by_team, collect_valid_entries
from generate_report_from_sheet import (
    DEFAULT_SERVICE_ACCOUNT,
    DEFAULT_SPREADSHEET_ID,
    fetch_sheet_rows,
    load_token,
)

CACHE_TTL_SECONDS = 300


@dataclass
class DataSnapshot:
    entries: Dict[tuple, tuple]
    team_totals: Dict[str, int]
    daily_totals: Dict[date, Dict[str, int]]
    fetched_at: datetime


class SheetDataService:
    def __init__(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        value_range: Optional[str],
        service_account: Path,
        cache_ttl: int,
    ) -> None:
        self._spreadsheet_id = spreadsheet_id
        self._sheet_name = sheet_name
        self._value_range = value_range
        self._service_account = service_account
        self._cache_ttl = cache_ttl
        self._lock = asyncio.Lock()
        self._snapshot: Optional[DataSnapshot] = None
        self._expires_at = datetime.min

    async def get_snapshot(self, force: bool = False) -> DataSnapshot:
        now = datetime.utcnow()
        async with self._lock:
            if not force and self._snapshot and now < self._expires_at:
                return self._snapshot

            snapshot = await asyncio.to_thread(self._load_snapshot)
            self._snapshot = snapshot
            self._expires_at = now + timedelta(seconds=self._cache_ttl)
            return snapshot

    def _load_snapshot(self) -> DataSnapshot:
        rows = fetch_sheet_rows(
            spreadsheet_id=self._spreadsheet_id,
            sheet=self._sheet_name,
            value_range=self._value_range,
            service_account=self._service_account,
        )
        entries = collect_valid_entries(rows)
        totals = aggregate_by_team(entries)
        daily_totals = build_daily_totals(entries)
        return DataSnapshot(
            entries=entries,
            team_totals=totals,
            daily_totals=daily_totals,
            fetched_at=datetime.utcnow(),
        )


def format_totals_table(totals: Dict[str, int], title: str) -> str:
    if not totals:
        return f"{title}\nNo data available."

    lines = [title]
    for idx, (team, steps) in enumerate(
        sorted(totals.items(), key=lambda item: item[1], reverse=True), start=1
    ):
        lines.append(f"{idx}. {team}: {steps}")
    return "\n".join(lines)


def format_daily_table(target_day: date, totals: Optional[Dict[str, int]]) -> str:
    label = target_day.strftime("%d.%m.%Y")
    if not totals:
        return f"{label}\nNo submissions for this day."

    lines = [label]
    for idx, (team, steps) in enumerate(
        sorted(totals.items(), key=lambda item: item[1], reverse=True), start=1
    ):
        lines.append(f"{idx}. {team}: {steps}")
    return "\n".join(lines)


def nearest_day_match(target: date, daily_totals: Dict[date, Dict[str, int]]) -> Optional[date]:
    if target in daily_totals:
        return target

    for day in sorted(daily_totals.keys()):
        if day.month == target.month and day.day == target.day:
            return day
    return None


def ensure_private(message: Message) -> bool:
    return message.chat.type == "private"


def build_help_text() -> str:
    return (
        "Hi! I can show marathon step stats.\n"
        "/leaderboard — total steps per team.\n"
        "/today — steps added today.\n"
        "/daybyday &lt;DD.MM&gt; — steps per team for a specific day.\n"
        "In groups only /report is available (also works in private chat)."
    )


async def handle_start(message: Message) -> None:
    await message.answer(build_help_text())


async def handle_leaderboard(message: Message, service: SheetDataService) -> None:
    if not ensure_private(message):
        await message.answer("Please use this command in a private chat with the bot.")
        return
    snapshot = await service.get_snapshot()
    text = format_totals_table(snapshot.team_totals, "Teams — total steps")
    await message.answer(text)


async def handle_today(
    message: Message,
    service: SheetDataService,
    current_date: Optional[date] = None,
) -> None:
    if not ensure_private(message):
        await message.answer("Please use this command in a private chat with the bot.")
        return
    today = current_date or date.today()
    snapshot = await service.get_snapshot()
    totals = snapshot.daily_totals.get(today)
    text = format_daily_table(today, totals)
    await message.answer(text)


async def handle_daybyday(
    message: Message,
    service: SheetDataService,
    argument: Optional[str],
    current_date: Optional[date] = None,
) -> None:
    if not ensure_private(message):
        await message.answer("Please use this command in a private chat with the bot.")
        return
    argument = (argument or "").strip()
    if not argument:
        await message.answer("Usage: /daybyday DD.MM")
        return
    today = current_date or date.today()
    try:
        target = datetime.strptime(argument, "%d.%m").date()
        target = target.replace(year=today.year)
    except ValueError:
        await message.answer("Cannot parse date. Use DD.MM format.")
        return

    snapshot = await service.get_snapshot()
    matched_day = nearest_day_match(target, snapshot.daily_totals)
    if not matched_day:
        await message.answer("No matching day found in the data set.")
        return

    totals = snapshot.daily_totals.get(matched_day)
    text = format_daily_table(matched_day, totals)
    await message.answer(text)


async def handle_report(
    message: Message,
    service: SheetDataService,
    current_date: Optional[date] = None,
) -> None:
    snapshot = await service.get_snapshot()
    totals_text = format_totals_table(snapshot.team_totals, "Teams — total steps")

    today = current_date or date.today()
    previous_day = today - timedelta(days=1)
    if previous_day not in snapshot.daily_totals:
        previous_candidates = [d for d in snapshot.daily_totals.keys() if d < today]
        previous_day = max(previous_candidates) if previous_candidates else None

    if previous_day:
        daily_text = format_daily_table(
            previous_day, snapshot.daily_totals.get(previous_day)
        )
        response = f"{totals_text}\n\nLast day increase:\n{daily_text}"
    else:
        response = (
            f"{totals_text}\n\nLast day increase:\nNo previous day data available."
        )

    await message.answer(response)


def create_router(service: SheetDataService):
    from aiogram import Router

    router = Router()

    @router.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        await handle_start(message)

    @router.message(Command("leaderboard"))
    async def cmd_leaderboard(message: Message) -> None:
        await handle_leaderboard(message, service)

    @router.message(Command("today"))
    async def cmd_today(message: Message) -> None:
        await handle_today(message, service)

    @router.message(Command("daybyday"))
    async def cmd_daybyday(message: Message, command: CommandObject) -> None:
        await handle_daybyday(message, service, command.args)

    @router.message(Command("report"))
    async def cmd_report(message: Message) -> None:
        await handle_report(message, service)

    return router


async def run_bot(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO)
    token = load_token(args.env_var)

    service = SheetDataService(
        spreadsheet_id=args.spreadsheet_id,
        sheet_name=args.sheet,
        value_range=args.range,
        service_account=args.service_account,
        cache_ttl=args.cache_ttl,
    )

    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(create_router(service))

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the marathon stats Telegram bot.")
    parser.add_argument("--spreadsheet-id", default=DEFAULT_SPREADSHEET_ID)
    parser.add_argument("--sheet", default="bot")
    parser.add_argument("--range")
    parser.add_argument(
        "--service-account",
        type=Path,
        default=DEFAULT_SERVICE_ACCOUNT,
    )
    parser.add_argument("--cache-ttl", type=int, default=CACHE_TTL_SECONDS)
    parser.add_argument("--env-var", default="BOT_TOKEN")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_bot(args))


if __name__ == "__main__":
    main()
