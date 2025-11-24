from __future__ import annotations

import os

import argparse
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from dotenv import load_dotenv as _load_dotenv

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart, Filter
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
DAILY_REPORT_CHAT_ID = -5052868617
ADMIN_IDS = [257026813]

logger = logging.getLogger("cbc_bot")


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
        now = datetime.now(UTC)
        async with self._lock:
            if not force and self._snapshot and now < self._expires_at:
                logger.info("Reusing cached snapshot (expires_in=%ss)", int((self._expires_at - now).total_seconds()))
                return self._snapshot

            snapshot = await asyncio.to_thread(self._load_snapshot)
            self._snapshot = snapshot
            self._expires_at = now + timedelta(seconds=self._cache_ttl)
            logger.info(
                "Refreshed snapshot: entries=%s teams=%s days=%s",
                len(snapshot.entries),
                len(snapshot.team_totals),
                len(snapshot.daily_totals),
            )
            return snapshot

    def _load_snapshot(self) -> DataSnapshot:
        logger.info(
            "Loading rows from sheet_id=%s sheet=%s range=%s",
            self._spreadsheet_id,
            self._sheet_name,
            self._value_range,
        )
        rows = fetch_sheet_rows(
            spreadsheet_id=self._spreadsheet_id,
            sheet=self._sheet_name,
            value_range=self._value_range,
            service_account=self._service_account,
        )
        entries = collect_valid_entries(rows)
        totals = aggregate_by_team(entries)
        daily_totals = build_daily_totals(entries)
        logger.info(
            "Loaded sheet data: raw_rows=%s valid_entries=%s",
            len(rows),
            len(entries),
        )
        return DataSnapshot(
            entries=entries,
            team_totals=totals,
            daily_totals=daily_totals,
            fetched_at=datetime.now(UTC),
        )


def format_totals_table(totals: Dict[str, int], title: str) -> str:
    if not totals:
        return f"{title}\nНет доступных данных по этому запросу."

    lines = [title]
    for idx, (team, steps) in enumerate(
        sorted(totals.items(), key=lambda item: item[1], reverse=True), start=1
    ):
        lines.append(f"{idx}. {team}: {steps}")
    return "\n".join(lines)


def format_daily_table(target_day: date, totals: Optional[Dict[str, int]]) -> str:
    label = target_day.strftime("%d.%m.%Y")
    if not totals:
        return f"{label}\nНет доступных данных по этому дню."

    lines = [label]
    for idx, (team, steps) in enumerate(
        sorted(totals.items(), key=lambda item: item[1], reverse=True), start=1
    ):
        lines.append(f"{idx}. {team}: +{steps}")
    return "\n".join(lines)


def nearest_day_match(target: date, daily_totals: Dict[date, Dict[str, int]]) -> Optional[date]:
    if target in daily_totals:
        return target

    for day in sorted(daily_totals.keys()):
        if day.month == target.month and day.day == target.day:
            return day
    return None


def latest_available_day(
    reference: date, daily_totals: Dict[date, Dict[str, int]]
) -> Optional[date]:
    candidates = [day for day in daily_totals if day <= reference]
    return max(candidates) if candidates else None


def ensure_private(message: Message) -> bool:
    if message.chat.type != "private":
        logger.info(
            "Rejecting command in non-private chat_id=%s chat_type=%s",
            message.chat.id,
            message.chat.type,
        )
        return False
    return True


def compose_report(snapshot: DataSnapshot, reference_date: date) -> tuple[str, Optional[date]]:
    totals_text = format_totals_table(snapshot.team_totals, "Команды — топ по шагам")

    previous_day = reference_date - timedelta(days=1)
    if previous_day not in snapshot.daily_totals:
        previous_candidates = [d for d in snapshot.daily_totals.keys() if d < reference_date]
        previous_day = max(previous_candidates) if previous_candidates else None

    if previous_day:
        daily_text = format_daily_table(
            previous_day, snapshot.daily_totals.get(previous_day)
        )
        response = f"{totals_text}\n\nПрирост за последний день:\n{daily_text}"
    else:
        response = (
            f"{totals_text}\n\nПрирост за последний день:\n"
            "Данные для предыдущего дня недоступны."
        )

    return response, previous_day


def build_help_text() -> str:
    return (
        "/leaderboard — общая таблица.\n"
        "/today — Посомтреть сколько было добавлено шагов за вчерашний день.\n"
        "/daybyday &lt;DD.MM&gt; — посмотреть статистику за определенный день по командам.\n\n"
        "В груп. чате только /report доступен (тут тоже работает). Отображает таблицу лидеров и стату за предыдущий день"
    )


async def handle_start(message: Message) -> None:
    logger.info("Handling /start chat_id=%s", message.chat.id)
    await message.answer(build_help_text())


async def handle_leaderboard(message: Message, service: SheetDataService) -> None:
    if not ensure_private(message):
        await message.answer("Эта команда доступна только в личном чате с ботом.")
        return
    logger.info("Handling /leaderboard chat_id=%s", message.chat.id)
    snapshot = await service.get_snapshot()
    text = format_totals_table(snapshot.team_totals, "Команды — топ по шагам")
    await message.answer(text)


async def handle_today(
    message: Message,
    service: SheetDataService,
    current_date: Optional[date] = None,
) -> None:
    if not ensure_private(message):
        await message.answer("Эта команда доступна только в личном чате с ботом.")
        return
    today = current_date or date.today()
    logger.info(
        "Handling /today chat_id=%s reference_date=%s",
        message.chat.id,
        today.isoformat(),
    )
    snapshot = await service.get_snapshot()
    target_day = latest_available_day(today, snapshot.daily_totals) or today
    totals = snapshot.daily_totals.get(target_day)
    logger.info(
        "Replying /today chat_id=%s target_day=%s entries=%s",
        message.chat.id,
        target_day.isoformat(),
        0 if totals is None else len(totals),
    )
    text = format_daily_table(target_day, totals)
    await message.answer(text)


async def handle_daybyday(
    message: Message,
    service: SheetDataService,
    argument: Optional[str],
    current_date: Optional[date] = None,
) -> None:
    if not ensure_private(message):
        await message.answer("Эта команда доступна только в личном чате с ботом.")
        return
    argument = (argument or "").strip()
    logger.info(
        "Handling /daybyday chat_id=%s argument=%r",
        message.chat.id,
        argument,
    )
    if not argument:
        await message.answer("Использование: /daybyday DD.MM")
        return
    today = current_date or date.today()
    try:
        target = datetime.strptime(argument, "%d.%m").date()
        target = target.replace(year=today.year)
    except ValueError:
        logger.info(
            "Failed to parse /daybyday argument chat_id=%s argument=%r",
            message.chat.id,
            argument,
        )
        await message.answer("Ошибка в дате. Используй DD.MM формат.")
        return

    snapshot = await service.get_snapshot()
    matched_day = nearest_day_match(target, snapshot.daily_totals)
    if not matched_day:
        logger.info(
            "No data for /daybyday chat_id=%s target=%s",
            message.chat.id,
            target.isoformat(),
        )
        await message.answer("Ничего не нашлось для этого дня.")
        return

    totals = snapshot.daily_totals.get(matched_day)
    logger.info(
        "Replying /daybyday chat_id=%s matched_day=%s entries=%s",
        message.chat.id,
        matched_day.isoformat(),
        0 if totals is None else len(totals),
    )
    text = format_daily_table(matched_day, totals)
    await message.answer(text)


async def handle_report(
    message: Message,
    service: SheetDataService,
    current_date: Optional[date] = None,
) -> None:
    logger.info(
        "Handling /report chat_id=%s chat_type=%s",
        message.chat.id,
        message.chat.type,
    )
    snapshot = await service.get_snapshot()
    today = current_date or date.today()
    response, previous_day = compose_report(snapshot, today)
    if previous_day:
        logger.info(
            "Replying /report chat_id=%s previous_day=%s",
            message.chat.id,
            previous_day.isoformat(),
        )
    else:
        logger.info(
            "Replying /report chat_id=%s previous_day=missing",
            message.chat.id,
        )
    await message.answer(response)


async def daily_report_loop(
    bot: Bot,
    service: SheetDataService,
    chat_id: int,
    hour: int = 18,
    minute: int = 0,
) -> None:
    logger.info(
        "Daily report scheduling enabled for chat_id=%s at %02d:%02d",
        chat_id,
        hour,
        minute,
    )
    try:
        while True:
            now = datetime.now()    
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_seconds = max(0, (target - now).total_seconds())
            logger.info(
                "Next scheduled report for chat_id=%s at %s (in %.0fs)",
                chat_id,
                target.isoformat(),
                wait_seconds,
            )
            try:
                await asyncio.sleep(wait_seconds)
            except asyncio.CancelledError:
                logger.info("Daily report loop cancelled while waiting for chat_id=%s", chat_id)
                raise

            try:
                snapshot = await service.get_snapshot(force=True)
                response, previous_day = compose_report(snapshot, target.date())
                await bot.send_message(chat_id, response)
                logger.info(
                    "Scheduled report sent chat_id=%s previous_day=%s",
                    chat_id,
                    previous_day.isoformat() if previous_day else "missing",
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Failed to send scheduled report to chat_id=%s", chat_id
                )
                await asyncio.sleep(10)
    except asyncio.CancelledError:
        logger.info("Daily report loop task cancelled for chat_id=%s", chat_id)
        raise


def _check_admin(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return False
    return True

class AdminFilter(Filter):
    """Admin filter. Returns bool when calle. Takes admin_ids list at init"""
    def __init__(self, admin_ids: list[int]):
        self.admin_ids = admin_ids

    async def __call__(self, message: Message) -> bool:
        is_admin = message.from_user.id in self.admin_ids
        logger.debug("User %s checked for admin. Result: %s", message.from_user.id, is_admin)
        return is_admin
    


def create_router(service: SheetDataService):
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


def _load_admin_mode() -> bool:
    env_var = "ADMIN_MODE"
    if _load_dotenv:
        _load_dotenv()

    admin_mode = os.getenv(env_var)
    return admin_mode

async def run_bot(args: argparse.Namespace) -> None:
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
    file_handler = TimedRotatingFileHandler(
        logs_dir / "bot.log",
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logging.getLogger().addHandler(file_handler)

    token = load_token(args.env_var)
    logger.info(
        "Starting bot with sheet_id=%s sheet=%s range=%s cache_ttl=%s",
        args.spreadsheet_id,
        args.sheet,
        args.range,
        args.cache_ttl,
    )

    ADMIN_MODE = _load_admin_mode()

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
    router: Router = create_router(service)

    logger.debug("ADMIN MODE is %s", ADMIN_MODE)

    if ADMIN_MODE:
        admin_filter = AdminFilter(ADMIN_IDS)
        router.message.filter(admin_filter)
        router.callback_query.filter(admin_filter)

    dp.include_router(router)

    scheduler_task = asyncio.create_task(
        daily_report_loop(bot, service, DAILY_REPORT_CHAT_ID)
    )
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduler_task
        logger.info("Shutting down bot")
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
