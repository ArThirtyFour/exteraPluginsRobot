from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import monotonic

from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter


@dataclass
class BroadcastRun:
    kind: str
    total: int
    sent: int = 0
    failed: int = 0
    current: int = 0
    started_at: float = 0.0
    finished: bool = False


_run: BroadcastRun | None = None
_lock = asyncio.Lock()


def active_broadcast() -> BroadcastRun | None:
    return _run


async def claim_broadcast(kind: str, total: int) -> BroadcastRun | None:
    global _run
    async with _lock:
        if _run and not _run.finished:
            return None
        _run = BroadcastRun(kind=kind, total=total, started_at=monotonic())
        return _run


async def deliver_broadcast(bot, recipients: list[int], text: str, run: BroadcastRun) -> BroadcastRun:
    for user_id in recipients:
        run.current += 1
        try:
            await bot.send_message(
                user_id,
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            run.sent += 1
        except TelegramRetryAfter as exc:
            try:
                await asyncio.sleep(max(1, int(exc.retry_after)))
                await bot.send_message(
                    user_id,
                    text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                run.sent += 1
            except Exception:
                run.failed += 1
        except Exception:
            run.failed += 1
        await asyncio.sleep(0.04)
    run.finished = True
    return run
