"""Simple fan-out bus for market row updates."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import AsyncIterator, Iterable, Mapping, Set

from .config import settings

if settings.ui_debounce_seconds <= 0:
    QUEUE_SIZE = 512
else:
    QUEUE_SIZE = 128

class UpdateBus:
    """In-process pub/sub that keeps only the most recent update per subscriber."""

    def __init__(self) -> None:
        self._subscribers: Set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()

    async def publish(self, payload: Mapping[str, object]) -> None:
        message = dict(payload)
        async with self._lock:
            subscribers: Iterable[asyncio.Queue] = list(self._subscribers)
        for queue in subscribers:
            # Always deliver the most recent event; drop stale ones if the queue is full.
            with suppress(asyncio.QueueFull):
                queue.put_nowait(message)
                continue
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            with suppress(asyncio.QueueFull):
                queue.put_nowait(message)

    async def subscribe(self) -> AsyncIterator[dict]:
        queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        async with self._lock:
            self._subscribers.add(queue)
        try:
            while True:
                item = await queue.get()
                yield item
        except asyncio.CancelledError:
            raise
        finally:
            async with self._lock:
                self._subscribers.discard(queue)

    async def close(self) -> None:
        async with self._lock:
            subscribers = list(self._subscribers)
            self._subscribers.clear()
        for queue in subscribers:
            with suppress(asyncio.QueueFull):
                queue.put_nowait({"type": "closed"})


bus = UpdateBus()
funding_bus = UpdateBus()

__all__ = ["UpdateBus", "bus", "funding_bus"]
