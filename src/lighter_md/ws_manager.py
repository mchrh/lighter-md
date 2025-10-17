"""Lifecycle manager for the WebSocket client and market store."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Optional, Set

from pydantic import ValidationError

from .bus import bus as default_bus
from .config import settings
from .dto import MarketStatsMsg, OrderBookMsg, MarketStatsBody, parse_ws_message
from .store import MarketStore
from .ws_client import run_ws_loop


SUBSCRIBE_ALL = {"type": "subscribe", "channel": "market_stats/all"}


class WebSocketManager:
    """Coordinates the WS client, store, and subscriptions."""

    def __init__(self, store: MarketStore, outbound_queue: Optional[asyncio.Queue] = None) -> None:
        self._store = store
        self._outbound: asyncio.Queue = outbound_queue or asyncio.Queue(maxsize=1024)
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._known_markets: Set[int] = set()
        self._logger = logging.getLogger("lighter_md.ws")

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(
            run_ws_loop(
                settings.ws_url,
                self._outbound,
                self._on_connect,
                self._on_message,
                self._stop_event,
                self._logger,
            )
        )

    async def stop(self) -> None:
        self._stop_event.set()
        with suppress(asyncio.QueueFull):
            self._outbound.put_nowait(None)
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        with suppress(asyncio.QueueEmpty):
            while True:
                self._outbound.get_nowait()
        await self._store.close()

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _on_connect(self):
        self._logger.info("Subscribing to %s markets", len(self._known_markets) + 1)
        messages = [{"type": "subscribe", "channel": "market_stats/all"}]
        messages.extend({"type": "subscribe", "channel": f"order_book/{mid}"} for mid in sorted(self._known_markets))
        return messages

    async def _on_message(self, payload: dict) -> None:
        if payload.get("type") == "update/market_stats":
            raw_stats = payload.get("market_stats")
            if isinstance(raw_stats, dict) and "market_id" not in raw_stats:
                await self._handle_market_stats_batch(payload.get("channel"), raw_stats)
                return
        try:
            message = parse_ws_message(payload)
        except (ValidationError, ValueError) as exc:
            self._logger.debug("Dropping invalid message: %s (%s)", payload, exc)
            return
        if isinstance(message, MarketStatsMsg):
            await self._handle_market_stats(message)
        elif isinstance(message, OrderBookMsg):
            await self._handle_order_book(message)

    async def _handle_market_stats(self, message: MarketStatsMsg) -> None:
        market_id = message.market_stats.market_id
        new_market = market_id not in self._known_markets
        await self._store.apply_market_stats(message)
        if new_market:
            self._known_markets.add(market_id)
            await self._enqueue({"type": "subscribe", "channel": f"order_book/{market_id}"})
            self._logger.info("Discovered market %s", market_id)

    async def _handle_order_book(self, message: OrderBookMsg) -> None:
        await self._store.apply_order_book(message)

    async def _enqueue(self, message: dict) -> None:
        await self._outbound.put(message)

    @property
    def store(self) -> MarketStore:
        return self._store

    async def _handle_market_stats_batch(self, channel: Optional[str], batch: dict) -> None:
        for value in batch.values():
            if not isinstance(value, dict) or "market_id" not in value:
                continue
            try:
                stats = MarketStatsBody.model_validate(value)
            except ValidationError as exc:
                self._logger.debug("Skipping invalid market stats entry %s: %s", value, exc)
                continue
            message = MarketStatsMsg(type="update/market_stats", channel=channel or "market_stats:all", market_stats=stats)
            await self._handle_market_stats(message)


def build_manager(bus=default_bus) -> WebSocketManager:
    store = MarketStore(bus)
    return WebSocketManager(store)
