"""State store for per-market data."""

from __future__ import annotations

import asyncio
import json
import math
import time
from pathlib import Path
import re
from typing import Dict, List, Optional, Sequence, Set, Tuple

from pydantic import BaseModel, ConfigDict

from .bus import UpdateBus
from .config import settings
from .dto import MarketStatsBody, MarketStatsMsg, OrderBookMsg, OrderLevel


def _now_ms() -> int:
    return int(time.time() * 1000)


class MarketRow(BaseModel):
    market_id: int
    symbol: Optional[str] = None
    best_bid_price: Optional[float] = None
    best_bid_size: Optional[float] = None
    best_ask_price: Optional[float] = None
    best_ask_size: Optional[float] = None
    last_price: Optional[float] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    mid_price: Optional[float] = None
    daily_volume: Optional[float] = None
    funding_rate: Optional[float] = None
    open_interest: Optional[float] = None
    basis: Optional[float] = None
    markout: Optional[float] = None
    spread: Optional[float] = None
    updated_ms: int

    model_config = ConfigDict(frozen=True)

    def for_wire(self) -> dict:
        data = self.model_dump()
        if data.get("symbol") is None:
            data["symbol"] = f"MKT-{self.market_id}"
        return data


class MarketStore:
    """Mutable in-memory store with debounced publishes."""

    def __init__(self, bus: UpdateBus, metadata_path: Optional[str] = None) -> None:
        self._bus = bus
        self._rows: Dict[int, MarketRow] = {}
        self._lock = asyncio.Lock()
        self._debounce = max(settings.ui_debounce_seconds, 0.05)
        self._last_publish: Dict[int, float] = {}
        self._pending: Dict[int, Tuple[MarketRow, Set[str]]] = {}
        self._flush_tasks: Dict[int, asyncio.Task] = {}
        self._metadata = self._load_metadata(metadata_path or settings.metadata_path)

    @staticmethod
    def _load_metadata(path: Optional[str]) -> Dict[int, str]:
        if not path:
            return {}
        file_path = Path(path)
        if not file_path.exists():
            return {}
        try:
            raw = json.loads(file_path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
        result: Dict[int, str] = {}
        for key, value in raw.items():
            try:
                market_id = int(key)
            except (ValueError, TypeError):
                continue
            if isinstance(value, str) and value:
                result[market_id] = value
        return result

    async def apply_market_stats(self, msg: MarketStatsMsg) -> Optional[MarketRow]:
        stats = msg.market_stats
        async with self._lock:
            row = self._rows.get(stats.market_id)
            existing = row.model_dump() if row else self._fresh_row(stats.market_id)
            changed_fields = self._apply_stats(existing, stats)
            if row is None:
                changed_fields.update(existing.keys())
            if not changed_fields:
                return None
            existing["updated_ms"] = _now_ms()
            changed_fields.add("updated_ms")
            new_row = MarketRow(**existing)
            self._rows[stats.market_id] = new_row
        await self._schedule_publish(new_row, changed_fields)
        return new_row

    async def apply_order_book(self, msg: OrderBookMsg) -> Optional[MarketRow]:
        market_id = self._extract_market_id(msg.channel)
        if market_id is None:
            return None
        order_book = msg.order_book
        async with self._lock:
            row = self._rows.get(market_id)
            existing = row.model_dump() if row else self._fresh_row(market_id)
            changed_fields = self._apply_order_book(existing, order_book.asks, order_book.bids)
            if row is None:
                changed_fields.update(existing.keys())
            if not changed_fields:
                return None
            existing["updated_ms"] = _now_ms()
            changed_fields.add("updated_ms")
            new_row = MarketRow(**existing)
            self._rows[market_id] = new_row
        await self._schedule_publish(new_row, changed_fields)
        return new_row

    async def snapshot(self) -> List[dict]:
        async with self._lock:
            rows = sorted(self._rows.values(), key=self._sort_key)
            return [row.for_wire() for row in rows]

    async def market_ids(self) -> List[int]:
        async with self._lock:
            return list(self._rows.keys())

    async def rows(self) -> List[MarketRow]:
        async with self._lock:
            return sorted(self._rows.values(), key=self._sort_key)

    def _fresh_row(self, market_id: int) -> Dict[str, Optional[float]]:
        return {
            "market_id": market_id,
            "symbol": self._metadata.get(market_id),
            "best_bid_price": None,
            "best_bid_size": None,
            "best_ask_price": None,
            "best_ask_size": None,
            "last_price": None,
            "mark_price": None,
            "index_price": None,
            "mid_price": None,
            "daily_volume": None,
            "funding_rate": None,
            "open_interest": None,
            "basis": None,
            "markout": None,
            "spread": None,
            "updated_ms": _now_ms(),
        }

    @staticmethod
    def _apply_stats(target: Dict[str, Optional[float]], stats: MarketStatsBody) -> Set[str]:
        changed: Set[str] = set()
        MarketStore._update_if_not_none(target, "last_price", stats.last_trade_price, changed)
        MarketStore._update_if_not_none(target, "mark_price", stats.mark_price, changed)
        MarketStore._update_if_not_none(target, "index_price", stats.index_price, changed)
        MarketStore._update_if_not_none(target, "open_interest", stats.open_interest, changed)
        funding = stats.effective_funding_rate
        MarketStore._update_if_not_none(target, "funding_rate", funding, changed)
        volume = stats.effective_daily_volume
        MarketStore._update_if_not_none(target, "daily_volume", volume, changed)
        basis = MarketStore._calc_basis(target)
        MarketStore._assign_optional(target, "basis", basis, changed)
        markout = MarketStore._calc_markout(target)
        MarketStore._assign_optional(target, "markout", markout, changed)
        return changed

    _CHANNEL_ID_RE = re.compile(r"(\d+)$")

    @classmethod
    def _extract_market_id(cls, channel: str) -> Optional[int]:
        match = cls._CHANNEL_ID_RE.search(channel)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    @staticmethod
    def _apply_order_book(
        target: Dict[str, Optional[float]],
        asks: Sequence[OrderLevel],
        bids: Sequence[OrderLevel],
    ) -> Set[str]:
        changed: Set[str] = set()

        def _best(side, *, reverse: bool) -> Optional[tuple[float, float]]:
            if not side:
                return None
            best_level = min(side, key=lambda level: level.price) if not reverse else max(
                side, key=lambda level: level.price
            )
            return best_level.price, best_level.size

        best_ask = _best(asks, reverse=False)
        best_bid = _best(bids, reverse=True)

        def _set(field_price: str, field_size: str, value: Optional[tuple[float, float]]) -> None:
            if value is None:
                MarketStore._assign_optional(target, field_price, None, changed)
                MarketStore._assign_optional(target, field_size, None, changed)
                return
            price, size = value
            MarketStore._assign_optional(target, field_price, price, changed)
            MarketStore._assign_optional(target, field_size, size, changed)

        _set("best_ask_price", "best_ask_size", best_ask)
        _set("best_bid_price", "best_bid_size", best_bid)
        mid_price = None
        spread_bps = None
        if best_ask and best_bid:
            mid_price = (best_ask[0] + best_bid[0]) / 2
            absolute_spread = best_ask[0] - best_bid[0]
            if mid_price not in (None, 0):
                spread_bps = (absolute_spread / mid_price) * 10_000
        MarketStore._assign_optional(target, "mid_price", mid_price, changed)
        MarketStore._assign_optional(target, "spread", spread_bps, changed)
        markout = MarketStore._calc_markout(target)
        MarketStore._assign_optional(target, "markout", markout, changed)
        return changed

    @staticmethod
    def _almost_equal(left: Optional[float], right: Optional[float]) -> bool:
        if left is None or right is None:
            return left is right
        return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9)

    @staticmethod
    def _assign_optional(
        target: Dict[str, Optional[float]],
        field: str,
        value: Optional[float],
        changed: Set[str],
    ) -> None:
        current = target.get(field)
        if value is None:
            if current is not None:
                target[field] = None
                changed.add(field)
            return
        if current is None or not MarketStore._almost_equal(current, value):
            target[field] = value
            changed.add(field)

    @staticmethod
    def _update_if_not_none(
        target: Dict[str, Optional[float]],
        field: str,
        value: Optional[float],
        changed: Set[str],
    ) -> None:
        if value is None:
            return
        MarketStore._assign_optional(target, field, value, changed)

    @staticmethod
    def _calc_basis(target: Dict[str, Optional[float]]) -> Optional[float]:
        mark = target.get("mark_price")
        index = target.get("index_price")
        if mark is None or index is None:
            return None
        return mark - index

    @staticmethod
    def _calc_markout(target: Dict[str, Optional[float]]) -> Optional[float]:
        mid = target.get("mid_price")
        last = target.get("last_price")
        if mid is None or last is None:
            return None
        return mid - last

    @staticmethod
    def _sort_key(row: MarketRow) -> tuple:
        oi = row.open_interest
        if oi is None:
            return (1, 0, row.market_id)
        return (0, -oi, row.market_id)

    async def _schedule_publish(self, row: MarketRow, fields: Set[str]) -> None:
        if not fields:
            return
        market_id = row.market_id
        fields = set(fields)
        fields.add("market_id")
        pending = self._pending.get(market_id)
        if pending:
            _, existing_fields = pending
            fields |= existing_fields
        self._pending[market_id] = (row, fields)
        now = time.time()
        last = self._last_publish.get(market_id, 0.0)
        if now - last >= self._debounce:
            await self._emit(market_id)
            return
        if market_id in self._flush_tasks:
            return
        delay = max(self._debounce - (now - last), 0.0)
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._delayed_emit(market_id, delay))
        self._flush_tasks[market_id] = task

    async def _delayed_emit(self, market_id: int, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            await self._emit(market_id)
        except asyncio.CancelledError:
            raise
        finally:
            self._flush_tasks.pop(market_id, None)

    async def _emit(self, market_id: int) -> None:
        data = self._pending.pop(market_id, None)
        if data is None:
            return
        row, fields = data
        self._last_publish[market_id] = time.time()
        payload = row.for_wire()
        update = {key: payload[key] for key in fields if key in payload}
        await self._bus.publish(update)

    async def close(self) -> None:
        for task in list(self._flush_tasks.values()):
            task.cancel()
        self._flush_tasks.clear()
