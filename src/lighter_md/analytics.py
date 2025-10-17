"""Periodic analytics for funding rate signals."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from .bus import funding_bus
from .config import settings
from .store import MarketRow, MarketStore


logger = logging.getLogger("lighter_md.analytics")


def compute_cross_sectional_zscores(
    rows: Sequence[MarketRow],
    *,
    min_assets_per_time: int = 3,
    ddof: int = 0,
) -> Dict[int, Optional[float]]:
    """Return z-scores keyed by market id for the provided market rows."""

    funding_values: List[tuple[int, float]] = []
    for row in rows:
        if row.funding_rate is None:
            continue
        funding_values.append((row.market_id, row.funding_rate))

    count = len(funding_values)
    if count < min_assets_per_time:
        return {row.market_id: None for row in rows}

    mean = sum(val for _, val in funding_values) / count
    variance_sum = sum((val - mean) ** 2 for _, val in funding_values)

    divisor = max(count - ddof, 1)
    std = math.sqrt(variance_sum / divisor)
    if std <= 0.0:
        return {row.market_id: None for row in rows}

    zscores: Dict[int, Optional[float]] = {}
    for row in rows:
        if row.funding_rate is None:
            zscores[row.market_id] = None
        else:
            zscores[row.market_id] = (row.funding_rate - mean) / std
    return zscores


@dataclass
class FundingSnapshot:
    timestamp_ms: int
    rows: List[dict]


class FundingAnalytics:
    """Computes the funding z-score signal on a schedule."""

    def __init__(
        self,
        store: MarketStore,
        *,
        publish_bus=funding_bus,
        interval_seconds: float = settings.funding_refresh_seconds,
        min_assets: int = settings.funding_min_assets,
    ) -> None:
        self._store = store
        self._bus = publish_bus
        self._interval = interval_seconds
        self._min_assets = min_assets
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._latest: Optional[FundingSnapshot] = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    @property
    def latest(self) -> Optional[FundingSnapshot]:
        return self._latest

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                await self._compute_and_publish()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise

    async def _compute_and_publish(self) -> None:
        rows = await self._store.rows()
        if not rows:
            snapshot = FundingSnapshot(timestamp_ms=int(time.time() * 1000), rows=[])
            self._latest = snapshot
            await self._bus.publish({"type": "snapshot", "timestamp": snapshot.timestamp_ms, "rows": []})
            return

        zscores = compute_cross_sectional_zscores(
            rows, min_assets_per_time=self._min_assets, ddof=0
        )
        now_ms = int(time.time() * 1000)

        def sort_key(row: MarketRow) -> tuple:
            z = zscores.get(row.market_id)
            z_key = float("inf") if z is None else -z
            oi = row.open_interest or 0.0
            return (z_key, -oi, row.market_id)

        ordered_rows = sorted(rows, key=sort_key)

        payload_rows: List[dict] = []
        for row in ordered_rows:
            wire = row.for_wire()
            payload_rows.append(
                {
                    "market_id": row.market_id,
                    "symbol": wire["symbol"],
                    "funding_rate": row.funding_rate,
                    "open_interest": row.open_interest,
                    "zscore": zscores.get(row.market_id),
                }
            )

        snapshot = FundingSnapshot(timestamp_ms=now_ms, rows=payload_rows)
        self._latest = snapshot
        await self._bus.publish({"type": "snapshot", "timestamp": now_ms, "rows": payload_rows})


__all__ = ["FundingAnalytics", "FundingSnapshot", "compute_cross_sectional_zscores"]
