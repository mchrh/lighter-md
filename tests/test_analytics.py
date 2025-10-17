import asyncio
import math

import pytest

from lighter_md.analytics import compute_cross_sectional_zscores, FundingAnalytics
from lighter_md.store import MarketRow


def make_row(market_id, **kwargs):
    defaults = dict(
        symbol=f"MKT-{market_id}",
        best_bid_price=None,
        best_bid_size=None,
        best_ask_price=None,
        best_ask_size=None,
        last_price=None,
        mark_price=None,
        index_price=None,
        mid_price=None,
        daily_volume=None,
        funding_rate=None,
        open_interest=None,
        basis=None,
        markout=None,
        spread=None,
        updated_ms=0,
    )
    defaults.update(kwargs)
    return MarketRow(market_id=market_id, **defaults)


def test_compute_cross_sectional_zscores_basic():
    rows = [
        make_row(1, funding_rate=0.01),
        make_row(2, funding_rate=0.02),
        make_row(3, funding_rate=0.03),
    ]
    zscores = compute_cross_sectional_zscores(rows, min_assets_per_time=3, ddof=0)
    assert set(zscores.keys()) == {1, 2, 3}
    assert zscores[2] == pytest.approx(0.0)
    assert zscores[1] == pytest.approx(-math.sqrt(1.5))
    assert zscores[3] == pytest.approx(math.sqrt(1.5))


def test_compute_cross_sectional_zscores_insufficient_assets():
    rows = [
        make_row(1, funding_rate=0.01),
        make_row(2, funding_rate=None),
    ]
    zscores = compute_cross_sectional_zscores(rows, min_assets_per_time=3, ddof=0)
    assert all(value is None for value in zscores.values())


class StubBus:
    def __init__(self) -> None:
        self.events = []

    async def publish(self, message):
        self.events.append(message)


class StubStore:
    def __init__(self, rows):
        self._rows = rows

    async def rows(self):
        return self._rows


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def test_funding_analytics_computes_snapshot(event_loop):
    rows = [
        make_row(1, funding_rate=0.01, open_interest=100),
        make_row(2, funding_rate=0.02, open_interest=200),
        make_row(3, funding_rate=0.03, open_interest=150),
    ]
    store = StubStore(rows)
    bus = StubBus()
    analytics = FundingAnalytics(store, publish_bus=bus, interval_seconds=0.1, min_assets=2)

    event_loop.run_until_complete(analytics._compute_and_publish())

    assert analytics.latest is not None
    assert bus.events
    payload = bus.events[-1]
    assert payload["type"] == "snapshot"
    symbols = [row["symbol"] for row in payload["rows"]]
    assert symbols[0] == "MKT-3"  # highest funding → highest z-score
