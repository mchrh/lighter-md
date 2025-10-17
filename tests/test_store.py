import asyncio
import pytest

from lighter_md.dto import MarketStatsMsg, OrderBookMsg
from lighter_md.store import MarketStore


class StubBus:
    def __init__(self) -> None:
        self.events = []

    async def publish(self, message):
        self.events.append(message)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def test_market_stats_prefer_quote_volume_and_current_funding(event_loop):
    bus = StubBus()
    store = MarketStore(bus)
    store._debounce = 0  # type: ignore[attr-defined]

    payload = {
        "type": "update/market_stats",
        "channel": "market_stats:1",
        "market_stats": {
            "market_id": 1,
            "last_trade_price": "100.12",
            "mark_price": "100.10",
            "index_price": "100.05",
            "open_interest": "2500",
            "current_funding_rate": "0.0042",
            "funding_rate": "0.0022",
            "daily_base_token_volume": "12.3",
            "daily_quote_token_volume": "98765.4",
        },
    }
    msg = MarketStatsMsg.model_validate(payload)
    event_loop.run_until_complete(store.apply_market_stats(msg))

    assert bus.events, "expected publish event"
    event = bus.events[-1]
    assert event["market_id"] == 1
    assert pytest.approx(event["daily_volume"]) == 98765.4
    assert pytest.approx(event["funding_rate"]) == 0.0042
    assert event["basis"] == pytest.approx(0.05)

    event_loop.run_until_complete(store.close())


def test_order_book_updates_top_of_book_and_spread(event_loop):
    bus = StubBus()
    store = MarketStore(bus)
    store._debounce = 0  # type: ignore[attr-defined]

    stats_msg = MarketStatsMsg.model_validate(
        {
            "type": "update/market_stats",
            "channel": "market_stats:5",
            "market_stats": {
                "market_id": 5,
                "last_trade_price": "110.10",
            },
        }
    )
    event_loop.run_until_complete(store.apply_market_stats(stats_msg))
    bus.events.clear()

    payload = {
        "type": "update/order_book",
        "channel": "order_book/5",
        "order_book": {
            "asks": [{"price": "110.5", "size": "3.2"}, {"price": "111.0", "size": "1.1"}],
            "bids": [{"price": "108.9", "size": "4.0"}, {"price": "109.2", "size": "2.5"}],
        },
    }
    msg = OrderBookMsg.model_validate(payload)
    event_loop.run_until_complete(store.apply_order_book(msg))

    assert bus.events, "order book update should publish"
    event = bus.events[-1]
    assert pytest.approx(event["best_ask_price"]) == 110.5
    assert pytest.approx(event["best_bid_price"]) == 109.2
    expected_mid = (110.5 + 109.2) / 2
    expected_spread_bps = ((110.5 - 109.2) / expected_mid) * 10_000
    expected_markout = expected_mid - 110.10
    assert pytest.approx(event["mid_price"]) == pytest.approx(expected_mid)
    assert pytest.approx(event["spread"]) == pytest.approx(expected_spread_bps)
    assert pytest.approx(event["markout"]) == pytest.approx(expected_markout)

    event_loop.run_until_complete(store.close())


def test_redundant_updates_do_not_publish(event_loop):
    bus = StubBus()
    store = MarketStore(bus)
    store._debounce = 0  # type: ignore[attr-defined]

    payload = {
        "type": "update/market_stats",
        "channel": "market_stats:99",
        "market_stats": {
            "market_id": 99,
            "last_trade_price": "10.0",
        },
    }
    msg = MarketStatsMsg.model_validate(payload)
    event_loop.run_until_complete(store.apply_market_stats(msg))
    first_count = len(bus.events)
    event_loop.run_until_complete(store.apply_market_stats(msg))
    assert len(bus.events) == first_count

    event_loop.run_until_complete(store.close())
