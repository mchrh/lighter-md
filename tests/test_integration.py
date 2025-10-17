import asyncio
import pytest

from lighter_md.store import MarketStore
from lighter_md.ws_manager import WebSocketManager


class RecordingBus:
    def __init__(self) -> None:
        self.events = []

    async def publish(self, message):
        self.events.append(message)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def test_manager_discovers_market_and_enqueues_subscription(event_loop):
    bus = RecordingBus()
    outbound = asyncio.Queue()
    store = MarketStore(bus)
    store._debounce = 0  # type: ignore[attr-defined]
    manager = WebSocketManager(store, outbound_queue=outbound)

    stats_payload = {
        "type": "update/market_stats",
        "channel": "market_stats/all",
        "market_stats": {
            "market_id": 7,
            "last_trade_price": "50.5",
            "current_funding_rate": "0.01",
        },
    }
    event_loop.run_until_complete(manager._on_message(stats_payload))

    subscribe_message = event_loop.run_until_complete(asyncio.wait_for(outbound.get(), 0.1))
    assert subscribe_message["channel"] == "order_book/7"

    assert bus.events, "store should emit snapshot for new market"
    snapshot_event = bus.events[-1]
    assert snapshot_event["market_id"] == 7
    assert pytest.approx(snapshot_event["funding_rate"]) == 0.01

    order_payload = {
        "type": "update/order_book",
        "channel": "order_book:7",
        "order_book": {
            "asks": [{"price": "51", "size": "1"}],
            "bids": [{"price": "49.5", "size": "2"}],
        },
    }
    event_loop.run_until_complete(manager._on_message(order_payload))
    order_event = bus.events[-1]
    expected_mid = (51 + 49.5) / 2
    expected_spread_bps = ((51 - 49.5) / expected_mid) * 10_000
    assert pytest.approx(order_event["mid_price"]) == pytest.approx(expected_mid)
    assert pytest.approx(order_event["spread"]) == pytest.approx(expected_spread_bps)
    assert pytest.approx(order_event["markout"]) == pytest.approx(expected_mid - 50.5)

    reconnect_messages = event_loop.run_until_complete(manager._on_connect())
    assert any(msg["channel"] == "order_book/7" for msg in reconnect_messages)

    event_loop.run_until_complete(store.close())
