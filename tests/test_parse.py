import pytest

from lighter_md.dto import OrderBookMsg, parse_ws_message, MarketStatsMsg


def test_parse_market_stats_message():
    payload = {
        "type": "update/market_stats",
        "channel": "market_stats:42",
        "market_stats": {
            "market_id": 42,
            "index_price": "3335.04",
            "mark_price": "3335.09",
            "open_interest": "235.25",
            "last_trade_price": "3335.65",
            "current_funding_rate": "0.0057",
            "funding_rate": "0.0005",
            "daily_base_token_volume": "123.45",
            "daily_quote_token_volume": "765295250.98",
        },
    }
    message = parse_ws_message(payload)
    assert isinstance(message, MarketStatsMsg)
    stats = message.market_stats
    assert stats.market_id == 42
    assert pytest.approx(stats.index_price, rel=0.0, abs=1e-9) == 3335.04
    assert stats.effective_funding_rate == pytest.approx(0.0057)
    assert stats.effective_daily_volume == pytest.approx(765295250.98)


def test_parse_order_book_message():
    payload = {
        "type": "update/order_book",
        "channel": "order_book:42",
        "order_book": {
            "asks": [{"price": "3338.80", "size": "10.2898"}],
            "bids": [{"price": "3327.46", "size": "29.0915"}],
        },
    }
    message = parse_ws_message(payload)
    assert isinstance(message, OrderBookMsg)
    assert message.order_book.asks[0].price == pytest.approx(3338.80)
    assert message.order_book.bids[0].size == pytest.approx(29.0915)


def test_parse_invalid_message_type():
    with pytest.raises(ValueError):
        parse_ws_message({"type": "unknown", "channel": "noop"})
