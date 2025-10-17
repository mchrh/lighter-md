"""Typed representations of WebSocket payloads."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError as exc:
            raise ValueError(f"invalid float value: {value}") from exc
    raise ValueError(f"unsupported type for float coercion: {type(value)!r}")


class OrderLevel(BaseModel):
    price: float
    size: float

    model_config = ConfigDict(extra="ignore")

    @field_validator("price", "size", mode="before")
    @classmethod
    def _parse_float(cls, value: Any) -> float:
        coerced = _coerce_float(value)
        if coerced is None:
            raise ValueError("price/size cannot be null")
        return coerced


class OrderBookPayload(BaseModel):
    asks: List[OrderLevel] = Field(default_factory=list)
    bids: List[OrderLevel] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore")


class OrderBookMsg(BaseModel):
    type: Literal["update/order_book"]
    channel: str
    order_book: OrderBookPayload

    model_config = ConfigDict(extra="ignore")


class MarketStatsBody(BaseModel):
    market_id: int
    index_price: Optional[float] = None
    mark_price: Optional[float] = None
    open_interest: Optional[float] = None
    last_trade_price: Optional[float] = None
    current_funding_rate: Optional[float] = None
    funding_rate: Optional[float] = None
    daily_base_token_volume: Optional[float] = None
    daily_quote_token_volume: Optional[float] = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    @field_validator(
        "index_price",
        "mark_price",
        "open_interest",
        "last_trade_price",
        "current_funding_rate",
        "funding_rate",
        "daily_base_token_volume",
        "daily_quote_token_volume",
        mode="before",
    )
    @classmethod
    def _parse_optional_float(cls, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return _coerce_float(value)
        except ValueError:
            return None

    @property
    def effective_funding_rate(self) -> Optional[float]:
        return (
            self.current_funding_rate
            if self.current_funding_rate is not None
            else self.funding_rate
        )

    @property
    def effective_daily_volume(self) -> Optional[float]:
        return (
            self.daily_quote_token_volume
            if self.daily_quote_token_volume is not None
            else self.daily_base_token_volume
        )


class MarketStatsMsg(BaseModel):
    type: Literal["update/market_stats"]
    channel: str
    market_stats: MarketStatsBody

    model_config = ConfigDict(extra="ignore")


WsMessage = Union[OrderBookMsg, MarketStatsMsg]


def parse_ws_message(payload: Dict[str, Any]) -> WsMessage:
    """Parse a raw dict into the appropriate message type."""
    msg_type = payload.get("type")
    if msg_type == "update/order_book":
        return OrderBookMsg.model_validate(payload)
    if msg_type == "update/market_stats":
        return MarketStatsMsg.model_validate(payload)
    raise ValueError(f"unsupported message type: {msg_type!r}")
