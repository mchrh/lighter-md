"""Configuration helpers for the Lighter market data service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import os
from pathlib import Path


DEFAULT_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"


def _float_env(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_env(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """Runtime settings with environment overrides."""

    ws_url: str = os.environ.get("LIGHTER_WS_URL", DEFAULT_WS_URL)
    ping_interval: float = _float_env("LIGHTER_WS_PING_INTERVAL", 20.0)
    reconnect_base_delay: float = _float_env("LIGHTER_WS_RECONNECT_BASE", 0.5)
    reconnect_max_delay: float = _float_env("LIGHTER_WS_RECONNECT_MAX", 30.0)
    ui_debounce_seconds: float = _float_env("LIGHTER_UI_DEBOUNCE", 0.2)
    dashboard_host: str = os.environ.get("LIGHTER_DASHBOARD_HOST", "0.0.0.0")
    dashboard_port: int = _int_env("LIGHTER_DASHBOARD_PORT", 8000)
    metadata_path: Optional[str] = os.environ.get("LIGHTER_MARKET_METADATA") or str(
        Path(__file__).resolve().with_name("market_metadata.json")
    )
    log_level: str = os.environ.get("LIGHTER_LOG_LEVEL", "INFO")
    funding_refresh_seconds: float = _float_env("LIGHTER_FUNDING_REFRESH_SECONDS", 60.0)
    funding_min_assets: int = _int_env("LIGHTER_FUNDING_MIN_ASSETS", 3)


settings = Settings()
