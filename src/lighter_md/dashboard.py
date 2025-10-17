"""FastAPI dashboard serving WebSocket-fed market data."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any, AsyncIterator

import uvicorn
from fastapi import APIRouter, FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .analytics import FundingAnalytics
from .bus import bus, funding_bus
from .config import settings
from .ws_manager import WebSocketManager, build_manager


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
static_dir = BASE_DIR / "static"

router = APIRouter()


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = build_manager()
    app.state.manager = manager
    await manager.start()
    analytics = FundingAnalytics(manager.store)
    app.state.analytics = analytics
    await analytics.start()
    try:
        yield
    finally:
        await analytics.stop()
        await manager.stop()


def create_app() -> FastAPI:
    logging.basicConfig(level=settings.log_level)
    app = FastAPI(title="Lighter Market Dashboard", lifespan=lifespan)
    app.include_router(router)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    return app


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html", {"request": request, "active_page": "markets"}
    )


@router.get("/funding", response_class=HTMLResponse)
async def funding_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "funding.html", {"request": request, "active_page": "funding"}
    )


@router.get("/charts", response_class=HTMLResponse)
async def charts_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "charts.html", {"request": request, "active_page": "charts"}
    )


@router.get("/health", response_class=JSONResponse)
async def health(request: Request) -> JSONResponse:
    manager: WebSocketManager = request.app.state.manager
    status = "ok" if manager.is_running() else "starting"
    market_ids = await manager.store.market_ids()
    return JSONResponse({"status": status, "markets": len(market_ids)})


@router.websocket("/ws")
async def websocket_updates(websocket: WebSocket) -> None:
    await websocket.accept()
    manager: WebSocketManager = websocket.app.state.manager
    snapshot = await manager.store.snapshot()
    await websocket.send_json({"type": "snapshot", "rows": snapshot})

    updates = bus.subscribe()
    try:
        async for update in updates:
            if update.get("type") == "closed":
                break
            await websocket.send_json({"type": "update", "row": update})
    except WebSocketDisconnect:
        pass
    finally:
        await updates.aclose()
        with suppress(Exception):
            await websocket.close()


@router.websocket("/ws/funding")
async def websocket_funding(websocket: WebSocket) -> None:
    await websocket.accept()
    analytics: FundingAnalytics = websocket.app.state.analytics
    latest = analytics.latest
    if latest:
        await websocket.send_json(
            {"type": "snapshot", "timestamp": latest.timestamp_ms, "rows": latest.rows}
        )

    updates = funding_bus.subscribe()
    try:
        async for update in updates:
            if update.get("type") == "closed":
                break
            await websocket.send_json(update)
    except WebSocketDisconnect:
        pass
    finally:
        await updates.aclose()
        with suppress(Exception):
            await websocket.close()


def main() -> None:
    uvicorn.run(
        "lighter_md.dashboard:app",
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        reload=False,
        factory=False,
    )


app = create_app()


__all__ = ["app", "main", "create_app"]
