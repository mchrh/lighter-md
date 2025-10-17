"""Low-level WebSocket client utilities."""

from __future__ import annotations

import asyncio
import logging
import random
from contextlib import suppress
from typing import Awaitable, Callable, Iterable, Optional

import orjson
import websockets
from anyio import sleep
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import WebSocketException

from .config import settings

OnMessage = Callable[[dict], Awaitable[None]]
OnConnect = Callable[[], Awaitable[Iterable[dict]]]


async def run_ws_loop(
    url: str,
    outbound_queue: "asyncio.Queue[Optional[dict]]",
    on_connect: OnConnect,
    on_message: OnMessage,
    stop_event: asyncio.Event,
    logger: logging.Logger,
) -> None:
    """Run a resilient WebSocket loop with automatic reconnect and send queue."""

    backoff = settings.reconnect_base_delay

    while not stop_event.is_set():
        try:
            async with websockets.connect(
                url,
                ping_interval=settings.ping_interval,
                ping_timeout=settings.ping_interval + 5,
                close_timeout=10,
                max_queue=None,
            ) as ws:
                logger.info("Connected to %s", url)
                backoff = settings.reconnect_base_delay
                await _handle_connection(ws, outbound_queue, on_connect, on_message, stop_event, logger)
                if stop_event.is_set():
                    break
        except asyncio.CancelledError:
            raise
        except (OSError, WebSocketException) as exc:
            logger.warning("WebSocket error: %s", exc)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unexpected WebSocket failure: %s", exc)

        if stop_event.is_set():
            break
        delay = min(backoff, settings.reconnect_max_delay)
        jitter = random.uniform(0.0, min(1.0, delay / 2))
        await sleep(delay + jitter)
        backoff = min(backoff * 2, settings.reconnect_max_delay)

    logger.info("WS loop for %s stopped", url)


async def _handle_connection(
    ws: WebSocketClientProtocol,
    outbound_queue: "asyncio.Queue[Optional[dict]]",
    on_connect: OnConnect,
    on_message: OnMessage,
    stop_event: asyncio.Event,
    logger: logging.Logger,
) -> None:
    send_task = asyncio.create_task(_sender(ws, outbound_queue, stop_event, logger))
    try:
        initial = await on_connect()
        for message in initial:
            await outbound_queue.put(message)
        async for raw in ws:
            if stop_event.is_set():
                break
            try:
                payload = orjson.loads(raw)
            except orjson.JSONDecodeError:
                logger.debug("Ignoring malformed JSON: %s", raw)
                continue
            await on_message(payload)
    except asyncio.CancelledError:
        raise
    finally:
        send_task.cancel()
        with suppress(asyncio.CancelledError):
            await send_task


async def _sender(
    ws: WebSocketClientProtocol,
    outbound_queue: "asyncio.Queue[Optional[dict]]",
    stop_event: asyncio.Event,
    logger: logging.Logger,
) -> None:
    try:
        while not stop_event.is_set():
            message = await outbound_queue.get()
            if message is None:
                break
            data = orjson.dumps(message).decode("utf-8")
            try:
                await ws.send(data)
            except Exception as exc:
                logger.debug("Send failed, will retry after reconnect: %s", exc)
                # The queue slot was consumed; put message back for the next session.
                with suppress(asyncio.QueueFull):
                    outbound_queue.put_nowait(message)
                raise
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.debug("Sender loop exiting due to error: %s", exc)
    finally:
        logger.debug("Sender loop stopped")
