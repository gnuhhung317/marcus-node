from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .config import ExecutorConfig
from .execution import CcxtSignalExecutor
from .ws_client import ResilientWebSocketClient

SignalHandler = Callable[[dict[str, Any]], Awaitable[None]]


class LocalExecutorEngine:
    """Trader-side core loop: listen to live signals and execute locally."""

    def __init__(self, config: ExecutorConfig, on_signal: SignalHandler | None = None) -> None:
        self._config = config
        self._logger = logging.getLogger(__name__)
        self._executor = CcxtSignalExecutor(config=config, logger=self._logger)
        self._on_signal = on_signal or self._default_signal_handler
        self._ws_client = ResilientWebSocketClient(
            ws_url=config.ws_url,
            ws_token=config.ws_token,
            heartbeat_interval_seconds=config.heartbeat_interval_seconds,
            heartbeat_timeout_seconds=config.heartbeat_timeout_seconds,
            reconnect_initial_delay_seconds=config.reconnect_initial_delay_seconds,
            reconnect_max_delay_seconds=config.reconnect_max_delay_seconds,
            on_signal=self._handle_signal,
            on_resync=self._on_resync,
            bot_id=config.bot_id,
            protocol_version=config.protocol_version,
            handshake_ack_required=config.handshake_ack_required,
            handshake_ack_timeout_seconds=config.handshake_ack_timeout_seconds,
            heartbeat_stale_seconds=config.heartbeat_stale_seconds,
            logger=self._logger,
        )

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        await self._ws_client.run(stop_event=stop_event)

    async def _handle_signal(self, payload: dict[str, Any]) -> None:
        await self._on_signal(payload)

    async def _on_resync(self, reason: str) -> None:
        self._logger.info("Resync hook ignored reason=%s", reason)

    async def _default_signal_handler(self, payload: dict[str, Any]) -> None:
        result = await self._executor.execute_signal(payload)
        signal_id = payload.get("signal_id") or payload.get("signalId") or "unknown"
        action = payload.get("action", "unknown")
        symbol = payload.get("symbol", "unknown")
        self._logger.info(
            "Signal executed signal_id=%s action=%s symbol=%s mode=%s order_id=%s",
            signal_id,
            action,
            symbol,
            result.mode,
            result.order_id or "n/a",
        )
