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
        stop_event = stop_event or asyncio.Event()
        
        self._logger.info("Starting LocalExecutorEngine loops.")
        tasks = [
            asyncio.create_task(self._ws_client.run(stop_event=stop_event), name="ws_client_run"),
            asyncio.create_task(self._balance_sync_loop(stop_event), name="balance_sync_loop")
        ]

        try:
            # Run concurrently until stopped or client errors out
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            # Propagate errors from done tasks if any
            for t in done:
                if not t.cancelled() and t.exception():
                    exc = t.exception()
                    self._logger.error(
                        "Task encountered fatal error: %s repr=%s",
                        exc.__class__.__name__,
                        repr(exc),
                    )
                    self._logger.debug("Full exception traceback:", exc_info=exc)
                    raise exc
        finally:
            # Graceful shutdown of remaining background tasks
            self._logger.info("Shutting down engine tasks.")
            stop_event.set()
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _balance_sync_loop(self, stop_event: asyncio.Event) -> None:
        """Periodically emits account equity to backend for dashboard views."""
        interval = max(5.0, self._config.balance_sync_interval_seconds)
        self._logger.info("Balance sync task initiated. interval=%.1fs", interval)
        
        # Wait brief moment to allow handshake to settle on startup
        await asyncio.sleep(5.0)

        while not stop_event.is_set():
            try:
                # 1. Fetch account snapshots safely from CcxtProxy
                bal = await self._executor.fetch_balance()
                
                # 2. Build compliant JSON audit-push frame
                # Note: Backend requires: type='audit-push', payload={kind: 'balance-snapshot', ...}
                frame = {
                    "type": "audit-push",
                    "botId": self._config.bot_id,
                    "payload": {
                        "kind": "balance-snapshot",
                        "total": bal["total"],
                        "free": bal["free"],
                        "used": bal["used"],
                        "currency": bal["currency"],
                        "mode": self._config.execution_mode,
                        "timestamp": self._ws_client._now_utc_iso()
                    }
                }
                
                # 3. Attempt push over raw socket
                success = await self._ws_client.send_frame(frame)
                if success:
                    self._logger.debug(
                        "Balance synced successfully total=%.2f currency=%s", 
                        bal["total"], bal["currency"]
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.warning("Balance sync attempt failed: %s", e)

            # Wait for next cyclic tick, gracefully breaking if stopped
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                break  # stop_event was set
            except asyncio.TimeoutError:
                continue  # cycle normally

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
