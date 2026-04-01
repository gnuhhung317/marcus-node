from __future__ import annotations

import asyncio
import copy
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .config import ExecutorConfig
from .resync_client import PositionSyncClient
from .ws_client import ResilientWebSocketClient

SignalHandler = Callable[[dict[str, Any]], Awaitable[None]]


class LocalExecutorEngine:
    """Trader-side core loop: re-sync state then listen to live signals."""

    def __init__(self, config: ExecutorConfig, on_signal: SignalHandler | None = None) -> None:
        self._config = config
        self._logger = logging.getLogger(__name__)
        self._on_signal = on_signal or self._default_signal_handler
        self._latest_position_snapshot: dict[str, Any] = {}
        self._resync_reason_counts: dict[str, int] = {}
        self._sync_client = PositionSyncClient(
            base_url=config.cms_base_url,
            token=config.http_token,
            timeout_seconds=config.request_timeout_seconds,
            ssl_verify=config.ssl_verify,
        )
        self._ws_client = ResilientWebSocketClient(
            ws_url=config.ws_url,
            ws_token=config.ws_token,
            heartbeat_interval_seconds=config.heartbeat_interval_seconds,
            heartbeat_timeout_seconds=config.heartbeat_timeout_seconds,
            reconnect_initial_delay_seconds=config.reconnect_initial_delay_seconds,
            reconnect_max_delay_seconds=config.reconnect_max_delay_seconds,
            on_signal=self._handle_signal,
            on_resync=self._resync_state,
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
        signal_id = payload.get("signal_id") or payload.get("signalId") or "unknown"

        payload_to_process = payload
        if not self._has_position_context(payload):
            self._logger.info(
                "Signal missing position context. Fallback re-sync required signal_id=%s",
                signal_id,
            )
            await self._resync_state("signal_fallback")
            payload_to_process = dict(payload)
            payload_to_process["position_snapshot"] = copy.deepcopy(self._latest_position_snapshot)
            self._logger.info(
                "Signal fallback path applied signal_id=%s snapshot_present=%s",
                signal_id,
                bool(self._latest_position_snapshot),
            )

        await self._on_signal(payload_to_process)

    async def _resync_state(self, reason: str) -> None:
        position = self._sync_client.fetch_current_position(self._config.bot_id)
        self._latest_position_snapshot = copy.deepcopy(position)
        self._resync_reason_counts[reason] = self._resync_reason_counts.get(reason, 0) + 1
        has_open_position = bool(position)
        self._logger.info(
            "State re-sync complete reason=%s reason_count=%d has_open_position=%s",
            reason,
            self._resync_reason_counts[reason],
            has_open_position,
        )

    @staticmethod
    def _has_position_context(payload: dict[str, Any]) -> bool:
        for key in (
            "position",
            "current_position",
            "currentPosition",
            "position_context",
            "position_snapshot",
        ):
            value = payload.get(key)
            if value not in (None, "", {}, []):
                return True
        return False

    async def _default_signal_handler(self, payload: dict[str, Any]) -> None:
        signal_id = payload.get("signal_id") or payload.get("signalId") or "unknown"
        action = payload.get("action", "unknown")
        symbol = payload.get("symbol", "unknown")
        self._logger.info(
            "Received signal -> signal_id=%s action=%s symbol=%s one_way=true",
            signal_id,
            action,
            symbol,
        )
