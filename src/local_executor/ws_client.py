from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed


class ResilientWebSocketClient:
    """WebSocket client with reconnect, explicit envelope handling, and heartbeat checks."""

    def __init__(
        self,
        ws_url: str,
        ws_token: str,
        heartbeat_interval_seconds: float,
        heartbeat_timeout_seconds: float,
        reconnect_initial_delay_seconds: float,
        reconnect_max_delay_seconds: float,
        on_signal: Callable[[dict[str, Any]], Awaitable[None]],
        on_resync: Callable[[str], Awaitable[None]],
        bot_id: str = "unknown-bot",
        protocol_version: str = "1.0",
        handshake_ack_required: bool = True,
        handshake_ack_timeout_seconds: float = 5.0,
        heartbeat_stale_seconds: float = 45.0,
        connect_func: Callable[..., Any] | None = None,
        sleep_func: Callable[[float], Awaitable[None]] = asyncio.sleep,
        logger: logging.Logger | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._ws_token = ws_token
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._reconnect_initial_delay_seconds = reconnect_initial_delay_seconds
        self._reconnect_max_delay_seconds = reconnect_max_delay_seconds
        self._on_signal = on_signal
        self._on_resync = on_resync
        self._bot_id = bot_id
        self._protocol_version = protocol_version
        self._handshake_ack_required = handshake_ack_required
        self._handshake_ack_timeout_seconds = handshake_ack_timeout_seconds
        self._heartbeat_stale_seconds = heartbeat_stale_seconds
        self._connect_func = connect_func or websockets.connect
        self._sleep_func = sleep_func
        self._logger = logger or logging.getLogger(__name__)
        self._last_heartbeat_monotonic = 0.0
        self._reconnect_count = 0
        self._heartbeat_timeout_count = 0
        self._invalid_message_count = 0

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        reconnect_delay = self._reconnect_initial_delay_seconds
        is_first_connect = True

        while not stop_event.is_set():
            try:
                async with self._connect_func(
                    self._ws_url,
                    extra_headers={"Authorization": f"Bearer {self._ws_token}"},
                    ping_interval=None,
                    ping_timeout=None,
                ) as websocket:
                    await self._perform_handshake(websocket)
                    self._mark_heartbeat("connect")
                    reason = "startup" if is_first_connect else "reconnect"
                    await self._on_resync(reason)
                    is_first_connect = False
                    reconnect_delay = self._reconnect_initial_delay_seconds
                    self._logger.info(
                        "Connected to system WebSocket. mode=consume_only bot_id=%s protocol_version=%s",
                        self._bot_id,
                        self._protocol_version,
                    )
                    await self._consume_loop(websocket, stop_event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - runtime needs broad reconnect handling
                self._logger.warning(
                    "WebSocket connection dropped error=%s reconnect_count=%d heartbeat_timeout_count=%d invalid_message_count=%d",
                    exc.__class__.__name__,
                    self._reconnect_count,
                    self._heartbeat_timeout_count,
                    self._invalid_message_count,
                )

            if stop_event.is_set():
                break

            self._reconnect_count += 1
            self._logger.info("Reconnecting in %.1f seconds.", reconnect_delay)
            await self._sleep_func(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, self._reconnect_max_delay_seconds)

    async def _consume_loop(self, websocket: Any, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                raw_message = await asyncio.wait_for(
                    websocket.recv(), timeout=self._heartbeat_interval_seconds
                )
                handled = await self._handle_frame(raw_message)
                if handled and self._is_heartbeat_stale():
                    raise RuntimeError("Heartbeat stale during active stream.")
            except asyncio.TimeoutError:
                self._heartbeat_timeout_count += 1
                await self._do_heartbeat(websocket)
                if self._is_heartbeat_stale():
                    raise RuntimeError("Heartbeat stale after ping/pong fallback.")
            except ConnectionClosed:
                raise

    async def _perform_handshake(self, websocket: Any) -> None:
        hello_payload = {
            "type": "subscribe",
            "payload": {
                "bot_id": self._bot_id,
                "protocol_version": self._protocol_version,
                "stream": "signal_execution",
                "mode": "consume_only",
            },
        }
        await websocket.send(json.dumps(hello_payload, separators=(",", ":")))

        if not self._handshake_ack_required:
            self._logger.info("Handshake ack wait disabled. Continuing without ack.")
            return

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._handshake_ack_timeout_seconds

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RuntimeError("Handshake ack timed out.")

            raw_message = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            message = self._decode_message(raw_message)
            if message is None:
                continue

            envelope = self._decode_envelope(message)
            if envelope is None:
                continue

            message_type, payload = envelope
            if message_type == "heartbeat":
                self._mark_heartbeat("protocol")
                continue
            if message_type in {"ack", "system"} and self._is_valid_handshake_ack(payload):
                self._logger.info("Handshake acknowledged for bot_id=%s", self._bot_id)
                return

            self._invalid_message_count += 1
            raise RuntimeError(f"Invalid handshake response type={message_type}")

    async def _handle_frame(self, raw_message: Any) -> bool:
        message = self._decode_message(raw_message)
        if message is None:
            return False

        envelope = self._decode_envelope(message)
        if envelope is None:
            return False

        message_type, payload = envelope
        if message_type == "signal":
            await self._on_signal(payload)
            return True

        if message_type == "heartbeat":
            self._mark_heartbeat("protocol")
            return True

        if message_type in {"ack", "system"}:
            self._logger.info("Ignoring non-signal frame type=%s", message_type)
            return True

        return False

    async def _do_heartbeat(self, websocket: Any) -> None:
        pong_waiter = websocket.ping()
        await asyncio.wait_for(pong_waiter, timeout=self._heartbeat_timeout_seconds)
        self._mark_heartbeat("ping_pong")

    def _mark_heartbeat(self, source: str) -> None:
        self._last_heartbeat_monotonic = asyncio.get_running_loop().time()
        self._logger.debug("Heartbeat updated source=%s", source)

    def _is_heartbeat_stale(self) -> bool:
        if self._last_heartbeat_monotonic <= 0:
            return False
        elapsed = asyncio.get_running_loop().time() - self._last_heartbeat_monotonic
        return elapsed > self._heartbeat_stale_seconds

    def _decode_envelope(self, message: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
        frame_type = message.get("type")
        if frame_type is None:
            if self._looks_like_legacy_signal(message):
                return "signal", message
            self._invalid_message_count += 1
            self._logger.warning("Skipping frame without type envelope.")
            return None

        if not isinstance(frame_type, str) or not frame_type.strip():
            self._invalid_message_count += 1
            self._logger.warning("Skipping frame with invalid type envelope.")
            return None

        normalized = frame_type.strip().lower()
        if normalized not in {"signal", "heartbeat", "ack", "system"}:
            self._invalid_message_count += 1
            self._logger.warning("Skipping unsupported frame type=%s", normalized)
            return None

        payload = message.get("payload")
        if normalized == "signal":
            if isinstance(payload, dict):
                return "signal", payload
            if payload is None and self._looks_like_legacy_signal(message):
                return "signal", message
            self._invalid_message_count += 1
            self._logger.warning("Skipping signal frame with invalid payload.")
            return None

        if isinstance(payload, dict):
            return normalized, payload

        return normalized, message

    def _is_valid_handshake_ack(self, payload: dict[str, Any]) -> bool:
        status = str(payload.get("status", "ok")).strip().lower()
        ack_type = str(payload.get("ack_type") or payload.get("for") or payload.get("event") or "").strip().lower()

        if status not in {"ok", "success", "accepted"}:
            return False

        if ack_type and ack_type not in {"subscribe", "hello", "subscribed", "session_ready"}:
            return False

        return True

    def get_counters(self) -> dict[str, int]:
        return {
            "reconnect_count": self._reconnect_count,
            "heartbeat_timeout_count": self._heartbeat_timeout_count,
            "invalid_message_count": self._invalid_message_count,
        }

    @staticmethod
    def _looks_like_legacy_signal(message: dict[str, Any]) -> bool:
        return any(key in message for key in ("signal_id", "signalId", "action", "symbol"))

    def _decode_message(self, raw_message: Any) -> dict[str, Any] | None:
        if isinstance(raw_message, bytes):
            text = raw_message.decode("utf-8", errors="replace")
        else:
            text = str(raw_message)

        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            self._invalid_message_count += 1
            self._logger.warning("Skipping non-JSON WebSocket frame.")
            return None

        if not isinstance(decoded, dict):
            self._invalid_message_count += 1
            self._logger.warning("Skipping non-object JSON frame.")
            return None

        return decoded
