from __future__ import annotations

# pyright: reportMissingImports=false

import asyncio
import json
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_executor.ws_client import ResilientWebSocketClient


class _FakeWebSocket:
    def __init__(
        self,
        messages: list[Any],
        idle_delay_seconds: float = 0.01,
        idle_message: Any = '{"type":"heartbeat","payload":{"status":"ok"}}',
        ping_hangs: bool = False,
    ) -> None:
        self._messages = list(messages)
        self._idle_delay_seconds = idle_delay_seconds
        self._idle_message = idle_message
        self._ping_hangs = ping_hangs
        self.sent_frames: list[str] = []

    async def __aenter__(self) -> "_FakeWebSocket":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def recv(self) -> Any:
        if not self._messages:
            await asyncio.sleep(self._idle_delay_seconds)
            return self._idle_message
        next_value = self._messages.pop(0)
        if isinstance(next_value, tuple) and len(next_value) == 2:
            await asyncio.sleep(float(next_value[0]))
            next_value = next_value[1]
        if isinstance(next_value, Exception):
            raise next_value
        return next_value

    async def send(self, message: str) -> None:
        self.sent_frames.append(message)

    def ping(self) -> asyncio.Future[None]:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if not self._ping_hangs:
            future.set_result(None)
        return future


class ResilientWebSocketClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_should_resync_before_processing_first_signal(self) -> None:
        stop_event = asyncio.Event()
        events: list[tuple[str, str]] = []

        async def on_signal(payload: dict[str, Any]) -> None:
            events.append(("signal", str(payload["signal_id"])))
            stop_event.set()

        async def on_resync(reason: str) -> None:
            events.append(("resync", reason))

        fake_ws = _FakeWebSocket(
            messages=[
                '{"type":"ack","payload":{"status":"ok","ack_type":"subscribe"}}',
                '{"type":"signal","payload":{"signal_id":"sig-1","action":"OPEN_LONG"}}',
            ]
        )

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=8,
            on_signal=on_signal,
            on_resync=on_resync,
            bot_id="bot-01",
            protocol_version="1.0",
            connect_func=lambda *args, **kwargs: fake_ws,
        )

        await client.run(stop_event=stop_event)

        self.assertGreaterEqual(len(events), 2)
        self.assertEqual(events[0], ("resync", "startup"))
        self.assertEqual(events[1], ("signal", "sig-1"))

        self.assertEqual(len(fake_ws.sent_frames), 1)
        hello = json.loads(fake_ws.sent_frames[0])
        self.assertEqual(hello["type"], "subscribe")
        self.assertEqual(hello["payload"]["bot_id"], "bot-01")
        self.assertEqual(hello["payload"]["protocol_version"], "1.0")
        self.assertEqual(hello["payload"]["mode"], "consume_only")

    async def test_should_use_exponential_backoff_on_reconnect(self) -> None:
        stop_event = asyncio.Event()
        sleep_durations: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleep_durations.append(duration)
            if len(sleep_durations) >= 3:
                stop_event.set()

        async def on_signal(payload: dict[str, Any]) -> None:
            _ = payload

        async def on_resync(reason: str) -> None:
            _ = reason

        def failing_connect(*args, **kwargs):
            raise OSError("network down")

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=4,
            on_signal=on_signal,
            on_resync=on_resync,
            connect_func=failing_connect,
            sleep_func=fake_sleep,
        )

        await client.run(stop_event=stop_event)

        self.assertEqual(sleep_durations, [1, 2, 4])

    async def test_should_reconnect_when_handshake_ack_is_invalid(self) -> None:
        stop_event = asyncio.Event()
        signals: list[str] = []
        sleeps: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleeps.append(duration)

        async def on_signal(payload: dict[str, Any]) -> None:
            signals.append(str(payload["signal_id"]))
            stop_event.set()

        async def on_resync(reason: str) -> None:
            _ = reason

        first_socket = _FakeWebSocket(
            messages=['{"type":"signal","payload":{"signal_id":"too-early"}}']
        )
        second_socket = _FakeWebSocket(
            messages=[
                '{"type":"ack","payload":{"status":"ok","ack_type":"subscribe"}}',
                '{"type":"signal","payload":{"signal_id":"sig-2"}}',
            ]
        )
        sockets = [first_socket, second_socket]

        def connect_func(*args, **kwargs):
            _ = args
            _ = kwargs
            if sockets:
                return sockets.pop(0)
            return second_socket

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=0.01,
            reconnect_max_delay_seconds=0.05,
            on_signal=on_signal,
            on_resync=on_resync,
            connect_func=connect_func,
            sleep_func=fake_sleep,
        )

        await client.run(stop_event=stop_event)

        counters = client.get_counters()
        self.assertEqual(signals, ["sig-2"])
        self.assertGreaterEqual(counters["reconnect_count"], 1)
        self.assertGreaterEqual(counters["invalid_message_count"], 1)
        self.assertGreaterEqual(len(sleeps), 1)

    async def test_should_reconnect_when_heartbeat_becomes_stale(self) -> None:
        stop_event = asyncio.Event()
        resync_reasons: list[str] = []

        async def fake_sleep(duration: float) -> None:
            _ = duration

        async def on_signal(payload: dict[str, Any]) -> None:
            _ = payload
            stop_event.set()

        async def on_resync(reason: str) -> None:
            resync_reasons.append(reason)

        first_socket = _FakeWebSocket(
            messages=[
                '{"type":"ack","payload":{"status":"ok"}}',
                (0.03, '{"type":"system","payload":{"event":"notice"}}'),
            ]
        )
        second_socket = _FakeWebSocket(
            messages=[
                '{"type":"ack","payload":{"status":"ok"}}',
                '{"type":"heartbeat","payload":{"status":"ok"}}',
                '{"type":"signal","payload":{"signal_id":"sig-3"}}',
            ]
        )
        sockets = [first_socket, second_socket]

        def connect_func(*args, **kwargs):
            _ = args
            _ = kwargs
            if sockets:
                return sockets.pop(0)
            return second_socket

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=1,
            heartbeat_timeout_seconds=0.5,
            reconnect_initial_delay_seconds=0.01,
            reconnect_max_delay_seconds=0.05,
            heartbeat_stale_seconds=0.01,
            on_signal=on_signal,
            on_resync=on_resync,
            connect_func=connect_func,
            sleep_func=fake_sleep,
        )

        await client.run(stop_event=stop_event)

        counters = client.get_counters()
        self.assertEqual(resync_reasons, ["startup", "reconnect"])
        self.assertGreaterEqual(counters["reconnect_count"], 1)

    async def test_should_filter_unsupported_message_types(self) -> None:
        stop_event = asyncio.Event()
        signals: list[str] = []

        async def on_signal(payload: dict[str, Any]) -> None:
            signals.append(str(payload["signal_id"]))
            stop_event.set()

        async def on_resync(reason: str) -> None:
            _ = reason

        socket = _FakeWebSocket(
            messages=[
                '{"type":"ack","payload":{"status":"ok"}}',
                '{"type":"unknown","payload":{"x":1}}',
                "not-json",
                '{"type":"system","payload":{"event":"noop"}}',
                '{"type":"heartbeat","payload":{"status":"ok"}}',
                '{"type":"signal","payload":{"signal_id":"sig-4"}}',
            ]
        )

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=8,
            on_signal=on_signal,
            on_resync=on_resync,
            connect_func=lambda *args, **kwargs: socket,
        )

        await client.run(stop_event=stop_event)

        counters = client.get_counters()
        self.assertEqual(signals, ["sig-4"])
        self.assertGreaterEqual(counters["invalid_message_count"], 2)


if __name__ == "__main__":
    unittest.main()
