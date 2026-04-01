from __future__ import annotations

# pyright: reportMissingImports=false

import json
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_executor.ws_client import ResilientWebSocketClient


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "contracts"


def _load_fixture(name: str) -> dict[str, Any]:
    fixture_path = FIXTURE_DIR / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


class _HandshakeWebSocket:
    def __init__(self, incoming_frames: list[str]) -> None:
        self._incoming_frames = list(incoming_frames)
        self.sent_frames: list[str] = []

    async def send(self, frame: str) -> None:
        self.sent_frames.append(frame)

    async def recv(self) -> str:
        return self._incoming_frames.pop(0)


class ExecutorContractFixtureTest(unittest.IsolatedAsyncioTestCase):
    async def test_should_emit_subscribe_frame_matching_contract_fixture(self) -> None:
        expected_subscribe = _load_fixture("ws_handshake_subscribe_v1.json")
        ack_valid = _load_fixture("ws_ack_valid_v1.json")

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=4,
            on_signal=self._noop_signal,
            on_resync=self._noop_resync,
            bot_id="bot-fixture-01",
            protocol_version="1.0",
            handshake_ack_required=True,
        )
        websocket = _HandshakeWebSocket(incoming_frames=[json.dumps(ack_valid)])

        await client._perform_handshake(websocket)

        self.assertEqual(len(websocket.sent_frames), 1)
        self.assertEqual(json.loads(websocket.sent_frames[0]), expected_subscribe)

    async def test_should_decode_signal_and_heartbeat_from_fixture_contract(self) -> None:
        signal_fixture = _load_fixture("ws_signal_envelope_v1.json")
        heartbeat_fixture = _load_fixture("ws_heartbeat_v1.json")

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=4,
            on_signal=self._noop_signal,
            on_resync=self._noop_resync,
        )

        signal_result = client._decode_envelope(signal_fixture)
        heartbeat_result = client._decode_envelope(heartbeat_fixture)

        self.assertEqual(signal_result, ("signal", signal_fixture["payload"]))
        self.assertEqual(heartbeat_result, ("heartbeat", heartbeat_fixture["payload"]))

    async def test_should_validate_ack_fixture_and_reject_invalid_ack_fixture(self) -> None:
        ack_valid = _load_fixture("ws_ack_valid_v1.json")
        ack_invalid = _load_fixture("ws_ack_invalid_v1.json")

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=4,
            on_signal=self._noop_signal,
            on_resync=self._noop_resync,
        )

        self.assertTrue(client._is_valid_handshake_ack(ack_valid["payload"]))
        self.assertFalse(client._is_valid_handshake_ack(ack_invalid["payload"]))

    async def test_should_reject_unsupported_envelope_fixture(self) -> None:
        unsupported = _load_fixture("ws_unsupported_v1.json")

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=4,
            on_signal=self._noop_signal,
            on_resync=self._noop_resync,
        )

        result = client._decode_envelope(unsupported)

        self.assertIsNone(result)
        self.assertEqual(client.get_counters()["invalid_message_count"], 1)

    async def _noop_signal(self, payload: dict[str, Any]) -> None:
        _ = payload

    async def _noop_resync(self, reason: str) -> None:
        _ = reason


if __name__ == "__main__":
    unittest.main()
