from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any

from local_executor.ws_client import ResilientWebSocketClient


class _HandshakeWebSocket:
    def __init__(self, incoming_frames: list[str]) -> None:
        self.incoming_frames = incoming_frames[:]  # copy
        self.sent_frames: list[str] = []

    async def send(self, frame: str) -> None:
        self.sent_frames.append(frame)

    async def recv(self) -> str:
        if not self.incoming_frames:
            raise RuntimeError("No more incoming frames")
        return self.incoming_frames.pop(0)


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "contracts"


def _load_fixture(name: str) -> dict[str, Any]:
    fixture_path = FIXTURE_DIR / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


class ContractMatrixV2Test(unittest.IsolatedAsyncioTestCase):
    async def test_handshake_v2_signature(self) -> None:
        expected_handshake = _load_fixture("ws_handshake_subscribe_v2.json")
        ack_valid = _load_fixture("ws_ack_valid_v1.json")

        client = ResilientWebSocketClient(
            ws_url="ws://test/ws",
            ws_token="secret-token",
            heartbeat_interval_seconds=5,
            heartbeat_timeout_seconds=2,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=4,
            on_signal=lambda *_: None,
            on_resync=lambda *_: None,
            bot_id="bot-fixture-01",
            protocol_version="2.0",
            handshake_ack_required=True,
            nonce_func=lambda: "fixture-nonce-01",
            timestamp_func=lambda: "2026-05-09T10:00:00Z",
        )
        websocket = _HandshakeWebSocket(incoming_frames=[json.dumps(ack_valid)])

        await client._perform_handshake(websocket)

        self.assertEqual(len(websocket.sent_frames), 1)
        actual_handshake = json.loads(websocket.sent_frames[0])
        self.assertEqual(actual_handshake["type"], "handshake")
        self.assertEqual(actual_handshake["botId"], expected_handshake["payload"]["bot_id"])
        self.assertEqual(actual_handshake["timestamp"], "2026-05-09T10:00:00Z")


if __name__ == "__main__":
    unittest.main()
