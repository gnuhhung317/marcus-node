from __future__ import annotations

# pyright: reportMissingImports=false

import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_executor.config import ExecutorConfig
from local_executor.engine import LocalExecutorEngine


class _FakeSyncClient:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self.calls: list[str] = []

    def fetch_current_position(self, bot_id: str) -> dict[str, Any]:
        self.calls.append(bot_id)
        if self._responses:
            return self._responses.pop(0)
        return {}


class LocalExecutorEngineTest(unittest.IsolatedAsyncioTestCase):
    async def test_should_fallback_resync_when_signal_missing_position_context(self) -> None:
        received: list[dict[str, Any]] = []

        async def on_signal(payload: dict[str, Any]) -> None:
            received.append(payload)

        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            cms_base_url="http://localhost:8080",
            bot_id="bot-01",
            http_token="http-token",
        )
        engine = LocalExecutorEngine(config=config, on_signal=on_signal)
        fake_sync = _FakeSyncClient(
            responses=[
                {"position": {"symbol": "BTCUSDT", "side": "LONG"}},
                {"position": {"symbol": "ETHUSDT", "side": "SHORT"}},
            ]
        )
        engine._sync_client = fake_sync

        await engine._resync_state("startup")
        await engine._handle_signal({"signal_id": "sig-1", "action": "OPEN_LONG"})

        self.assertEqual(fake_sync.calls, ["bot-01", "bot-01"])
        self.assertEqual(len(received), 1)
        self.assertIn("position_snapshot", received[0])
        self.assertEqual(received[0]["position_snapshot"]["position"]["symbol"], "ETHUSDT")

    async def test_should_not_fallback_when_signal_has_position_context(self) -> None:
        received: list[dict[str, Any]] = []

        async def on_signal(payload: dict[str, Any]) -> None:
            received.append(payload)

        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            cms_base_url="http://localhost:8080",
            bot_id="bot-01",
            http_token="http-token",
        )
        engine = LocalExecutorEngine(config=config, on_signal=on_signal)
        fake_sync = _FakeSyncClient(
            responses=[
                {"position": {"symbol": "BTCUSDT", "side": "LONG"}},
            ]
        )
        engine._sync_client = fake_sync

        await engine._resync_state("startup")
        await engine._handle_signal(
            {
                "signal_id": "sig-2",
                "action": "CLOSE",
                "position": {"symbol": "BTCUSDT", "side": "LONG"},
            }
        )

        self.assertEqual(fake_sync.calls, ["bot-01"])
        self.assertEqual(len(received), 1)
        self.assertNotIn("position_snapshot", received[0])


if __name__ == "__main__":
    unittest.main()
