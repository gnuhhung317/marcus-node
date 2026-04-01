from __future__ import annotations

# pyright: reportMissingImports=false

import io
import sys
import unittest
from pathlib import Path
from urllib import error

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_executor.resync_client import PositionSyncClient


class _FakeResponse:
    def __init__(self, body: str) -> None:
        self._body = body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return self._body.encode("utf-8")


class PositionSyncClientTest(unittest.TestCase):
    def test_should_fetch_current_position_payload(self) -> None:
        captured: dict[str, str] = {}

        def fake_urlopen(req, timeout, context):
            captured["url"] = req.full_url
            captured["auth"] = req.headers.get("Authorization", "")
            _ = timeout
            _ = context
            return _FakeResponse('{"position":{"symbol":"BTCUSDT","side":"LONG"}}')

        client = PositionSyncClient(
            base_url="http://localhost:8080",
            token="system-token",
            timeout_seconds=5,
            urlopen_func=fake_urlopen,
        )

        payload = client.fetch_current_position("bot-01")

        self.assertEqual(
            captured["url"],
            "http://localhost:8080/api/v1/bots/bot-01/current-position",
        )
        self.assertEqual(captured["auth"], "Bearer system-token")
        self.assertIn("position", payload)

    def test_should_return_empty_payload_when_not_found(self) -> None:
        def fake_urlopen(req, timeout, context):
            _ = req
            _ = timeout
            _ = context
            raise error.HTTPError(
                url="http://localhost/api/v1/bots/bot-01/current-position",
                code=404,
                msg="Not Found",
                hdrs=None,
                fp=io.BytesIO(b""),
            )

        client = PositionSyncClient(
            base_url="http://localhost:8080",
            token="system-token",
            timeout_seconds=5,
            urlopen_func=fake_urlopen,
        )

        payload = client.fetch_current_position("bot-01")
        self.assertEqual(payload, {})


if __name__ == "__main__":
    unittest.main()
