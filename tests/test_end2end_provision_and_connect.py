from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "end2end_provision_and_connect.py"


@pytest.fixture(scope="module")
def provision_script():
    spec = importlib.util.spec_from_file_location("end2end_provision_and_connect", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_extract_ws_token_from_nested_subscription_response(provision_script):
    payload = {
        "subscriptions": [
            {"botId": "bot-other", "wsToken": "ws_other"},
            {"botId": "bot_1", "wsToken": "ws_target"},
        ]
    }

    assert provision_script._extract_ws_token(payload, bot_id="bot_1") == "ws_target"


def test_subscribe_bot_recovers_existing_subscription(monkeypatch, provision_script):
    class Response:
        def __init__(self, status_code: int, json_data: dict, text: str):
            self.status_code = status_code
            self._json_data = json_data
            self.text = text

        def json(self):
            return self._json_data

        def raise_for_status(self):
            raise AssertionError("raise_for_status should not be called when recovery succeeds")

    post_calls = []
    get_calls = []

    def fake_post(url, headers=None, timeout=None):
        post_calls.append((url, headers, timeout))
        return Response(403, {"message": "Forbidden"}, '{"message":"Forbidden"}')

    def fake_get(url, headers=None, timeout=None):
        get_calls.append((url, headers, timeout))
        if url.endswith("/api/v1/subscriptions/bot_1/active"):
            return Response(
                200,
                [{"botId": "bot_1", "wsToken": "ws_existing", "status": "ACTIVE"}],
                '{"items":[{"botId":"bot_1","wsToken":"ws_existing","status":"ACTIVE"}]}'
            )
        return Response(404, {"message": "Not found"}, '{"message":"Not found"}')

    monkeypatch.setattr(provision_script.requests, "post", fake_post)
    monkeypatch.setattr(provision_script.requests, "get", fake_get)

    result = provision_script.subscribe_bot("https://example.test", "trader-token", "bot_1")

    assert provision_script._extract_ws_token(result, bot_id="bot_1") == "ws_existing"
    assert len(post_calls) == 1
    assert any(call[0].endswith("/api/v1/subscriptions/bot_1/active") for call in get_calls)
