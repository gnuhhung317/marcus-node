from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "remote_executor_e2e.py"


@pytest.fixture(scope="module")
def e2e_script():
    spec = importlib.util.spec_from_file_location("remote_executor_e2e", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_validate_state_requires_runtime_credentials(e2e_script):
    e2e_script.validate_state(
        {
            "bot_id": "bot-1",
            "bot_api_key": "ak-1",
            "bot_signer_secret": "sk-1",
            "ws_token": "ws-1",
        }
    )

    with pytest.raises(RuntimeError, match="bot_signer_secret"):
        e2e_script.validate_state(
            {
                "bot_id": "bot-1",
                "bot_api_key": "ak-1",
                "ws_token": "ws-1",
            }
        )


def test_signal_payload_uses_backend_required_fields(e2e_script):
    payload = e2e_script.build_signal_payload(
        bot_id="bot-1",
        signal_id="sig-1",
        amount=0.001,
        symbol="BTCUSDT",
        action="OPEN_SHORT",
        market_type="FUTURE",
        order_type="MARKET",
        entry=76823.7,
    )

    assert payload["signalId"] == "sig-1"
    assert payload["botId"] == "bot-1"
    assert payload["marketType"] == "FUTURE"
    assert payload["orderType"] == "MARKET"
    assert payload["action"] == "OPEN_SHORT"
    assert payload["entry"] == 76823.7
    assert payload["generatedTimestamp"]


def test_canonical_json_matches_sdk_network_shape(e2e_script):
    payload = {"b": 2, "a": "x", "metadata": {"z": 1}}

    assert e2e_script.canonical_json(payload) == json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def test_mask_secret_preserves_edges(e2e_script):
    assert e2e_script._mask_secret("1234567890abcdef") == "1234...cdef"
    assert e2e_script._mask_secret("short") == "***"


def test_redact_text_masks_known_secrets(e2e_script):
    assert e2e_script.redact_text(
        "token=abcdefgh12345678 visible",
        ["abcdefgh12345678"],
    ) == "token=abcd...5678 visible"
