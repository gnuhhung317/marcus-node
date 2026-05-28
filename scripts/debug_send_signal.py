#!/usr/bin/env python3
"""Debug helper: read provisioning state, compute HMAC signature, POST to /api/v1/signals and print request+response.

Usage (from project root):
  python local-executor-client/scripts/debug_send_signal.py --state /path/to/e2e_provision_state.json --base https://marcus-api.tromoi.xyz

Inside docker where state is mounted at /state use:
  python /app/scripts/debug_send_signal.py --state /state/e2e_provision_state.json --base https://marcus-api.tromoi.xyz
"""
from __future__ import annotations

import argparse
import json
import time
from uuid import uuid4
import hashlib
import hmac
import requests
from pathlib import Path


def canonical_json(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def generate_hmac(payload: dict, secret: str, timestamp: str) -> str:
    message = f"{timestamp}\n{canonical_json(payload)}".encode("utf-8")
    key = secret.encode("utf-8")
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--state", required=True, help="Path to e2e_provision_state.json")
    p.add_argument("--base", required=True, help="Base URL, e.g. https://marcus-api.tromoi.xyz")
    args = p.parse_args()

    state_path = Path(args.state)
    if not state_path.exists():
        print("State file not found:", state_path)
        return

    state = json.loads(state_path.read_text(encoding="utf-8"))
    bot_api_key = state.get("bot_api_key")
    bot_signer_secret = state.get("bot_signer_secret")
    bot_id = state.get("bot_id")

    print("bot_id:", bot_id)
    print("bot_api_key present:", bool(bot_api_key))
    print("bot_signer_secret present:", bool(bot_signer_secret))
    if bot_api_key:
        print("bot_api_key (truncated):", bot_api_key[:16] + "..." if len(bot_api_key) > 24 else bot_api_key)
    if bot_signer_secret:
        print("bot_signer_secret len:", len(bot_signer_secret))

    # Example payload (use the one from your error if you prefer)
    payload = {
        "botId": bot_id,
        "action": "OPEN_SHORT",
        "symbol": "BTCUSDT",
        "marketType": "FUTURE",
        "orderType": "MARKET",
        "entry": 76823.7,
        "amount": 0.00019525224637709457,
        "leverage": 1,
        "marginMode": "CROSS",
        "generatedTimestamp": "2026-05-26T06:00:00",
        "metadata": {"strategy": "funding_arbitrage"},
    }

    # signalId is required by the API; generate a UUIDv4 if missing
    if "signalId" not in payload and "signal_id" not in payload:
        payload["signalId"] = str(uuid4())

    base = args.base.rstrip("/")
    url = f"{base}/api/v1/signals"

    timestamp = str(int(time.time() * 1000))

    body = canonical_json(payload)

    headers = {"Content-Type": "application/json"}
    headers["X-Timestamp"] = timestamp
    headers["X-Bot-Api-Key"] = bot_api_key or ""

    if bot_signer_secret:
        sig = generate_hmac(payload, bot_signer_secret, timestamp)
        headers["X-Signature"] = sig
    else:
        sig = None

    print("--- Request ---")
    print("URL:", url)
    print("Headers:")
    for k, v in headers.items():
        if k == "X-Signature":
            print(f"  {k}: {v}")
        elif k == "X-Bot-Api-Key":
            print(f"  {k}: {v[:16] + '...' if v else v}")
        else:
            print(f"  {k}: {v}")
    print("Body:", body)
    print("computed signature:", sig)

    try:
        r = requests.post(url, headers=headers, data=body, timeout=10)
        print("--- Response ---")
        print(r.status_code, r.reason)
        try:
            print(r.text)
        except Exception:
            print("(no text)")
    except Exception as e:
        print("Request error:", e)


if __name__ == '__main__':
    main()
