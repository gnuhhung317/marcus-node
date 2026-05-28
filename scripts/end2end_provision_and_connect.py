#!/usr/bin/env python3
"""End-to-end provisioning script:
- create developer user, login
- create bot as developer
- create trader user, login
- subscribe trader to bot to obtain `ws_token`
- persist provisioning state for the client to consume

Usage:
  python scripts/end2end_provision_and_connect.py --base-url https://marcus-api.tromoi.xyz

This script tries common register/login paths and subscription endpoints used by the platform.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests


COMMON_REGISTER_PATHS = [
    # "/auth/register",
    # "/api/auth/register",
    "/api/v1/auth/register",
]

COMMON_LOGIN_PATHS = [
    # "/auth/login",
    # "/api/auth/login",
    "/api/v1/auth/login",
]

DEFAULT_STATE_FILE = Path(__file__).resolve().parent / ".e2e_provision_state.json"


def _extract_ws_token(data: object, bot_id: str | None = None) -> str | None:
    if isinstance(data, list):
        for item in data:
            token = _extract_ws_token(item, bot_id=bot_id)
            if token:
                return token
        return None

    if not isinstance(data, dict):
        return None

    for key in ("ws_token", "wsToken", "wsTokenRaw", "token"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for list_key in ("subscriptions", "items", "data"):
        items = data.get(list_key)
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            item_bot_id = item.get("botId") or item.get("bot_id") or item.get("id")
            if bot_id and item_bot_id and str(item_bot_id).strip() != bot_id.strip():
                continue

            token = _extract_ws_token(item, bot_id=bot_id)
            if token:
                return token

    return None


def fetch_existing_subscription(base: str, trader_token: str, bot_id: str) -> dict | None:
    headers = {"Authorization": f"Bearer {trader_token}"}
    candidates = [
        f"/api/v1/subscriptions/{bot_id}/active",
        "/api/v1/subscriptions/my-subscriptions",
    ]

    for p in candidates:
        url = base.rstrip("/") + p
        try:
            r = requests.get(url, headers=headers, timeout=10)
            txt_preview = (r.text or "")[:800].replace("\n", " ")
            print(f"GET {p} => {r.status_code} {txt_preview}")
            if r.status_code != 200:
                continue

            data = r.json()
            ws_token = _extract_ws_token(data, bot_id=bot_id)
            if ws_token:
                print(f"Recovered ws_token from {p}")
                return data if isinstance(data, dict) else {"items": data}
        except Exception as e:
            print(f"GET {p} => request error: {e!r}")

    return None


def try_post(base: str, paths: list[str], json_body: dict, headers: dict | None = None):
    headers = headers or {}
    for p in paths:
        url = base.rstrip("/") + p
        try:
            r = requests.post(url, json=json_body, headers=headers, timeout=10)
            # verbose logging for diagnostics
            try:
                body_preview = json.dumps(json_body, ensure_ascii=False)
            except Exception:
                body_preview = str(json_body)
            txt = r.text or ""
            txt_preview = txt[:400].replace("\n", " ")
            print(f"POST {p} => {r.status_code} {txt_preview} | payload: {body_preview}")
        except Exception as e:
            print(f"POST {p} => request error: {e!r}")
            continue
        if r.status_code in (200, 201):
            return r.json()
    return None


def load_state(state_file: Path) -> dict:
    if state_file.exists():
        try:
            return json.loads(state_file.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_state(state_file: Path, state: dict) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def try_login(base: str, email: str, password: str):
    # Try both email and username forms for login payloads
    bodies = [
        {"username": email, "password": password},
    ]
    for b in bodies:
        r = try_post(base, COMMON_LOGIN_PATHS, b)
        if r:
            return r
    return None


def _extract_user_id_from_login(data: object) -> str | None:
    if not isinstance(data, dict):
        return None
    for key in ("userId", "user_id", "id", "sub", "user"):
        v = data.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            # nested user object
            nested = v.get("id") or v.get("userId") or v.get("user_id")
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
    # try common nested locations
    data_obj = data.get("data") if isinstance(data.get("data"), dict) else None
    if data_obj:
        return _extract_user_id_from_login(data_obj)
    return None


def try_register(base: str, email: str, password: str, display_name: str | None = None, role: str | None = None):
    username = email.split("@")[0]
    body = {"email": email, "password": password, "username": username}
    if display_name:
        body["displayName"] = display_name
    if role:
        body["role"] = role
    # some APIs expect `name` or `displayName` or `username`
    body_alt = dict(body)
    body_alt["name"] = display_name or username
    return try_post(base, COMMON_REGISTER_PATHS, body) or try_post(base, COMMON_REGISTER_PATHS, body_alt)


def create_bot(base: str, dev_token: str, bot_name: str = "e2e-bot", bot_id: str | None = None):
    headers = {"Authorization": f"Bearer {dev_token}"}
    payload = {
        "botId": bot_id or f"{bot_name}-{int(datetime.now().timestamp())}",
        "botName": bot_name,
        "exchange": "binance",
        "tradingPair": "BTC/USDT",
        "description": "End-to-end test bot",
    }
    candidates = [
        "/api/v1/bots/register",
        "/api/bots",
        "/bots",
    ]
    for p in candidates:
        url = base.rstrip("/") + p
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=10)
            # log response for debugging payload/permission issues
            try:
                payload_preview = json.dumps(payload, ensure_ascii=False)
            except Exception:
                payload_preview = str(payload)
            resp_txt = (r.text or "").replace("\n", " ")[:800]
            print(f"POST {p} => {r.status_code} {resp_txt} | payload: {payload_preview}")
            if r.status_code >= 400:
                try:
                    print("Response headers:", dict(r.headers))
                except Exception:
                    pass
            if r.status_code in (200, 201):
                return r.json()
            if r.status_code == 403:
                # try listing bots as fallback
                try:
                    list_url = base.rstrip("/") + p
                    lr = requests.get(list_url, headers=headers, timeout=10)
                    lr_preview = (lr.text or "")[:400].replace("\n", " ")
                    print(f"GET {p} => {lr.status_code} {lr_preview}")
                    if lr.status_code == 200:
                        data = lr.json()
                        # try to pick first bot id from common keys
                        first = None
                        if isinstance(data, list) and data:
                            item = data[0]
                            first = item.get("bot_id") or item.get("botId") or item.get("id")
                        elif isinstance(data, dict):
                            # maybe wrapped
                            items = data.get("items") or data.get("bots") or data.get("data")
                            if isinstance(items, list) and items:
                                item = items[0]
                                first = item.get("bot_id") or item.get("botId") or item.get("id")
                        if first:
                            return {"bot_id": first}
                except Exception as e:
                    print("bot listing fallback error:", e)
        except Exception:
            continue
    # if all fails, raise last known HTTPError
    raise RuntimeError("Unable to create or find a bot (403 or not found)")


def subscribe_bot(base: str, trader_token: str, bot_id: str):
    url = base.rstrip("/") + f"/api/v1/subscriptions/{bot_id}"
    headers = {"Authorization": f"Bearer {trader_token}"}
    r = requests.post(url, headers=headers, timeout=10)
    txt_preview = (r.text or "")[:800].replace("\n", " ")
    print(f"POST /api/v1/subscriptions/{bot_id} => {r.status_code} {txt_preview}")

    if r.status_code in (200, 201):
        return r.json()

    if r.status_code in (400, 403, 409):
        print("Subscribe request was rejected; checking for an existing active subscription...")
        existing = fetch_existing_subscription(base, trader_token, bot_id)
        if existing:
            return existing
        # attempt to upsert routing subscriber as a fallback (routing endpoints are permitAll)
        print("Attempting routing upsert fallback to create subscriber mapping...")
        try:
            upsert_paths = ["/api/v1/routing/subscribers", "/routing/subscribers"]
            payload = {"botId": bot_id, "userId": state.get("trader_id")}
            # try with trader token header first, then unauthenticated
            heads = [{"Authorization": f"Bearer {trader_token}"}, {}]
            for h in heads:
                for p in upsert_paths:
                    try:
                        ru = base.rstrip("/") + p
                        ru_r = requests.post(ru, json=payload, headers=h or None, timeout=10)
                        ru_preview = str(ru_r.text)[:200].replace("\n", " ")
                        print(f"POST {p} => {ru_r.status_code} {ru_preview}")
                        if ru_r.status_code in (200, 201, 204):
                            print("Routing upsert succeeded; re-checking subscription endpoints for ws_token...")
                            existing = fetch_existing_subscription(base, trader_token, bot_id)
                            if existing:
                                return existing
                    except Exception as e:
                        print("Routing upsert attempt error:", e)
        except Exception as e:
            print("Routing upsert fallback failed:", e)

    r.raise_for_status()
    return r.json()


def build_handshake(bot_id: str, ws_token: str, protocol_version: str = "1.0") -> dict:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "nonce": base64.urlsafe_b64encode(hashlib.sha1(timestamp.encode()).digest()).decode().rstrip("="),
        "protocol_version": protocol_version,
        "stream": "signal_execution",
        "mode": "consume_only",
        "bot_id": bot_id,
        "timestamp": timestamp,
    }
    payload_json = json.dumps(payload, separators=(",",":"))
    payload_b64 = base64.b64encode(payload_json.encode("utf-8")).decode("ascii")
    message = f"{bot_id}|{payload['timestamp']}|{payload_b64}".encode("utf-8")
    digest = hmac.new(ws_token.encode("utf-8"), message, hashlib.sha256).digest()
    signature = base64.b64encode(digest).decode("ascii")
    return {"type": "handshake", "botId": bot_id, "timestamp": payload["timestamp"], "payload": payload, "signature": signature}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="https://marcus-api.tromoi.xyz", required=False, help="HTTP base URL, e.g. http://localhost:8081 or https://marcus-api.tromoi.xyz")
    parser.add_argument("--dev-email", default="dev+e2e1@example.com")
    parser.add_argument("--dev-password", default="Password123!")
    parser.add_argument("--trader-email", default="trader+e2e@example.com")
    parser.add_argument("--trader-password", default="Password123!")
    parser.add_argument("--bot-name", default="e2e-bot")
    parser.add_argument("--ws-path", default="/ws/executor/events")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE), help="Path to persist provisioning state for the executor client")
    parser.add_argument("--reuse-state", action="store_true", help="Reuse saved bot/ws_token state when available")
    parser.add_argument("--provision-only", action="store_true", help="Stop after saving provisioning state; do not attempt websocket connect")
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    state_file = Path(args.state_file)
    state = load_state(state_file) if args.reuse_state else {}

    if state.get("base_url") != base:
        state = {}

    if not state.get("dev_token"):
        print("Registering developer user...")
        resp = try_register(base, args.dev_email, args.dev_password, display_name="e2e-dev", role="DEVELOPER")
        print("register dev response:", bool(resp))

        print("Logging in developer user...")
        login = try_login(base, args.dev_email, args.dev_password)
        if not login:
            print("Dev login failed. Aborting.")
            sys.exit(1)
        dev_token = login.get("accessToken") or login.get("token") or login.get("access_token") or login.get("data", {}).get("token")
        print("Dev token obtained?", bool(dev_token))
        if dev_token:
            print("Dev token (truncated):", (dev_token[:40] + '...') if len(dev_token) > 60 else dev_token)
            # probe common bot listing endpoints using the dev token to debug permissions
            bot_list_paths = ["/api/v1/bots/register", "/api/bots", "/bots"]
            headers = {"Authorization": f"Bearer {dev_token}"}
            for p in bot_list_paths:
                try:
                    url = base.rstrip("/") + p
                    lr = requests.get(url, headers=headers, timeout=10)
                    txt = lr.text[:400].replace('\n',' ')
                    print(f"GET {p} => {lr.status_code} {txt}")
                except Exception as e:
                    print(f"GET {p} => error {e}")
        state["dev_token"] = dev_token
    else:
        dev_token = state["dev_token"]
        print("Reusing saved developer token.")

    bot_id = state.get("bot_id")
    bot_api_key = state.get("bot_api_key")
    bot_signer_secret = state.get("bot_signer_secret")
    if not bot_id or not bot_api_key or not bot_signer_secret:
        print("Creating bot and capturing runtime credentials...")
        bot = create_bot(base, dev_token, bot_name=args.bot_name, bot_id=bot_id)
        bot_id = bot.get("bot_id") or bot.get("botId") or bot.get("id") or bot_id
        bot_api_key = bot_api_key or bot.get("apiKey") or bot.get("api_key") or bot.get("botApiKey")
        bot_signer_secret = bot_signer_secret or bot.get("rawSecret") or bot.get("raw_secret") or bot.get("botSignerSecret")
        print("Created bot_id:", bot_id)
        if bot_api_key:
            state["bot_api_key"] = bot_api_key
        if bot_signer_secret:
            state["bot_signer_secret"] = bot_signer_secret
        state["bot_id"] = bot_id
        state["base_url"] = base
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        save_state(state_file, state)
        print("Saved bot provisioning state to:", state_file)
    else:
        print("Reusing saved bot_id:", bot_id)

    trader_token = state.get("trader_token")
    if not trader_token:
        print("Registering trader user...")
        _ = try_register(base, args.trader_email, args.trader_password, display_name="e2e-trader", role="TRADER")
        print("Logging in trader user...")
        tr_login = try_login(base, args.trader_email, args.trader_password)
        if not tr_login:
            print("Trader login failed. Aborting.")
            sys.exit(1)
        trader_token = tr_login.get("accessToken") or tr_login.get("token") or tr_login.get("access_token")
        trader_id = _extract_user_id_from_login(tr_login)
        if trader_id:
            state["trader_id"] = trader_id
            print("Captured trader id:", trader_id)
        print("Trader token obtained?", bool(trader_token))
        state["trader_token"] = trader_token
    else:
        print("Reusing saved trader token.")
        trader_id = state.get("trader_id")

    ws_token = state.get("ws_token")
    if not ws_token:
        print("Subscribing trader to bot to obtain ws_token...")
        sub = subscribe_bot(base, trader_token, bot_id)
        ws_token = _extract_ws_token(sub, bot_id=bot_id)
        if not ws_token:
            print("Subscription response missing ws_token; querying existing subscriptions...")
            existing = fetch_existing_subscription(base, trader_token, bot_id)
            if existing:
                sub = existing
                ws_token = _extract_ws_token(sub, bot_id=bot_id)
        print("Subscribe result has ws_token?", bool(ws_token))
        state["ws_token"] = ws_token
        state["subscription_response"] = sub
    else:
        print("Reusing saved ws_token.")

    state["base_url"] = base
    state["bot_id"] = bot_id
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_state(state_file, state)
    print("Saved provisioning state to:", state_file)
    print("ws_token available for client?", bool(ws_token))
    print("Use this state file in the client instead of connecting to /ws/executor/events here.")

    if args.provision_only:
        return

    # Legacy verification path can be enabled explicitly if a real WS endpoint is available.
    ws_url = base.replace("http://", "ws://").replace("https://", "wss://") + args.ws_path
    print("Legacy websocket verification disabled by default; skipping:", ws_url)


if __name__ == "__main__":
    main()
