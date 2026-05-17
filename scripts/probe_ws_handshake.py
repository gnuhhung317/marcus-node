#!/usr/bin/env python3
"""Probe WebSocket handshake status codes for common dispatcher paths.

Reads `.e2e_provision_state.json` for host and token, then tries candidate paths.
Prints exception status codes and response headers to help diagnose InvalidStatusCode.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import websockets
from websockets.exceptions import InvalidStatusCode


STATE = Path(__file__).resolve().parent / ".e2e_provision_state.json"


def load_state() -> dict:
    if not STATE.exists():
        raise RuntimeError(f"State file not found: {STATE}")
    return json.loads(STATE.read_text(encoding="utf-8"))


async def try_connect(url: str, token: str) -> None:
    print(f"Trying: {url}")
    try:
        async with websockets.connect(url, extra_headers={"Authorization": f"Bearer {token}"}, ping_interval=None, ping_timeout=None) as ws:
            print(f"Connected successfully to {url}")
            await ws.close()
    except InvalidStatusCode as e:
        # websockets exposes the status code on this exception
        code = getattr(e, "status_code", None)
        print(f"InvalidStatusCode: {code} for {url} -> {e}")
    except Exception as e:
        print(f"Error connecting to {url}: {type(e).__name__}: {e}")


async def main():
    state = load_state()
    token = state.get("ws_token") or state.get("subscription_response", {}).get("wsToken")
    if not token:
        raise RuntimeError("ws_token not found in state file")

    # Host and port from user-provided dispatcher
    host = "171.244.195.150:8081"

    candidates = [
        "/ws/signals",
        "/ws/executor/events",
        "/ws/executor",
        "/ws",
        "/socket",
    ]

    for p in candidates:
        url = f"ws://{host}{p}"
        await try_connect(url, token)


if __name__ == "__main__":
    asyncio.run(main())
