#!/usr/bin/env python3
"""Run a quick signal -> executor smoke test (dry-run).

- Reads provisioning state from `.e2e_provision_state.json` for `bot_id`.
- Sets minimal environment variables required by `ExecutorConfig`.
- Calls `CcxtSignalExecutor.execute_signal` for two synthetic signals.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import logging

from local_executor.config import ExecutorConfig
from local_executor.execution import CcxtSignalExecutor


STATE_FILE = Path(__file__).resolve().parent / ".e2e_provision_state.json"


def load_state() -> dict:
    if not STATE_FILE.exists():
        raise RuntimeError(f"Provision state not found: {STATE_FILE}")
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def prepare_env_from_state(state: dict) -> None:
    # Minimal envs required by ExecutorConfig.from_env
    os.environ.setdefault("BOT_ID", state.get("bot_id", "test-bot"))
    # WS url/token unused for direct executor call, but set for completeness
    os.environ.setdefault("SYSTEM_WS_URL", state.get("base_url", "ws://localhost:8080/ws/signals"))
    os.environ.setdefault("SYSTEM_WS_TOKEN", state.get("ws_token", "ws_test_token"))

    # Exchange dummy values for dry-run
    os.environ.setdefault("EXCHANGE_ID", "binance")
    os.environ.setdefault("EXCHANGE_API_KEY", "testkey")
    os.environ.setdefault("EXCHANGE_API_SECRET", "testsecret")
    os.environ.setdefault("EXCHANGE_SANDBOX", "true")
    os.environ.setdefault("DEFAULT_ORDER_AMOUNT", "0.001")
    os.environ.setdefault("EXECUTION_MODE", "dry-run")
    os.environ.setdefault("LOG_LEVEL", "INFO")


async def run_smoke():
    state = load_state()
    prepare_env_from_state(state)

    # configure logging
    logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
    logger = logging.getLogger("signal-smoke")

    config = ExecutorConfig.from_env()
    executor = CcxtSignalExecutor(config=config, logger=logger)

    # two signals: market open and limit close
    signals = [
        {"signal_id": "smoke-open-1", "action": "OPEN_LONG", "symbol": "BTCUSDT", "amount": 0.001},
        {"signal_id": "smoke-close-1", "action": "CLOSE_LONG", "symbol": "BTCUSDT", "order_type": "limit", "price": 100000.0, "amount": 0.001},
    ]

    for s in signals:
        logger.info("Executing synthetic signal: %s", s["signal_id"])
        result = await executor.execute_signal(s)
        logger.info("Result: mode=%s order_id=%s errors=%s details=%s", result.mode, result.order_id, result.errors, result.details)


def main():
    asyncio.run(run_smoke())


if __name__ == "__main__":
    main()
