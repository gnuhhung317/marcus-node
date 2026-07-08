#!/usr/bin/env python3
"""Remote end-to-end runner for the Marcus local executor.

The runner intentionally reads exchange credentials from environment variables
only. It never writes them to the provisioning state file and redacts configured
secret values from child-process logs.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import os
import queue
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import websockets
import requests


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE_FILE = ROOT / "scripts" / ".e2e_provision_state.json"
DEFAULT_DB_PATH = ROOT / "scripts" / ".e2e_executor_state.db"
PROVISION_SCRIPT = ROOT / "scripts" / "end2end_provision_and_connect.py"
EXECUTOR_SCRIPT = ROOT / "local_executor.py"

REQUIRED_STATE_KEYS = ("bot_id", "bot_api_key", "bot_signer_secret", "ws_token")
EXCHANGE_SECRET_ENV_KEYS = ("EXCHANGE_API_KEY", "EXCHANGE_API_SECRET", "EXCHANGE_API_PASSPHRASE")


@dataclass(slots=True)
class HttpResult:
    status: int
    body: str
    json_body: Any | None


class ChildLogReader:
    def __init__(self, process: subprocess.Popen[str], secrets: list[str]) -> None:
        self._process = process
        self._secrets = [secret for secret in secrets if secret]
        self._queue: queue.Queue[str] = queue.Queue()
        self._lines: list[str] = []
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    def _read_loop(self) -> None:
        assert self._process.stdout is not None
        for raw_line in self._process.stdout:
            line = self._redact(raw_line.rstrip("\n"))
            self._lines.append(line)
            self._queue.put(line)
            print(f"[executor] {line}", flush=True)

    def _redact(self, text: str) -> str:
        result = text
        for secret in self._secrets:
            result = result.replace(secret, _mask_secret(secret))
        return result

    def wait_for_any(self, needles: tuple[str, ...], timeout_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        if any(any(needle in line for needle in needles) for line in self._lines):
            return True
        while time.monotonic() < deadline:
            try:
                line = self._queue.get(timeout=0.25)
            except queue.Empty:
                if self._process.poll() is not None:
                    return any(any(needle in line for needle in needles) for line in self._lines)
                continue
            if any(needle in line for needle in needles):
                return True
        return False

    @property
    def lines(self) -> list[str]:
        return list(self._lines)


def main() -> None:
    args = build_parser().parse_args()
    if args.env_file:
        load_env_file(Path(args.env_file))

    state_file = Path(args.state_file)
    db_path = Path(args.executor_db_path)

    if db_path.exists():
        db_path.unlink()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{db_path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()

    if not args.skip_provision:
        run_provisioning(args, state_file)

    state = load_state(state_file)
    validate_state(state)

    asyncio.run(probe_handshake(args.ws_url, state["bot_id"], state["ws_token"], args.ws_timeout_seconds))
    if args.probe_only:
        print("Probe-only mode completed after provisioning and websocket handshake.")
        return

    executor_process: subprocess.Popen[str] | None = None
    log_reader: ChildLogReader | None = None
    signal_id = args.signal_id or f"e2e-{int(time.time())}-{uuid4().hex[:8]}"

    try:
        if not args.skip_executor:
            executor_process, log_reader = start_executor(args, state, db_path)
            connected = log_reader.wait_for_any(
                ("Handshake acknowledged", "Connected to system WebSocket"),
                args.executor_start_timeout_seconds,
            )
            if not connected:
                raise RuntimeError("executor did not complete websocket handshake before timeout")

        payload = build_signal_payload(
            bot_id=state["bot_id"],
            signal_id=signal_id,
            amount=args.amount,
            symbol=args.symbol,
            action=args.signal_action,
            market_type=args.market_type,
            order_type=args.order_type,
            entry=args.entry,
        )
        print(f"Publishing signed signal signal_id={signal_id} symbol={args.symbol} amount={args.amount}")
        send_result = send_signed_json(
            url=f"{args.base_url.rstrip('/')}/api/v1/signals",
            api_key=state["bot_api_key"],
            signer_secret=state["bot_signer_secret"],
            payload=payload,
            timeout_seconds=args.http_timeout_seconds,
        )
        require_status(send_result, {200}, "signal publish")

        if not args.skip_idempotency:
            duplicate = send_signed_json(
                url=f"{args.base_url.rstrip('/')}/api/v1/signals",
                api_key=state["bot_api_key"],
                signer_secret=state["bot_signer_secret"],
                payload=payload,
                timeout_seconds=args.http_timeout_seconds,
            )
            require_status(duplicate, {400, 409, 422}, "duplicate signal rejection")

        if not args.skip_invalid_payload:
            invalid_payload = {
                "signalId": "",
                "botId": state["bot_id"],
                "symbol": args.symbol,
                "action": "OPEN_LONG",
                "orderType": "MARKET",
                "generatedTimestamp": generated_timestamp(),
            }
            invalid = send_signed_json(
                url=f"{args.base_url.rstrip('/')}/api/v1/signals",
                api_key=state["bot_api_key"],
                signer_secret=state["bot_signer_secret"],
                payload=invalid_payload,
                timeout_seconds=args.http_timeout_seconds,
            )
            require_status(invalid, {400, 422}, "invalid payload rejection")

        if not args.skip_backend_poll:
            backend_signal = poll_backend_signal(
                base_url=args.base_url,
                signal_id=signal_id,
                timeout_seconds=args.backend_poll_timeout_seconds,
                interval_seconds=2.0,
            )
            print(f"Backend signal lookup returned {len(backend_signal)} item(s)")

        if not args.skip_executor:
            state_row = poll_local_signal_state(
                db_path=db_path,
                signal_id=signal_id,
                timeout_seconds=args.execution_timeout_seconds,
                interval_seconds=2.0,
            )
            signal_state = str(state_row.get("signal_state", ""))
            print(f"Local executor state for {signal_id}: {json.dumps(state_row, sort_keys=True)}")
            if signal_state not in {"OPEN", "CLOSED", "REJECTED"}:
                raise RuntimeError(f"unexpected local signal_state={signal_state!r}")

            if signal_state == "REJECTED":
                print("Signal reached executor but sandbox exchange rejected execution; inspect executor log above.")

            if log_reader is not None:
                log_reader.wait_for_any(("Balance synced", "Balance sync failed"), 15.0)

    finally:
        if executor_process is not None:
            stop_process(executor_process)

    print("Remote executor e2e completed.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run remote Marcus local-executor e2e checks.")
    parser.add_argument("--base-url", default="https://marcus-api.tromoi.xyz")
    parser.add_argument("--ws-url", default="ws://171.244.195.150:8081/ws/executor")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--executor-db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--env-file", default=None, help="Optional local env file containing exchange credentials.")
    parser.add_argument("--dev-email", default="demo-dev@gmail.com")
    parser.add_argument("--dev-password", default="Password123!")
    parser.add_argument("--trader-email", default="demo-trader@gmail.com")
    parser.add_argument("--trader-password", default="Password123!")
    parser.add_argument("--bot-name", default="e2e-bot")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--amount", type=float, default=0.001)
    parser.add_argument("--signal-action", default="OPEN_SHORT")
    parser.add_argument("--market-type", default="FUTURE")
    parser.add_argument("--order-type", default="MARKET")
    parser.add_argument("--entry", type=float, default=76823.7)
    parser.add_argument("--signal-id", default=None)
    parser.add_argument("--exchange-id", default=os.getenv("EXCHANGE_ID", "binance"))
    parser.add_argument("--default-order-type", default="market")
    parser.add_argument("--log-level", default="DEBUG")
    parser.add_argument("--skip-provision", action="store_true")
    parser.add_argument("--skip-executor", action="store_true")
    parser.add_argument("--skip-idempotency", action="store_true")
    parser.add_argument("--skip-invalid-payload", action="store_true")
    parser.add_argument("--skip-backend-poll", action="store_true")
    parser.add_argument("--probe-only", action="store_true", help="Stop after provisioning state validation and websocket handshake.")
    parser.add_argument("--ws-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--http-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--executor-start-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--execution-timeout-seconds", type=float, default=75.0)
    parser.add_argument("--backend-poll-timeout-seconds", type=float, default=30.0)
    return parser


def load_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def run_provisioning(args: argparse.Namespace, state_file: Path) -> None:
    command = [
        sys.executable,
        str(PROVISION_SCRIPT),
        "--base-url",
        args.base_url,
        "--dev-email",
        args.dev_email,
        "--dev-password",
        args.dev_password,
        "--trader-email",
        args.trader_email,
        "--trader-password",
        args.trader_password,
        "--bot-name",
        args.bot_name,
        "--state-file",
        str(state_file),
        "--reuse-state",
        "--provision-only",
    ]
    print(f"Running provisioning script state_file={state_file}")
    completed = subprocess.run(
        command,
        cwd=str(ROOT),
        text=True,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    secrets: list[str] = []
    if state_file.exists():
        try:
            state = load_state(state_file)
            secrets.extend(str(state.get(key, "")) for key in REQUIRED_STATE_KEYS)
            secrets.extend(str(state.get(key, "")) for key in ("dev_token", "trader_token"))
        except Exception:
            pass
    print(redact_text(completed.stdout or "", secrets), end="")
    if completed.returncode != 0:
        raise RuntimeError(f"provisioning failed with exit code {completed.returncode}")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"state file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"state file must contain a JSON object: {path}")
    return data


def validate_state(state: dict[str, Any]) -> None:
    missing = [key for key in REQUIRED_STATE_KEYS if not str(state.get(key) or "").strip()]
    if missing:
        raise RuntimeError(f"provisioning state missing required keys: {', '.join(missing)}")


async def probe_handshake(ws_url: str, bot_id: str, ws_token: str, timeout_seconds: float) -> None:
    print(f"Probing websocket handshake url={ws_url} bot_id={bot_id}")
    async with websockets.connect(
        ws_url,
        extra_headers={"Authorization": f"Bearer {ws_token}"},
        ping_interval=None,
        ping_timeout=None,
        close_timeout=2,
    ) as websocket:
        frame = build_handshake_frame(bot_id=bot_id, ws_token=ws_token)
        await websocket.send(json.dumps(frame, separators=(",", ":")))
        raw = await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds)
        message = json.loads(raw)
        payload = message.get("payload") if isinstance(message, dict) else None
        if not isinstance(payload, dict):
            raise RuntimeError(f"handshake response missing payload: {raw}")
        status = str(payload.get("status", "")).lower()
        ack_type = str(payload.get("ack_type") or payload.get("for") or "").lower()
        if message.get("type") not in {"ack", "handshake-ack"} or status != "ok" or ack_type != "handshake":
            raise RuntimeError(f"unexpected handshake response: {raw}")
        print("WebSocket handshake accepted.")


def build_handshake_frame(bot_id: str, ws_token: str) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "nonce": uuid4().hex,
        "protocol_version": "1.0",
        "stream": "signal_execution",
        "mode": "consume_only",
        "bot_id": bot_id,
        "timestamp": timestamp,
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    payload_b64 = base64.b64encode(payload_json.encode("utf-8")).decode("ascii")
    message = f"{bot_id}|{timestamp}|{payload_b64}".encode("utf-8")
    signature = base64.b64encode(hmac.new(ws_token.encode("utf-8"), message, hashlib.sha256).digest()).decode("ascii")
    return {"type": "handshake", "botId": bot_id, "timestamp": timestamp, "payload": payload, "signature": signature}


def start_executor(
    args: argparse.Namespace,
    state: dict[str, Any],
    db_path: Path,
) -> tuple[subprocess.Popen[str], ChildLogReader]:
    required_exchange_env = ("EXCHANGE_API_KEY", "EXCHANGE_API_SECRET")
    missing = [key for key in required_exchange_env if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"missing exchange credential env vars: {', '.join(missing)}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "SYSTEM_WS_URL": args.ws_url,
            "SYSTEM_WS_TOKEN": str(state["ws_token"]),
            "BOT_ID": str(state["bot_id"]),
            "EXECUTION_MODE": "live",
            "EXCHANGE_SANDBOX": "true",
            "EXCHANGE_ID": args.exchange_id,
            "DEFAULT_ORDER_AMOUNT": str(args.amount),
            "DEFAULT_ORDER_TYPE": args.default_order_type,
            "LOG_LEVEL": args.log_level,
            "EXECUTOR_DB_PATH": str(db_path),
            "PYTHONUNBUFFERED": "1",
        }
    )

    secrets = [str(state.get("ws_token", ""))]
    secrets.extend(str(os.getenv(key, "")) for key in EXCHANGE_SECRET_ENV_KEYS)

    print(f"Starting local executor db={db_path}")
    process = subprocess.Popen(
        [sys.executable, str(EXECUTOR_SCRIPT)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    return process, ChildLogReader(process, secrets)


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def build_signal_payload(
    *,
    bot_id: str,
    signal_id: str,
    amount: float,
    symbol: str,
    action: str,
    market_type: str,
    order_type: str,
    entry: float | None,
) -> dict[str, Any]:
    return {
        "signalId": signal_id,
        "botId": bot_id,
        "symbol": symbol,
        "action": action,
        "marketType": market_type,
        "orderType": order_type,
        "amount": amount,
        "entry": entry,
        "generatedTimestamp": generated_timestamp(),
        "metadata": {"source": "remote_executor_e2e"},
    }


def generated_timestamp() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def send_signed_json(
    *,
    url: str,
    api_key: str,
    signer_secret: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> HttpResult:
    timestamp = str(int(time.time() * 1000))
    body = canonical_json(payload)
    signature = hmac.new(
        signer_secret.encode("utf-8"),
        f"{timestamp}\n{body}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    try:
        response = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "X-Bot-Api-Key": api_key,
                "X-Timestamp": timestamp,
                "X-Signature": signature,
            },
            data=body,
            timeout=timeout_seconds,
        )
        return parse_http_result(response.status_code, response.text or "")
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        if response is not None:
            return parse_http_result(getattr(response, "status_code", 0) or 0, getattr(response, "text", "") or "")
        raise


def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def parse_http_result(status: int, body: str) -> HttpResult:
    parsed: Any | None = None
    if body.strip():
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = None
    return HttpResult(status=status, body=body, json_body=parsed)


def require_status(result: HttpResult, expected: set[int], context: str) -> None:
    if result.status not in expected:
        preview = result.body[:500].replace("\n", " ")
        raise RuntimeError(f"{context} returned HTTP {result.status}, expected {sorted(expected)} body={preview}")
    print(f"{context}: HTTP {result.status}")


def poll_backend_signal(
    *,
    base_url: str,
    signal_id: str,
    timeout_seconds: float,
    interval_seconds: float,
) -> list[Any]:
    deadline = time.monotonic() + timeout_seconds
    url = f"{base_url.rstrip('/')}/api/v1/signals"
    last_status = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(
                url,
                params={"signalId": signal_id, "limit": 1},
                timeout=10,
            )
            last_status = response.status_code
            if response.status_code == 200:
                body = response.text or "[]"
                data = json.loads(body or "[]")
                if isinstance(data, list) and data:
                    return data
        except Exception as exc:  # noqa: BLE001 - diagnostic polling
            last_status = f"{exc.__class__.__name__}: {exc}"
        time.sleep(interval_seconds)
    raise RuntimeError(f"backend signal lookup timed out for signal_id={signal_id} last_status={last_status}")


def poll_local_signal_state(
    *,
    db_path: Path,
    signal_id: str,
    timeout_seconds: float,
    interval_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            row = read_signal_state(db_path, signal_id)
            if row:
                return row
        except Exception as exc:  # noqa: BLE001 - diagnostic polling
            last_error = f"{exc.__class__.__name__}: {exc}"
        time.sleep(interval_seconds)
    raise RuntimeError(f"local executor state timed out for signal_id={signal_id} last_error={last_error}")


def read_signal_state(db_path: Path, signal_id: str) -> dict[str, Any] | None:
    if not db_path.exists():
        return None
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            """
            SELECT signal_id, signal_state, order_state, position_state, order_id, order_symbol, updated_at
            FROM execution_signals
            WHERE signal_id = ?
            """,
            (signal_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _mask_secret(value: str) -> str:
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def redact_text(text: str, secrets: list[str]) -> str:
    result = text
    for secret in secrets:
        if secret:
            result = result.replace(secret, _mask_secret(secret))
    return result


if __name__ == "__main__":
    main()
