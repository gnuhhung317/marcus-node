from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class ExecutorConfig:
    ws_url: str
    ws_token: str
    bot_id: str
    exchange_id: str
    exchange_api_key: str
    exchange_api_secret: str
    exchange_api_passphrase: str | None = None
    exchange_sandbox: bool = True
    exchange_default_type: str | None = None
    default_order_amount: float = 0.0
    default_order_type: str = "market"
    execution_mode: str = "dry-run"
    heartbeat_interval_seconds: float = 15.0
    heartbeat_timeout_seconds: float = 10.0
    reconnect_initial_delay_seconds: float = 1.0
    reconnect_max_delay_seconds: float = 30.0
    protocol_version: str = "1.0"
    handshake_ack_required: bool = True
    handshake_ack_timeout_seconds: float = 5.0
    heartbeat_stale_seconds: float = 45.0
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "ExecutorConfig":
        ws_url = _required("SYSTEM_WS_URL")
        ws_token = _required("SYSTEM_WS_TOKEN")
        bot_id = _required("BOT_ID")

        exchange_id = _required("EXCHANGE_ID")
        exchange_api_key = _required("EXCHANGE_API_KEY")
        exchange_api_secret = _required("EXCHANGE_API_SECRET")
        exchange_api_passphrase = _optional("EXCHANGE_API_PASSPHRASE")
        exchange_default_type = _optional("EXCHANGE_DEFAULT_TYPE")
        default_order_amount = _float("DEFAULT_ORDER_AMOUNT", 0.0)
        if default_order_amount <= 0:
            raise ValueError("DEFAULT_ORDER_AMOUNT must be > 0")
        execution_mode = os.getenv("EXECUTION_MODE", "dry-run").strip().lower() or "dry-run"
        if execution_mode not in {"dry-run", "live"}:
            raise ValueError("EXECUTION_MODE must be one of: dry-run, live")

        return cls(
            ws_url=ws_url,
            ws_token=ws_token,
            bot_id=bot_id,
            exchange_id=exchange_id,
            exchange_api_key=exchange_api_key,
            exchange_api_secret=exchange_api_secret,
            exchange_api_passphrase=exchange_api_passphrase,
            exchange_sandbox=_bool("EXCHANGE_SANDBOX", True),
            exchange_default_type=exchange_default_type,
            default_order_amount=default_order_amount,
            default_order_type=os.getenv("DEFAULT_ORDER_TYPE", "market").strip().lower() or "market",
            execution_mode=execution_mode,
            heartbeat_interval_seconds=_float("HEARTBEAT_INTERVAL_SECONDS", 15.0),
            heartbeat_timeout_seconds=_float("HEARTBEAT_TIMEOUT_SECONDS", 10.0),
            reconnect_initial_delay_seconds=_float("RECONNECT_INITIAL_DELAY_SECONDS", 1.0),
            reconnect_max_delay_seconds=_float("RECONNECT_MAX_DELAY_SECONDS", 30.0),
            protocol_version=os.getenv("PROTOCOL_VERSION", "1.0").strip() or "1.0",
            handshake_ack_required=_bool("HANDSHAKE_ACK_REQUIRED", True),
            handshake_ack_timeout_seconds=_float("HANDSHAKE_ACK_TIMEOUT_SECONDS", 5.0),
            heartbeat_stale_seconds=_float("HEARTBEAT_STALE_SECONDS", 45.0),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )


def _required(key: str) -> str:
    value = os.getenv(key)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {key}")
    return value.strip()


def _float(key: str, default: float) -> float:
    value = os.getenv(key)
    if value is None or not value.strip():
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {key}: {value}") from exc
    if parsed <= 0:
        raise ValueError(f"{key} must be > 0")
    return parsed


def _bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean for {key}: {value}")


def _optional(key: str) -> str | None:
    value = os.getenv(key)
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None
