from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class ExecutorConfig:
    ws_url: str
    ws_token: str
    cms_base_url: str
    bot_id: str
    http_token: str
    heartbeat_interval_seconds: float = 15.0
    heartbeat_timeout_seconds: float = 10.0
    reconnect_initial_delay_seconds: float = 1.0
    reconnect_max_delay_seconds: float = 30.0
    request_timeout_seconds: float = 10.0
    protocol_version: str = "1.0"
    handshake_ack_required: bool = True
    handshake_ack_timeout_seconds: float = 5.0
    heartbeat_stale_seconds: float = 45.0
    ssl_verify: bool = True
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "ExecutorConfig":
        ws_url = _required("SYSTEM_WS_URL")
        ws_token = _required("SYSTEM_WS_TOKEN")
        cms_base_url = _required("CMS_BASE_URL")
        bot_id = _required("BOT_ID")

        http_token = os.getenv("SYSTEM_HTTP_TOKEN", ws_token)

        return cls(
            ws_url=ws_url,
            ws_token=ws_token,
            cms_base_url=cms_base_url.rstrip("/"),
            bot_id=bot_id,
            http_token=http_token,
            heartbeat_interval_seconds=_float("HEARTBEAT_INTERVAL_SECONDS", 15.0),
            heartbeat_timeout_seconds=_float("HEARTBEAT_TIMEOUT_SECONDS", 10.0),
            reconnect_initial_delay_seconds=_float("RECONNECT_INITIAL_DELAY_SECONDS", 1.0),
            reconnect_max_delay_seconds=_float("RECONNECT_MAX_DELAY_SECONDS", 30.0),
            request_timeout_seconds=_float("REQUEST_TIMEOUT_SECONDS", 10.0),
            protocol_version=os.getenv("PROTOCOL_VERSION", "1.0").strip() or "1.0",
            handshake_ack_required=_bool("HANDSHAKE_ACK_REQUIRED", True),
            handshake_ack_timeout_seconds=_float("HANDSHAKE_ACK_TIMEOUT_SECONDS", 5.0),
            heartbeat_stale_seconds=_float("HEARTBEAT_STALE_SECONDS", 45.0),
            ssl_verify=_bool("SSL_VERIFY", True),
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
