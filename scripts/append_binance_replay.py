#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import ccxt
import dotenv
import psycopg2
from psycopg2.extras import execute_values

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_STATE_FILE = SCRIPT_DIR / ".append_binance_replay_state.json"
sys.path.insert(0, str(SCRIPT_DIR))

import sync_exchange_history as seed  # noqa: E402

DEFAULT_BOOTSTRAP_DAYS = 30
DEFAULT_OVERLAP_DAYS = 7
DEFAULT_TRADER_EMAIL = "demo-trader@gmail.com"
DEFAULT_DEV_EMAIL = "demo-dev@gmail.com"


@dataclass(slots=True)
class AnchorPoint:
    timestamp: datetime
    cash: float
    equity: float
    realized_pnl: float
    unrealized_pnl: float
    total_fees: float


def as_utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    raise TypeError(f"Expected datetime, got {type(value)!r}")


def load_config() -> dict[str, Any]:
    dotenv.load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    return {
        "EXCHANGE_ID": os.getenv("EXCHANGE_ID", "binance"),
        "EXCHANGE_API_KEY": _required_env("EXCHANGE_API_KEY"),
        "EXCHANGE_API_SECRET": _required_env("EXCHANGE_API_SECRET"),
        "EXCHANGE_API_PASSPHRASE": os.getenv("EXCHANGE_API_PASSPHRASE"),
        "EXCHANGE_SANDBOX": _bool_env("EXCHANGE_SANDBOX", False),
        "EXCHANGE_DEFAULT_TYPE": os.getenv("EXCHANGE_DEFAULT_TYPE", "future"),
        "DB_HOST": os.getenv("DB_HOST", "171.244.195.150"),
        "DB_PORT": int(os.getenv("DB_PORT", "5432")),
        "DB_NAME": os.getenv("DB_NAME", "signal_db"),
        "DB_USER": os.getenv("DB_USER", "user"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", "password"),
        "SYSTEM_WS_TOKEN": os.getenv("SYSTEM_WS_TOKEN", "ws_default_token"),
        "APPEND_BOOTSTRAP_DAYS": int(os.getenv("APPEND_BOOTSTRAP_DAYS", str(DEFAULT_BOOTSTRAP_DAYS))),
        "APPEND_OVERLAP_DAYS": int(os.getenv("APPEND_OVERLAP_DAYS", str(DEFAULT_OVERLAP_DAYS))),
    }


def _required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {key}")
    return value.strip()


def _bool_env(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean for {key}: {value}")


def build_exchange(config: dict[str, Any]) -> Any:
    exchange_id = str(config["EXCHANGE_ID"]).lower()
    exchange_class = getattr(ccxt, exchange_id, None)
    if not exchange_class:
        raise RuntimeError(f"CCXT does not support exchange: {exchange_id}")

    exchange_params = {
        "apiKey": config["EXCHANGE_API_KEY"],
        "secret": config["EXCHANGE_API_SECRET"],
        "enableRateLimit": True,
    }
    if exchange_id == "binance":
        exchange_params["options"] = {"defaultType": config["EXCHANGE_DEFAULT_TYPE"]}

    exchange = exchange_class(exchange_params)
    if config["EXCHANGE_SANDBOX"]:
        if hasattr(exchange, "enable_demo_trading"):
            exchange.enable_demo_trading(True)
        elif hasattr(exchange, "enableDemoTrading"):
            exchange.enableDemoTrading(True)
        else:
            exchange.set_sandbox_mode(True)

    exchange.load_markets()
    return exchange


def fetch_live_balance_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    exchange = build_exchange(config)
    now = datetime.now(timezone.utc)

    balance = exchange.fetch_balance()
    unrealized_pnl = _fetch_unrealized_pnl_sync(exchange)

    total = 0.0
    free = 0.0
    used = 0.0
    currency = "NONE"

    total_balances = balance.get("total") if isinstance(balance.get("total"), dict) else {}
    free_balances = balance.get("free") if isinstance(balance.get("free"), dict) else {}
    used_balances = balance.get("used") if isinstance(balance.get("used"), dict) else {}

    for asset in ("USDT", "USDC", "USD", "USDS", "BUSD"):
        if asset in total_balances and total_balances.get(asset) is not None:
            total = float(total_balances[asset] or 0.0)
            free_value = free_balances.get(asset) if asset in free_balances else None
            used_value = used_balances.get(asset) if asset in used_balances else None
            free = float(total if free_value is None else free_value)
            used = float(0.0 if used_value is None else used_value)
            currency = asset
            if total > 0:
                break

    if total <= 0.0 and isinstance(total_balances, dict):
        for asset, value in total_balances.items():
            if value and float(value) > 0:
                total = float(value)
                free_value = free_balances.get(asset) if asset in free_balances else None
                used_value = used_balances.get(asset) if asset in used_balances else None
                free = float(value if free_value is None else free_value)
                used = float(0.0 if used_value is None else used_value)
                currency = asset
                break

    return {
        "timestamp": now,
        "total": total,
        "free": free,
        "used": used,
        "currency": currency,
        "unrealizedPnl": unrealized_pnl,
    }


def _fetch_unrealized_pnl_sync(exchange: Any) -> float:
    try:
        if not hasattr(exchange, "fetch_positions"):
            return 0.0

        positions = exchange.fetch_positions()
        total = 0.0
        for position in positions or []:
            if not isinstance(position, dict):
                continue
            raw_pnl = position.get("unrealizedPnl")
            if raw_pnl is None:
                raw_pnl = position.get("unrealized_pnl")
            if raw_pnl is None:
                info = position.get("info")
                if isinstance(info, dict):
                    raw_pnl = info.get("unRealizedProfit") or info.get("unrealizedPnl")
            if raw_pnl is not None:
                total += float(raw_pnl)
        return total
    except Exception:
        return 0.0


def stable_uuid(*parts: Any) -> str:
    payload = "|".join(str(part) for part in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"marcus-append:{payload}"))


def parse_symbols(symbols_arg: str | None) -> list[str]:
    if not symbols_arg:
        return []
    return [symbol.strip() for symbol in symbols_arg.split(",") if symbol.strip()]


def state_scope_key(exchange_id: str, bot_id: str, user_id: str) -> str:
    return f"{str(exchange_id).lower()}::{bot_id}::{user_id}"


def load_replay_state(state_file: Path) -> dict[str, Any]:
    if not state_file.exists():
        return {"version": 1, "scopes": {}}
    try:
        loaded = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "scopes": {}}
    if not isinstance(loaded, dict):
        return {"version": 1, "scopes": {}}
    if "scopes" in loaded and isinstance(loaded["scopes"], dict):
        return loaded
    return {"version": 1, "scopes": {}}


def load_scoped_replay_state(state_file: Path, scope_key: str) -> dict[str, Any]:
    state = load_replay_state(state_file)
    scopes = state.get("scopes") if isinstance(state.get("scopes"), dict) else {}
    scope_state = scopes.get(scope_key, {})
    return scope_state if isinstance(scope_state, dict) else {}


def save_scoped_replay_state(state_file: Path, scope_key: str, scope_state: dict[str, Any]) -> None:
    state = load_replay_state(state_file)
    scopes = state.get("scopes")
    if not isinstance(scopes, dict):
        scopes = {}
    scopes[scope_key] = scope_state
    payload = {
        "version": 1,
        "scopes": scopes,
    }
    state_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def parse_state_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return as_utc_datetime(value)
    text = str(value).strip()
    if not text:
        return None
    return as_utc_datetime(datetime.fromisoformat(text.replace("Z", "+00:00")))


def select_bot_and_user(conn: Any, config: dict[str, Any], bot_id: str | None, bot_name: str | None, trader_email: str) -> tuple[str, str, str]:
    users = seed.list_users(conn)
    bots = seed.list_bots(conn)
    if not bots:
        raise RuntimeError("No active bots found in signal_db.")

    developer_user = seed.select_user_by_email(users, config.get("DEV_EMAIL", DEFAULT_DEV_EMAIL))
    developer_user_id = developer_user[0] if developer_user else None

    selected_bot_id = seed.select_bot_from_catalog(
        bots,
        bot_id=bot_id,
        bot_name_hint=bot_name,
        dev_email=config.get("DEV_EMAIL", DEFAULT_DEV_EMAIL),
        developer_user_id=developer_user_id,
    )
    if not selected_bot_id:
        raise RuntimeError("Could not resolve a bot. Pass --bot-id or --bot-name.")

    selected_user = seed.select_user_by_email(users, trader_email)
    if not selected_user:
        raise RuntimeError(f"Trader email '{trader_email}' not found in local DB.")

    selected_user_id, selected_user_email = selected_user
    return selected_bot_id, selected_user_id, selected_user_email


def load_latest_trade_ids(conn: Any, bot_id: str) -> set[str]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT trade_id FROM bot_dry_run_closed_trades WHERE bot_id = %s;", (bot_id,))
        return {str(row[0]) for row in cur.fetchall()}
    finally:
        cur.close()


def load_existing_bot_points(conn: Any, bot_id: str) -> list[dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT timestamp, cash, equity, realized_pnl, unrealized_pnl, total_fees
            FROM bot_dry_run_portfolios
            WHERE bot_id = %s
            ORDER BY timestamp ASC;
            """,
            (bot_id,),
        )
        rows = cur.fetchall()
        return [
            {
                "timestamp": as_utc_datetime(row[0]),
                "cash": float(row[1] or 0.0),
                "equity": float(row[2] or 0.0),
                "realized_pnl": float(row[3] or 0.0),
                "unrealized_pnl": float(row[4] or 0.0),
                "total_fees": float(row[5] or 0.0),
            }
            for row in rows
        ]
    finally:
        cur.close()


def load_last_trade_exit(conn: Any, bot_id: str) -> datetime | None:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MAX(exit_timestamp) FROM bot_dry_run_closed_trades WHERE bot_id = %s;",
            (bot_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def load_anchor_point(conn: Any, bot_id: str, user_id: str) -> AnchorPoint | None:
    queries = [
        (
            "SELECT timestamp, cash, equity, realized_pnl, unrealized_pnl, total_fees "
            "FROM bot_dry_run_portfolios WHERE bot_id = %s ORDER BY timestamp DESC LIMIT 1;",
            (bot_id,),
        ),
        (
            "SELECT snapshot_at, total, free, realized_pnl, unrealized_pnl "
            "FROM portfolio_aggregate_history WHERE user_id = %s ORDER BY snapshot_at DESC LIMIT 1;",
            (user_id,),
        ),
        (
            "SELECT last_sync_at, total_capital, available_balance, realized_pnl, unrealized_pnl "
            "FROM user_portfolios WHERE user_id = %s LIMIT 1;",
            (user_id,),
        ),
    ]

    newest: AnchorPoint | None = None
    cur = conn.cursor()
    try:
        for sql, params in queries:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row or row[0] is None:
                continue

            timestamp = as_utc_datetime(row[0])
            if "bot_dry_run_portfolios" in sql:
                cash = float(row[1] or 0.0)
                equity = float(row[2] or 0.0)
                realized_pnl = float(row[3] or 0.0)
                unrealized_pnl = float(row[4] or 0.0)
                total_fees = float(row[5] or 0.0) if len(row) > 5 else 0.0
            else:
                cash = float(row[2] if row[2] is not None else row[1] or 0.0)
                equity = float(row[1] if row[1] is not None else row[2] or 0.0)
                realized_pnl = float(row[3] or 0.0)
                unrealized_pnl = float(row[4] or 0.0)
                total_fees = 0.0

            candidate = AnchorPoint(
                timestamp=timestamp,
                cash=cash,
                equity=equity,
                realized_pnl=realized_pnl,
                unrealized_pnl=unrealized_pnl,
                total_fees=total_fees,
            )
            if newest is None or candidate.timestamp > newest.timestamp:
                newest = candidate
    finally:
        cur.close()

    return newest


def compute_since_days(last_exit_at: datetime | None, bootstrap_days: int, overlap_days: int, now: datetime | None = None) -> int:
    if last_exit_at is None:
        return bootstrap_days

    now = now or datetime.now(timezone.utc)
    last_exit_at = as_utc_datetime(last_exit_at)
    age_days = max(0, math.ceil((now - last_exit_at).total_seconds() / 86400.0))
    return max(overlap_days, age_days + overlap_days)


def resolve_fetch_checkpoint(last_exit_at: datetime | None, state_checkpoint: datetime | None) -> datetime | None:
    if state_checkpoint and last_exit_at:
        return max(last_exit_at, state_checkpoint)
    return state_checkpoint or last_exit_at


def filter_new_closed_trades(
    closed_trades: list[dict[str, Any]],
    existing_trade_ids: set[str],
    watermark: datetime | None,
    overlap_days: int,
) -> list[dict[str, Any]]:
    if watermark is None:
        return [trade for trade in closed_trades if trade["trade_id"] not in existing_trade_ids]

    watermark = as_utc_datetime(watermark)
    threshold = watermark - timedelta(days=overlap_days)
    return [
        trade
        for trade in closed_trades
        if trade["trade_id"] not in existing_trade_ids and trade["exit_timestamp"] >= threshold
    ]


def to_iso_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return as_utc_datetime(value).isoformat()


def from_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return as_utc_datetime(value)
    text = str(value).strip()
    if not text:
        return None
    return as_utc_datetime(datetime.fromisoformat(text.replace("Z", "+00:00")))


def serialize_fifo_queues(queues: dict[str, deque[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    payload: dict[str, list[dict[str, Any]]] = {}
    for symbol, queue in queues.items():
        payload[symbol] = []
        for lot in queue:
            payload[symbol].append(
                {
                    "side": lot["side"],
                    "price": float(lot["price"]),
                    "amount": float(lot["amount"]),
                    "fee": float(lot["fee"]),
                    "timestamp": int(lot["timestamp"]),
                    "trade_id": str(lot["trade_id"]),
                    "remaining_amount": float(lot["remaining_amount"]),
                }
            )
    return payload


def deserialize_fifo_queues(payload: dict[str, Any] | None) -> dict[str, deque[dict[str, Any]]]:
    queues: dict[str, deque[dict[str, Any]]] = {}
    if not payload:
        return queues
    for symbol, lots in payload.items():
        queue: deque[dict[str, Any]] = deque()
        for lot in lots or []:
            queue.append(
                {
                    "side": str(lot["side"]).lower(),
                    "price": float(lot["price"]),
                    "amount": float(lot["amount"]),
                    "fee": float(lot.get("fee", 0.0) or 0.0),
                    "timestamp": int(lot["timestamp"]),
                    "dt": datetime.fromtimestamp(int(lot["timestamp"]) / 1000.0, tz=timezone.utc),
                    "trade_id": str(lot["trade_id"]),
                    "remaining_amount": float(lot.get("remaining_amount", lot["amount"]) or 0.0),
                }
            )
        queues[str(symbol)] = queue
    return queues


def normalize_raw_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for trade in trades:
        if trade.get("timestamp") is None:
            continue
        normalized.append(
            {
                "id": str(trade.get("id")),
                "symbol": str(trade.get("symbol")),
                "side": str(trade.get("side")).lower(),
                "price": float(trade.get("price") or 0.0),
                "amount": float(trade.get("amount") or 0.0),
                "fee": float(trade.get("fee", {}).get("cost") or 0.0) if isinstance(trade.get("fee"), dict) else 0.0,
                "timestamp": int(trade["timestamp"]),
                "dt": datetime.fromtimestamp(int(trade["timestamp"]) / 1000.0, tz=timezone.utc),
            }
        )
    normalized.sort(key=lambda item: (item["timestamp"], item["id"]))
    return normalized


def match_trades_with_state(
    trades: list[dict[str, Any]],
    initial_queues: dict[str, deque[dict[str, Any]]] | None = None,
    recent_trade_ids: dict[str, set[str]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, deque[dict[str, Any]]], list[dict[str, Any]]]:
    queues: dict[str, deque[dict[str, Any]]] = {}
    for symbol, queue in (initial_queues or {}).items():
        queues[symbol] = deque(queue)

    seen_trade_ids = recent_trade_ids or {}
    closed_trades: list[dict[str, Any]] = []
    processed_trades: list[dict[str, Any]] = []

    for trade in normalize_raw_trades(trades):
        symbol = trade["symbol"]
        trade_id = trade["id"]
        if trade_id in seen_trade_ids.get(symbol, set()):
            continue

        processed_trades.append(trade)
        queue = queues.setdefault(symbol, deque())

        side = trade["side"]
        price = float(trade["price"])
        amount = float(trade["amount"])
        fee = float(trade["fee"])
        dt = trade["dt"]
        timestamp = trade["timestamp"]

        if not queue:
            queue.append(
                {
                    "side": side,
                    "price": price,
                    "amount": amount,
                    "fee": fee,
                    "timestamp": timestamp,
                    "dt": dt,
                    "trade_id": trade_id,
                    "remaining_amount": amount,
                }
            )
            continue

        queue_side = queue[0]["side"]
        if queue_side == side:
            queue.append(
                {
                    "side": side,
                    "price": price,
                    "amount": amount,
                    "fee": fee,
                    "timestamp": timestamp,
                    "dt": dt,
                    "trade_id": trade_id,
                    "remaining_amount": amount,
                }
            )
            continue

        rem_amount = amount
        while rem_amount > 0.00000001 and queue:
            oldest = queue[0]
            match_qty = min(rem_amount, float(oldest["remaining_amount"]))
            entry_prop_fee = float(oldest["fee"]) * (match_qty / float(oldest["amount"]))
            exit_prop_fee = fee * (match_qty / amount)

            oldest["remaining_amount"] = float(oldest["remaining_amount"]) - match_qty
            rem_amount -= match_qty

            if oldest["side"] == "buy":
                pnl = (price - float(oldest["price"])) * match_qty
                trade_side = "LONG"
            else:
                pnl = (float(oldest["price"]) - price) * match_qty
                trade_side = "SHORT"

            closed_trades.append(
                {
                    "symbol": symbol,
                    "side": trade_side,
                    "quantity": match_qty,
                    "entry_price": float(oldest["price"]),
                    "exit_price": price,
                    "pnl": pnl - (entry_prop_fee + exit_prop_fee),
                    "fees": entry_prop_fee + exit_prop_fee,
                    "entry_timestamp": oldest["dt"],
                    "exit_timestamp": dt,
                    "trade_id": f"{oldest['trade_id']}_{trade_id}",
                }
            )

            if float(oldest["remaining_amount"]) <= 0.00000001:
                queue.popleft()

        if rem_amount > 0.00000001:
            queue.append(
                {
                    "side": side,
                    "price": price,
                    "amount": rem_amount,
                    "fee": fee * (rem_amount / amount),
                    "timestamp": timestamp,
                    "dt": dt,
                    "trade_id": trade_id,
                    "remaining_amount": rem_amount,
                }
            )

    closed_trades.sort(key=lambda item: item["exit_timestamp"])
    return closed_trades, queues, processed_trades


def build_trade_seed_records(bot_id: str, trade: dict[str, Any], now: datetime) -> dict[str, Any]:
    trade_id = trade["trade_id"]
    side = str(trade["side"]).upper()
    entry_action = "OPEN_LONG" if side == "LONG" else "OPEN_SHORT"
    exit_action = "CLOSE_LONG" if side == "LONG" else "CLOSE_SHORT"

    entry_signal_id = stable_uuid("signal", bot_id, trade_id, "entry")
    exit_signal_id = stable_uuid("signal", bot_id, trade_id, "exit")

    entry_events = [
        ("SIGNAL_ACCEPTED", 0, {"signal_id": entry_signal_id, "status": "accepted"}),
        ("ORDER_PLACED", 1, {"order_id": f"{entry_signal_id}_ord", "price": trade["entry_price"]}),
        ("ORDER_FILLED", 2, {"order_id": f"{entry_signal_id}_ord", "fill_price": trade["entry_price"], "price": trade["entry_price"]}),
        ("POSITION_OPENED", 3, {"position_size": trade["quantity"], "size": trade["quantity"]}),
    ]
    exit_events = [
        ("SIGNAL_ACCEPTED", 0, {"signal_id": exit_signal_id, "status": "accepted"}),
        ("ORDER_PLACED", 1, {"order_id": f"{exit_signal_id}_ord", "price": trade["exit_price"]}),
        ("ORDER_FILLED", 2, {"order_id": f"{exit_signal_id}_ord", "fill_price": trade["exit_price"], "price": trade["exit_price"]}),
        ("POSITION_CLOSED", 3, {"pnl": trade["pnl"], "exit_price": trade["exit_price"]}),
    ]

    def _event_rows(signal_id: str, trade_ts: datetime, events: list[tuple[str, int, dict[str, Any]]]) -> list[dict[str, Any]]:
        rows = []
        for event_type, sequence, payload in events:
            rows.append(
                {
                    "event_id": stable_uuid("execution_event", signal_id, event_type, sequence),
                    "signal_id": signal_id,
                    "sequence": sequence,
                    "event_type": event_type,
                    "sent_at": trade_ts,
                    "exchange_time": trade_ts,
                    "payload": payload,
                    "created_at": now,
                }
            )
        return rows

    return {
        "entry_signal": {
            "id": entry_signal_id,
            "signal_id": entry_signal_id,
            "bot_id": bot_id,
            "symbol": trade["symbol"],
            "action": entry_action,
            "market_type": "FUTURE",
            "order_type": "MARKET",
            "entry": trade["entry_price"],
            "amount": trade["quantity"],
            "reduce_only": False,
            "status": "ACKNOWLEDGED",
            "generated_timestamp": trade["entry_timestamp"],
            "timeframe": "60",
            "created_at": now,
            "updated_at": now,
        },
        "exit_signal": {
            "id": exit_signal_id,
            "signal_id": exit_signal_id,
            "bot_id": bot_id,
            "symbol": trade["symbol"],
            "action": exit_action,
            "market_type": "FUTURE",
            "order_type": "MARKET",
            "entry": trade["exit_price"],
            "amount": trade["quantity"],
            "reduce_only": True,
            "status": "ACKNOWLEDGED",
            "generated_timestamp": trade["exit_timestamp"],
            "timeframe": "60",
            "created_at": now,
            "updated_at": now,
        },
        "entry_state": {
            "signal_id": entry_signal_id,
            "signal_state": "OPEN",
            "order_state": "FILLED",
            "position_state": "OPENED",
            "last_sequence": 3,
            "last_event_time": trade["entry_timestamp"],
            "closed_at": None,
            "created_at": now,
            "updated_at": now,
            "version": 0,
        },
        "exit_state": {
            "signal_id": exit_signal_id,
            "signal_state": "CLOSED",
            "order_state": "FILLED",
            "position_state": "CLOSED",
            "last_sequence": 3,
            "last_event_time": trade["exit_timestamp"],
            "closed_at": trade["exit_timestamp"],
            "created_at": now,
            "updated_at": now,
            "version": 0,
        },
        "events": _event_rows(entry_signal_id, trade["entry_timestamp"], entry_events)
        + _event_rows(exit_signal_id, trade["exit_timestamp"], exit_events),
        "closed_trade": {
            "id": stable_uuid("bot_dry_run_closed_trades", bot_id, trade_id),
            "bot_id": bot_id,
            "trade_id": trade_id,
            "data_source": "OUT_OF_SAMPLE",
            "symbol": trade["symbol"],
            "market_type": "FUTURE",
            "side": trade["side"],
            "quantity": trade["quantity"],
            "entry_price": trade["entry_price"],
            "exit_price": trade["exit_price"],
            "pnl": trade["pnl"],
            "fees": trade["fees"],
            "entry_timestamp": trade["entry_timestamp"],
            "exit_timestamp": trade["exit_timestamp"],
            "entry_signal_id": entry_signal_id,
            "exit_signal_id": exit_signal_id,
            "created_at": now,
            "updated_at": now,
        },
    }


def load_portfolio_anchor_before(conn: Any, bot_id: str, user_id: str, before_ts: datetime | None) -> AnchorPoint | None:
    before_ts = as_utc_datetime(before_ts) if before_ts is not None else None
    queries: list[tuple[str, tuple[Any, ...]]] = []
    if before_ts is not None:
        queries.extend(
            [
                (
                    "SELECT timestamp, cash, equity, realized_pnl, unrealized_pnl, total_fees "
                    "FROM bot_dry_run_portfolios WHERE bot_id = %s AND timestamp < %s ORDER BY timestamp DESC LIMIT 1;",
                    (bot_id, before_ts),
                ),
                (
                    "SELECT snapshot_at, total, free, realized_pnl, unrealized_pnl "
                    "FROM portfolio_aggregate_history WHERE user_id = %s AND snapshot_at < %s ORDER BY snapshot_at DESC LIMIT 1;",
                    (user_id, before_ts),
                ),
                (
                    "SELECT last_sync_at, total_capital, available_balance, realized_pnl, unrealized_pnl "
                    "FROM user_portfolios WHERE user_id = %s AND last_sync_at < %s LIMIT 1;",
                    (user_id, before_ts),
                ),
            ]
        )
    else:
        queries.extend(
            [
                (
                    "SELECT timestamp, cash, equity, realized_pnl, unrealized_pnl, total_fees "
                    "FROM bot_dry_run_portfolios WHERE bot_id = %s ORDER BY timestamp DESC LIMIT 1;",
                    (bot_id,),
                ),
                (
                    "SELECT snapshot_at, total, free, realized_pnl, unrealized_pnl "
                    "FROM portfolio_aggregate_history WHERE user_id = %s ORDER BY snapshot_at DESC LIMIT 1;",
                    (user_id,),
                ),
                (
                    "SELECT last_sync_at, total_capital, available_balance, realized_pnl, unrealized_pnl "
                    "FROM user_portfolios WHERE user_id = %s LIMIT 1;",
                    (user_id,),
                ),
            ]
        )

    newest: AnchorPoint | None = None
    cur = conn.cursor()
    try:
        for sql, params in queries:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row or row[0] is None:
                continue

            timestamp = as_utc_datetime(row[0])
            if "bot_dry_run_portfolios" in sql:
                cash = float(row[1] or 0.0)
                equity = float(row[2] or 0.0)
                realized_pnl = float(row[3] or 0.0)
                unrealized_pnl = float(row[4] or 0.0)
                total_fees = float(row[5] or 0.0) if len(row) > 5 else 0.0
            else:
                cash = float(row[2] if row[2] is not None else row[1] or 0.0)
                equity = float(row[1] if row[1] is not None else row[2] or 0.0)
                realized_pnl = float(row[3] or 0.0)
                unrealized_pnl = float(row[4] or 0.0)
                total_fees = 0.0

            candidate = AnchorPoint(
                timestamp=timestamp,
                cash=cash,
                equity=equity,
                realized_pnl=realized_pnl,
                unrealized_pnl=unrealized_pnl,
                total_fees=total_fees,
            )
            if newest is None or candidate.timestamp > newest.timestamp:
                newest = candidate
    finally:
        cur.close()

    return newest


def load_existing_bot_points_before(conn: Any, bot_id: str, before_ts: datetime | None) -> list[dict[str, Any]]:
    cur = conn.cursor()
    try:
        if before_ts is None:
            cur.execute(
                """
                SELECT timestamp, cash, equity, realized_pnl, unrealized_pnl, total_fees
                FROM bot_dry_run_portfolios
                WHERE bot_id = %s
                ORDER BY timestamp ASC;
                """,
                (bot_id,),
            )
        else:
            cur.execute(
                """
                SELECT timestamp, cash, equity, realized_pnl, unrealized_pnl, total_fees
                FROM bot_dry_run_portfolios
                WHERE bot_id = %s AND timestamp < %s
                ORDER BY timestamp ASC;
                """,
                (bot_id, before_ts),
            )
        rows = cur.fetchall()
        return [
            {
                "timestamp": as_utc_datetime(row[0]),
                "cash": float(row[1] or 0.0),
                "equity": float(row[2] or 0.0),
                "realized_pnl": float(row[3] or 0.0),
                "unrealized_pnl": float(row[4] or 0.0),
                "total_fees": float(row[5] or 0.0),
            }
            for row in rows
        ]
    finally:
        cur.close()


def build_dashboard_history_points(
    anchor: AnchorPoint,
    dashboard_points: list[dict[str, Any]],
    window_closed_trades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not dashboard_points:
        return []

    transformed: list[dict[str, Any]] = []
    for point in seed._normalize_portfolio_points(dashboard_points):
        point_copy = dict(point)
        point_copy["realized_pnl"] = float(point_copy.get("realized_pnl", 0.0)) + float(anchor.realized_pnl)
        point_copy["total_fees"] = float(point_copy.get("total_fees", 0.0)) + float(anchor.total_fees)
        transformed.append(point_copy)

    enriched = seed._attach_cumulative_fees(transformed, window_closed_trades)
    for point in enriched:
        point["total_fees"] = float(point.get("total_fees", 0.0)) + float(anchor.total_fees)
    return seed._normalize_portfolio_points(enriched)


def delete_repair_window_rows(
    conn: Any,
    bot_id: str,
    user_id: str,
    window_start: datetime,
    window_end: datetime,
    window_closed_trades: list[dict[str, Any]],
) -> None:
    signal_ids = sorted(
        {
            str(trade["entry_signal_id"])
            for trade in window_closed_trades
        }
        | {
            str(trade["exit_signal_id"])
            for trade in window_closed_trades
        }
    )
    trade_ids = sorted({str(trade["trade_id"]) for trade in window_closed_trades})
    cur = conn.cursor()
    try:
        if signal_ids:
            cur.execute("DELETE FROM execution_event WHERE signal_id = ANY(%s);", (signal_ids,))
            cur.execute("DELETE FROM execution_state WHERE signal_id = ANY(%s);", (signal_ids,))
            cur.execute("DELETE FROM signals WHERE signal_id = ANY(%s);", (signal_ids,))

        if trade_ids:
            cur.execute(
                "DELETE FROM bot_dry_run_closed_trades WHERE bot_id = %s AND trade_id = ANY(%s);",
                (bot_id, trade_ids),
            )

        cur.execute(
            """
            DELETE FROM bot_dry_run_portfolios
            WHERE bot_id = %s AND timestamp >= %s AND timestamp <= %s;
            """,
            (bot_id, window_start, window_end),
        )
        cur.execute(
            """
            DELETE FROM portfolio_balance_history
            WHERE user_id = %s AND snapshot_at >= %s AND snapshot_at <= %s;
            """,
            (user_id, window_start, window_end),
        )
        cur.execute(
            """
            DELETE FROM portfolio_aggregate_history
            WHERE user_id = %s AND snapshot_at >= %s AND snapshot_at <= %s;
            """,
            (user_id, window_start, window_end),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def compute_fetch_since_days(fetch_start: datetime, now: datetime, fallback_days: int) -> int:
    fetch_start = as_utc_datetime(fetch_start)
    now = as_utc_datetime(now)
    age_days = max(1, math.ceil((now - fetch_start).total_seconds() / 86400.0))
    return max(fallback_days, age_days)


def resolve_window_bounds(
    mode: str,
    dashboard_start: datetime | None,
    dashboard_end: datetime | None,
    state_checkpoint: datetime | None,
    last_trade_exit: datetime | None,
    bootstrap_days: int,
    overlap_days: int,
    now: datetime,
) -> tuple[datetime, datetime]:
    now = as_utc_datetime(now)
    end = as_utc_datetime(dashboard_end) if dashboard_end is not None else now
    if dashboard_start is not None:
        start = as_utc_datetime(dashboard_start)
        return start, end

    checkpoint = resolve_fetch_checkpoint(last_trade_exit, state_checkpoint)
    if checkpoint is None:
        checkpoint = now - timedelta(days=bootstrap_days)

    if mode == "repair-window":
        return checkpoint - timedelta(days=overlap_days), end

    return checkpoint - timedelta(days=overlap_days), end


def build_scope_state(
    scope_key: str,
    bot_id: str,
    user_id: str,
    exchange_id: str,
    mode: str,
    window_start: datetime,
    window_end: datetime,
    processed_trades: list[dict[str, Any]],
    queues: dict[str, deque[dict[str, Any]]],
    overlap_days: int,
) -> dict[str, Any]:
    if processed_trades:
        latest_trade_ts = max(trade["dt"] for trade in processed_trades)
        cutoff = latest_trade_ts - timedelta(days=overlap_days)
        recent_trade_ids: dict[str, list[str]] = defaultdict(list)
        for trade in processed_trades:
            if trade["dt"] >= cutoff:
                recent_trade_ids[trade["symbol"]].append(trade["id"])
    else:
        latest_trade_ts = window_end
        recent_trade_ids = defaultdict(list)

    return {
        "scope_key": scope_key,
        "bot_id": bot_id,
        "user_id": user_id,
        "exchange_id": exchange_id,
        "mode": mode,
        "last_fetch_at": window_end.isoformat(),
        "last_trade_seen_at": latest_trade_ts.isoformat() if isinstance(latest_trade_ts, datetime) else None,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "open_positions": serialize_fifo_queues(queues),
        "recent_trade_ids": dict(recent_trade_ids),
    }


def load_scope_trade_state(scope_state: dict[str, Any]) -> tuple[dict[str, deque[dict[str, Any]]], dict[str, set[str]]]:
    queues = deserialize_fifo_queues(scope_state.get("open_positions") if isinstance(scope_state.get("open_positions"), dict) else None)
    recent_trade_ids: dict[str, set[str]] = {}
    payload = scope_state.get("recent_trade_ids")
    if isinstance(payload, dict):
        for symbol, trade_ids in payload.items():
            recent_trade_ids[str(symbol)] = {str(trade_id) for trade_id in trade_ids or []}
    return queues, recent_trade_ids


def resolve_symbols(conn: Any, bot_id: str, symbols_arg: str | None) -> list[str]:
    symbols = parse_symbols(symbols_arg)
    if symbols:
        return symbols

    cur = conn.cursor()
    try:
        cur.execute("SELECT trading_pair FROM bots WHERE bot_id = %s LIMIT 1;", (bot_id,))
        row = cur.fetchone()
        if row and row[0]:
            trading_pair = str(row[0]).strip()
            if "/" in trading_pair:
                return [trading_pair]
    finally:
        cur.close()

    return []


def execute_window_replay(
    conn: Any,
    config: dict[str, Any],
    scope_state: dict[str, Any],
    scope_key: str,
    bot_id: str,
    user_id: str,
    user_email: str,
    symbols: list[str],
    mode: str,
    dashboard_start: datetime | None,
    dashboard_end: datetime | None,
    bootstrap_days: int,
    overlap_days: int,
    state_file: Path,
    yes: bool,
) -> tuple[int, int, int]:
    now = datetime.now(timezone.utc)
    state_checkpoint = from_iso_datetime(scope_state.get("last_fetch_at"))
    last_trade_exit = load_last_trade_exit(conn, bot_id)
    window_start, window_end = resolve_window_bounds(
        mode,
        dashboard_start,
        dashboard_end,
        state_checkpoint,
        last_trade_exit,
        bootstrap_days,
        overlap_days,
        now,
    )
    trade_fetch_start = window_start - timedelta(days=bootstrap_days if mode == "repair-window" else overlap_days)
    trade_since_days = compute_fetch_since_days(trade_fetch_start, window_end, bootstrap_days)

    print(f"Window start: {window_start.isoformat()}")
    print(f"Window end:   {window_end.isoformat()}")
    print(f"Trade fetch:  since_days={trade_since_days} (from {trade_fetch_start.isoformat()})")

    dashboard_points = seed.fetch_dashboard_portfolio_points(config, window_start, dashboard_end or window_end)
    print(f"Fetched {len(dashboard_points)} dashboard point(s)")

    anchor = load_portfolio_anchor_before(conn, bot_id, user_id, window_start)
    if anchor is None:
        first_point = dashboard_points[0]
        anchor = AnchorPoint(
            timestamp=first_point["timestamp"] - timedelta(microseconds=1),
            cash=float(first_point["cash"]),
            equity=float(first_point["equity"]),
            realized_pnl=0.0,
            unrealized_pnl=float(first_point.get("unrealized_pnl", 0.0)),
            total_fees=0.0,
        )

    initial_queues, recent_trade_ids = load_scope_trade_state(scope_state)
    if mode == "repair-window" and not scope_state:
        initial_queues = {}
        recent_trade_ids = {}

    raw_trades, _ = seed.fetch_trades_from_exchange(config, symbols, trade_since_days, mock=False)
    matched_closed_trades, updated_queues, processed_trades = match_trades_with_state(
        raw_trades,
        initial_queues=initial_queues,
        recent_trade_ids=recent_trade_ids,
    )
    window_closed_trades = [
        trade
        for trade in matched_closed_trades
        if trade["exit_timestamp"] >= window_start and trade["exit_timestamp"] <= window_end
    ]

    if mode == "repair-window":
        delete_repair_window_rows(conn, bot_id, user_id, window_start, window_end, window_closed_trades)

    if not yes:
        print(
            f"\nAbout to seed {len(window_closed_trades)} trade(s) and {len(dashboard_points)} dashboard point(s) for {user_email}."
        )
        confirm = input("Continue? (y/n): ").strip().lower()
        if confirm not in {"y", "yes"}:
            print("Aborted.")
            return 0, 0, 0

    window_points = build_dashboard_history_points(anchor, dashboard_points, window_closed_trades)
    if not window_points:
        raise RuntimeError("No dashboard history points were produced for the requested window.")

    current_point = window_points[-1]
    current_state = {
        "timestamp": current_point["timestamp"],
        "total": float(current_point["equity"]),
        "free": float(current_point["cash"]),
        "used": 0.0,
        "realized_pnl": float(current_point["realized_pnl"]),
        "unrealizedPnl": float(current_point["unrealized_pnl"]),
        "currency": str(scope_state.get("currency") or "USDT"),
        "exchange_id": config["EXCHANGE_ID"],
    }

    start_date = anchor.timestamp
    user_subscription_id = ensure_subscription(
        conn,
        user_id,
        bot_id,
        os.getenv("SYSTEM_WS_TOKEN", "ws_default_token"),
        current_point["timestamp"],
        start_date,
    )

    inserted_trades = append_trade_seed_data(conn, bot_id, window_closed_trades, current_point["timestamp"]) if window_closed_trades else 0
    inserted_points = append_portfolio_rows(
        conn,
        user_id,
        bot_id,
        user_subscription_id,
        window_points,
        current_state,
        current_point["timestamp"],
    )
    upsert_portfolio_account(conn, user_id, bot_id, user_subscription_id, current_state, current_point["timestamp"])
    upsert_user_portfolio(conn, user_id, current_state, current_point["timestamp"])

    existing_prefix_points = load_existing_bot_points_before(conn, bot_id, window_start)
    metric_points = seed._normalize_portfolio_points(existing_prefix_points + window_points)
    upsert_bot_metrics(conn, bot_id, metric_points, current_point["timestamp"])

    state_payload = build_scope_state(
        scope_key,
        bot_id,
        user_id,
        config["EXCHANGE_ID"],
        mode,
        window_start,
        window_end,
        processed_trades,
        updated_queues,
        overlap_days,
    )
    state_payload["currency"] = current_state["currency"]
    save_scoped_replay_state(state_file, scope_key, state_payload)

    print("Replay completed.")
    print(f"  Closed trades seeded: {inserted_trades}")
    print(f"  Portfolio points seeded: {inserted_points}")
    print(f"  Current equity: {current_state['total']:.8f} {current_state['currency']}")
    return inserted_trades, inserted_points, len(window_points)


def build_append_points(
    anchor: AnchorPoint,
    new_closed_trades: list[dict[str, Any]],
    live_balance: dict[str, Any],
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    last_ts = anchor.timestamp
    running_cash = float(anchor.cash)
    running_equity = float(anchor.equity)
    running_realized = float(anchor.realized_pnl)
    running_fees = float(anchor.total_fees)

    for trade in sorted(new_closed_trades, key=lambda item: item["exit_timestamp"]):
        ts = trade["exit_timestamp"]
        if ts <= last_ts:
            ts = last_ts + timedelta(microseconds=1)
        running_cash += float(trade["pnl"])
        running_equity = running_cash
        running_realized += float(trade["pnl"])
        running_fees += float(trade.get("fees", 0.0) or 0.0)
        points.append(
            {
                "timestamp": ts,
                "cash": running_cash,
                "equity": running_equity,
                "realized_pnl": running_realized,
                "unrealized_pnl": 0.0,
                "total_fees": running_fees,
            }
        )
        last_ts = ts

    live_ts = live_balance.get("timestamp") or datetime.now(timezone.utc)
    if live_ts <= last_ts:
        live_ts = last_ts + timedelta(microseconds=1)

    live_total_value = live_balance.get("total")
    live_free_value = live_balance.get("free")
    live_unrealized_value = live_balance.get("unrealizedPnl")
    live_total = running_equity if live_total_value is None else float(live_total_value)
    live_free = live_total if live_free_value is None else float(live_free_value)
    live_unrealized = 0.0 if live_unrealized_value is None else float(live_unrealized_value)
    points.append(
        {
            "timestamp": live_ts,
            "cash": live_free,
            "equity": live_total,
            "realized_pnl": running_realized,
            "unrealized_pnl": live_unrealized,
            "total_fees": running_fees,
        }
    )
    return seed._normalize_portfolio_points(points)


def _point_id(prefix: str, identity: str, timestamp: datetime) -> str:
    return stable_uuid(prefix, identity, timestamp.isoformat())


def ensure_subscription(conn: Any, user_id: str, bot_id: str, ws_token: str, now: datetime, start_date: datetime) -> str:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT user_subscription_id FROM subscriptions WHERE user_id = %s AND bot_id = %s AND status = 'ACTIVE';",
            (user_id, bot_id),
        )
        row = cur.fetchone()
        if row:
            user_subscription_id = row[0]
            cur.execute(
                """
                UPDATE subscriptions
                SET ws_token = %s, executor_connected = true, updated_at = %s
                WHERE user_subscription_id = %s;
                """,
                (ws_token, now, user_subscription_id),
            )
            conn.commit()
            return user_subscription_id

        user_subscription_id = "sub_" + str(uuid.uuid4()).replace("-", "")[:28]
        cur.execute(
            """
            INSERT INTO subscriptions (
                id, user_id, bot_id, user_subscription_id, ws_token, status,
                executor_connected, start_date, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                str(uuid.uuid4()),
                user_id,
                bot_id,
                user_subscription_id,
                ws_token,
                "ACTIVE",
                True,
                start_date,
                now,
                now,
            ),
        )
        conn.commit()
        return user_subscription_id
    finally:
        cur.close()


def upsert_portfolio_account(
    conn: Any,
    user_id: str,
    bot_id: str,
    user_subscription_id: str,
    live_balance: dict[str, Any],
    now: datetime,
) -> None:
    cur = conn.cursor()
    try:
        exchange_id = str(live_balance.get("exchange_id") or os.getenv("EXCHANGE_ID", "binance")).upper()
        currency = str(live_balance.get("currency") or "USDT")
        total_value = live_balance.get("total")
        free_value = live_balance.get("free")
        realized_value = live_balance.get("realized_pnl")
        unrealized_value = live_balance.get("unrealizedPnl")
        total = 0.0 if total_value is None else float(total_value)
        free = total if free_value is None else float(free_value)
        realized = 0.0 if realized_value is None else float(realized_value)
        unrealized = 0.0 if unrealized_value is None else float(unrealized_value)
        last_sync_at = live_balance.get("timestamp") or now

        cur.execute("SELECT id FROM portfolio_accounts WHERE user_subscription_id = %s;", (user_subscription_id,))
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE portfolio_accounts
                SET user_id = %s, bot_id = %s, exchange_id = %s, currency = %s, execution_mode = %s,
                    total = %s, free = %s, used = 0, realized_pnl = %s, unrealized_pnl = %s,
                    last_sync_at = %s, updated_at = %s, is_active = true
                WHERE user_subscription_id = %s;
                """,
                (
                    user_id,
                    bot_id,
                    exchange_id,
                    currency,
                    "DRY_RUN",
                    total,
                    free,
                    realized,
                    unrealized,
                    last_sync_at,
                    now,
                    user_subscription_id,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO portfolio_accounts (
                    id, user_id, user_subscription_id, bot_id, ws_token, exchange_id, currency,
                    execution_mode, total, free, used, realized_pnl, unrealized_pnl, last_sync_at,
                    is_active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s, true, %s, %s);
                """,
                (
                    str(uuid.uuid4()),
                    user_id,
                    user_subscription_id,
                    bot_id,
                    os.getenv("SYSTEM_WS_TOKEN", "ws_default_token"),
                    exchange_id,
                    currency,
                    "DRY_RUN",
                    total,
                    free,
                    realized,
                    unrealized,
                    last_sync_at,
                    now,
                    now,
                ),
            )
        conn.commit()
    finally:
        cur.close()


def append_trade_seed_data(conn: Any, bot_id: str, closed_trades: list[dict[str, Any]], now: datetime) -> int:
    inserted = 0
    cur = conn.cursor()
    try:
        for trade in closed_trades:
            seed_rows = build_trade_seed_records(bot_id, trade, now)

            cur.execute(
                """
                INSERT INTO signals (
                    id, signal_id, bot_id, symbol, action, market_type, order_type,
                    entry, amount, reduce_only, status, generated_timestamp, timeframe,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (signal_id) DO UPDATE SET
                    bot_id = EXCLUDED.bot_id,
                    symbol = EXCLUDED.symbol,
                    action = EXCLUDED.action,
                    market_type = EXCLUDED.market_type,
                    order_type = EXCLUDED.order_type,
                    entry = EXCLUDED.entry,
                    amount = EXCLUDED.amount,
                    reduce_only = EXCLUDED.reduce_only,
                    status = EXCLUDED.status,
                    generated_timestamp = EXCLUDED.generated_timestamp,
                    timeframe = EXCLUDED.timeframe,
                    updated_at = EXCLUDED.updated_at;
                """,
                tuple(seed_rows["entry_signal"].values()),
            )
            cur.execute(
                """
                INSERT INTO signals (
                    id, signal_id, bot_id, symbol, action, market_type, order_type,
                    entry, amount, reduce_only, status, generated_timestamp, timeframe,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (signal_id) DO UPDATE SET
                    bot_id = EXCLUDED.bot_id,
                    symbol = EXCLUDED.symbol,
                    action = EXCLUDED.action,
                    market_type = EXCLUDED.market_type,
                    order_type = EXCLUDED.order_type,
                    entry = EXCLUDED.entry,
                    amount = EXCLUDED.amount,
                    reduce_only = EXCLUDED.reduce_only,
                    status = EXCLUDED.status,
                    generated_timestamp = EXCLUDED.generated_timestamp,
                    timeframe = EXCLUDED.timeframe,
                    updated_at = EXCLUDED.updated_at;
                """,
                tuple(seed_rows["exit_signal"].values()),
            )
            cur.execute(
                """
                INSERT INTO execution_state (
                    signal_id, signal_state, order_state, position_state, last_sequence,
                    last_event_time, closed_at, created_at, updated_at, version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (signal_id) DO UPDATE SET
                    signal_state = EXCLUDED.signal_state,
                    order_state = EXCLUDED.order_state,
                    position_state = EXCLUDED.position_state,
                    last_sequence = EXCLUDED.last_sequence,
                    last_event_time = EXCLUDED.last_event_time,
                    closed_at = EXCLUDED.closed_at,
                    updated_at = EXCLUDED.updated_at,
                    version = EXCLUDED.version;
                """,
                tuple(seed_rows["entry_state"].values()),
            )
            cur.execute(
                """
                INSERT INTO execution_state (
                    signal_id, signal_state, order_state, position_state, last_sequence,
                    last_event_time, closed_at, created_at, updated_at, version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (signal_id) DO UPDATE SET
                    signal_state = EXCLUDED.signal_state,
                    order_state = EXCLUDED.order_state,
                    position_state = EXCLUDED.position_state,
                    last_sequence = EXCLUDED.last_sequence,
                    last_event_time = EXCLUDED.last_event_time,
                    closed_at = EXCLUDED.closed_at,
                    updated_at = EXCLUDED.updated_at,
                    version = EXCLUDED.version;
                """,
                tuple(seed_rows["exit_state"].values()),
            )

            for event in seed_rows["events"]:
                cur.execute(
                    """
                    INSERT INTO execution_event (
                        event_id, signal_id, sequence, event_type, sent_at, exchange_time, payload, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING;
                    """,
                    (
                        event["event_id"],
                        event["signal_id"],
                        event["sequence"],
                        event["event_type"],
                        event["sent_at"],
                        event["exchange_time"],
                        json.dumps(event["payload"], separators=(",", ":")),
                        event["created_at"],
                    ),
                )

            cur.execute(
                """
                INSERT INTO bot_dry_run_closed_trades (
                    id, bot_id, trade_id, data_source, symbol, market_type, side, quantity,
                    entry_price, exit_price, pnl, fees, entry_timestamp, exit_timestamp,
                    entry_signal_id, exit_signal_id, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bot_id, trade_id) DO UPDATE SET
                    data_source = EXCLUDED.data_source,
                    symbol = EXCLUDED.symbol,
                    market_type = EXCLUDED.market_type,
                    side = EXCLUDED.side,
                    quantity = EXCLUDED.quantity,
                    entry_price = EXCLUDED.entry_price,
                    exit_price = EXCLUDED.exit_price,
                    pnl = EXCLUDED.pnl,
                    fees = EXCLUDED.fees,
                    entry_timestamp = EXCLUDED.entry_timestamp,
                    exit_timestamp = EXCLUDED.exit_timestamp,
                    entry_signal_id = EXCLUDED.entry_signal_id,
                    exit_signal_id = EXCLUDED.exit_signal_id,
                    updated_at = EXCLUDED.updated_at;
                """,
                tuple(seed_rows["closed_trade"].values()),
            )
            inserted += 1

        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def append_portfolio_rows(
    conn: Any,
    user_id: str,
    bot_id: str,
    user_subscription_id: str,
    points: list[dict[str, Any]],
    live_balance: dict[str, Any],
    now: datetime,
) -> int:
    if not points:
        return 0

    exchange_id = str(live_balance.get("exchange_id") or os.getenv("EXCHANGE_ID", "binance")).upper()
    currency = str(live_balance.get("currency") or "USDT")

    bot_rows = []
    balance_rows = []
    aggregate_rows = []
    for point in points:
        ts = point["timestamp"]
        bot_rows.append(
            (
                _point_id("bot_dry_run_portfolios", bot_id, ts),
                bot_id,
                "OUT_OF_SAMPLE",
                ts,
                point["cash"],
                point["equity"],
                point["realized_pnl"],
                point["unrealized_pnl"],
                point["total_fees"],
                now,
                now,
            )
        )
        balance_rows.append(
            (
                _point_id("portfolio_balance_history", user_id, ts),
                now,
                now,
                exchange_id,
                point["equity"],
                ts,
                point["cash"],
                point["unrealized_pnl"],
                user_id,
                user_subscription_id,
                bot_id,
                currency,
                "DRY_RUN",
            )
        )
        aggregate_rows.append(
            (
                _point_id("portfolio_aggregate_history", user_id, ts),
                user_id,
                point["equity"],
                point["cash"],
                0,
                point["realized_pnl"],
                point["unrealized_pnl"],
                1,
                0,
                "FRESH",
                exchange_id,
                ts,
                now,
                now,
            )
        )

    cur = conn.cursor()
    try:
        execute_values(
            cur,
            """
            INSERT INTO bot_dry_run_portfolios (
                id, bot_id, data_source, timestamp, cash, equity, realized_pnl, unrealized_pnl,
                total_fees, created_at, updated_at
            ) VALUES %s
            ON CONFLICT (bot_id, timestamp) DO UPDATE SET
                data_source = EXCLUDED.data_source,
                cash = EXCLUDED.cash,
                equity = EXCLUDED.equity,
                realized_pnl = EXCLUDED.realized_pnl,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                total_fees = EXCLUDED.total_fees,
                updated_at = EXCLUDED.updated_at;
            """,
            bot_rows,
            page_size=1000,
        )

        execute_values(
            cur,
            """
            INSERT INTO portfolio_balance_history (
                id, created_at, updated_at, exchange_id, total, snapshot_at, free, unrealized_pnl,
                user_id, user_subscription_id, bot_id, currency, execution_mode
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                exchange_id = EXCLUDED.exchange_id,
                total = EXCLUDED.total,
                snapshot_at = EXCLUDED.snapshot_at,
                free = EXCLUDED.free,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                user_id = EXCLUDED.user_id,
                user_subscription_id = EXCLUDED.user_subscription_id,
                bot_id = EXCLUDED.bot_id,
                currency = EXCLUDED.currency,
                execution_mode = EXCLUDED.execution_mode;
            """,
            balance_rows,
            page_size=1000,
        )

        execute_values(
            cur,
            """
            INSERT INTO portfolio_aggregate_history (
                id, user_id, total, free, used, realized_pnl, unrealized_pnl,
                fresh_accounts_count, stale_accounts_count, data_freshness, exchange_id,
                snapshot_at, created_at, updated_at
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                total = EXCLUDED.total,
                free = EXCLUDED.free,
                used = EXCLUDED.used,
                realized_pnl = EXCLUDED.realized_pnl,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                fresh_accounts_count = EXCLUDED.fresh_accounts_count,
                stale_accounts_count = EXCLUDED.stale_accounts_count,
                data_freshness = EXCLUDED.data_freshness,
                exchange_id = EXCLUDED.exchange_id,
                snapshot_at = EXCLUDED.snapshot_at,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at;
            """,
            aggregate_rows,
            page_size=1000,
        )
        conn.commit()
        return len(points)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def upsert_user_portfolio(conn: Any, user_id: str, live_balance: dict[str, Any], now: datetime) -> None:
    cur = conn.cursor()
    try:
        exchange_id = str(live_balance.get("exchange_id") or os.getenv("EXCHANGE_ID", "binance")).upper()
        total_value = live_balance.get("total")
        free_value = live_balance.get("free")
        realized_value = live_balance.get("realized_pnl")
        unrealized_value = live_balance.get("unrealizedPnl")
        total = 0.0 if total_value is None else float(total_value)
        free = total if free_value is None else float(free_value)
        realized = 0.0 if realized_value is None else float(realized_value)
        unrealized = 0.0 if unrealized_value is None else float(unrealized_value)
        last_sync_at = live_balance.get("timestamp") or now

        cur.execute("SELECT id FROM user_portfolios WHERE user_id = %s;", (user_id,))
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE user_portfolios
                SET total_capital = %s, available_balance = %s, realized_pnl = %s, unrealized_pnl = %s,
                    exchange_id = %s, last_sync_at = %s, fresh_accounts_count = 1, stale_accounts_count = 0,
                    data_freshness = 'FRESH', updated_at = %s
                WHERE user_id = %s;
                """,
                (total, free, realized, unrealized, exchange_id, last_sync_at, now, user_id),
            )
        else:
            cur.execute(
                """
                INSERT INTO user_portfolios (
                    id, user_id, total_capital, available_balance, realized_pnl, unrealized_pnl,
                    max_drawdown_threshold, medium_risk_threshold, exchange_id, last_sync_at,
                    fresh_accounts_count, stale_accounts_count, data_freshness, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, 0.1000, 0.0500, %s, %s, 1, 0, 'FRESH', %s, %s);
                """,
                (
                    str(uuid.uuid4()),
                    user_id,
                    total,
                    free,
                    realized,
                    unrealized,
                    exchange_id,
                    last_sync_at,
                    now,
                    now,
                ),
            )
        conn.commit()
    finally:
        cur.close()


def upsert_bot_metrics(conn: Any, bot_id: str, points: list[dict[str, Any]], now: datetime) -> None:
    metric_points = [{"timestamp": point["timestamp"], "equity": point["equity"]} for point in points]
    cagr, max_dd, sharpe, sample_days = seed.calculate_metrics(metric_points)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO bot_leaderboard_metrics (
                bot_id, data_source, cagr, max_drawdown, sharpe, sample_days, last_calculated_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (bot_id, data_source) DO UPDATE SET
                cagr = EXCLUDED.cagr,
                max_drawdown = EXCLUDED.max_drawdown,
                sharpe = EXCLUDED.sharpe,
                sample_days = EXCLUDED.sample_days,
                last_calculated_at = EXCLUDED.last_calculated_at,
                updated_at = EXCLUDED.updated_at;
            """,
            (bot_id, "DRY_RUN", cagr, max_dd, sharpe, sample_days, now, now, now),
        )
        conn.commit()
    finally:
        cur.close()


def ensure_noninteractive_defaults(args: argparse.Namespace) -> None:
    if not args.symbols:
        args.symbols = ""
    if not args.overlap_days:
        args.overlap_days = DEFAULT_OVERLAP_DAYS
    if not args.bootstrap_days:
        args.bootstrap_days = DEFAULT_BOOTSTRAP_DAYS
    if not getattr(args, "state_file", None):
        args.state_file = str(DEFAULT_STATE_FILE)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hybrid cloud-balance and Binance-trade replay runner.")
    parser.add_argument("--mode", choices=("repair-window", "append"), default="repair-window", help="Repair a window first, then use append for deltas.")
    parser.add_argument("--bot-id", type=str, default=None, help="Target bot ID. If omitted, select from catalog by hints.")
    parser.add_argument("--bot-name", type=str, default=None, help="Optional bot name hint for catalog selection.")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbols to fetch from Binance.")
    parser.add_argument("--trader-email", type=str, default=DEFAULT_TRADER_EMAIL, help="Trader account email for portfolio seeding.")
    parser.add_argument("--dev-email", type=str, default=DEFAULT_DEV_EMAIL, help="Developer email hint used for bot selection.")
    parser.add_argument("--bootstrap-days", type=int, default=DEFAULT_BOOTSTRAP_DAYS, help="Initial lookback window when no checkpoint exists.")
    parser.add_argument("--overlap-days", type=int, default=DEFAULT_OVERLAP_DAYS, help="Overlap window used to safely re-read recent trades.")
    parser.add_argument("--dashboard-start", type=str, default=None, help="Optional dashboard window start (ISO timestamp).")
    parser.add_argument("--dashboard-end", type=str, default=None, help="Optional dashboard window end (ISO timestamp).")
    parser.add_argument("--state-file", type=str, default=str(DEFAULT_STATE_FILE), help="Replay checkpoint file path.")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    ensure_noninteractive_defaults(args)
    state_file = Path(args.state_file)
    config = load_config()
    config["DEV_EMAIL"] = args.dev_email
    dashboard_start = from_iso_datetime(args.dashboard_start)
    dashboard_end = from_iso_datetime(args.dashboard_end)

    print("=" * 72)
    print("  MARCUS TRADING - HYBRID CLOUD/BINANCE REPLAY")
    print("=" * 72)

    conn = seed.get_db_connection(config)
    try:
        bot_id, user_id, user_email = select_bot_and_user(conn, config, args.bot_id, args.bot_name, args.trader_email)
        print(f"Selected bot: {bot_id}")
        print(f"Selected trader: {user_email}")

        symbols = resolve_symbols(conn, bot_id, args.symbols)
        if not symbols:
            raise RuntimeError("Could not resolve trading symbols. Pass --symbols or ensure the bot has a trading_pair.")

        scope_key = state_scope_key(config["EXCHANGE_ID"], bot_id, user_id)
        scope_state = load_scoped_replay_state(state_file, scope_key)

        print(f"Mode: {args.mode}")
        print(f"State file: {state_file}")
        print(f"Symbols: {', '.join(symbols)}")

        if args.mode == "repair-window":
            execute_window_replay(
                conn=conn,
                config=config,
                scope_state=scope_state,
                scope_key=scope_key,
                bot_id=bot_id,
                user_id=user_id,
                user_email=user_email,
                symbols=symbols,
                mode=args.mode,
                dashboard_start=dashboard_start,
                dashboard_end=dashboard_end,
                bootstrap_days=args.bootstrap_days,
                overlap_days=args.overlap_days,
                state_file=state_file,
                yes=args.yes,
            )
        else:
            execute_window_replay(
                conn=conn,
                config=config,
                scope_state=scope_state,
                scope_key=scope_key,
                bot_id=bot_id,
                user_id=user_id,
                user_email=user_email,
                symbols=symbols,
                mode=args.mode,
                dashboard_start=dashboard_start,
                dashboard_end=dashboard_end,
                bootstrap_days=args.bootstrap_days,
                overlap_days=args.overlap_days,
                state_file=state_file,
                yes=args.yes,
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
