from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any

from .config import ExecutorConfig


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ExecutionResult:
    mode: str                         # "dry-run" | "live" | "error"
    order_id: str | None
    details: dict[str, Any]
    errors: list[str] | None = None
    entry_order_id: str | None = None
    tp_order_id: str | None = None
    sl_order_id: str | None = None
    symbol: str | None = None
    market_type: str | None = None
    action: str | None = None
    requested_amount: float | None = None
    filled_amount: float | None = None
    take_profit: float | None = None
    stop_loss: float | None = None
    execution_status: str | None = None
    protection_status: str | None = None
    raw_orders: dict[str, Any] | None = None
    warnings: list[str] | None = None


# ---------------------------------------------------------------------------
# Canonical action → CCXT mapping
#
# Only 4 semantic actions are supported. Aliases (BUY, SELL, SELL_SHORT, …)
# are intentionally omitted — they are ambiguous and not part of the backend
# SignalAction enum.
#
# Layout: action_name → (ccxt_side, default_reduce_only)
# ---------------------------------------------------------------------------

_ACTION_MAP: dict[str, tuple[str, bool]] = {
    "OPEN_LONG":   ("buy",  False),  # Open a long position
    "CLOSE_LONG":  ("sell", True),   # Close an existing long (reduce-only sell)
    "OPEN_SHORT":  ("sell", False),  # Open a short position
    "CLOSE_SHORT": ("buy",  True),   # Close an existing short (reduce-only buy)
}

# UPDATE_TP_SL is a special action that modifies an open order — not mapped to
# create_order. It is valid in VALID_ACTIONS so schema validation passes, but
# _build_order() will raise if called with it.
VALID_ACTIONS: frozenset[str] = frozenset(_ACTION_MAP) | {"UPDATE_TP_SL"}

VALID_MARKET_TYPES: frozenset[str] = frozenset({"SPOT", "FUTURE", "MARGIN"})
VALID_ORDER_TYPES: frozenset[str] = frozenset({"LIMIT", "MARKET"})
VALID_MARGIN_MODES: frozenset[str] = frozenset({"CROSS", "ISOLATED"})

_ACTION_ALIASES: dict[str, str] = {
    "BUY": "OPEN_LONG",
    "SELL": "CLOSE_LONG",
    "SELL_SHORT": "OPEN_SHORT",
    "BUY_TO_COVER": "CLOSE_SHORT",
}

_CLIENT_ORDER_ID_MAX_LEN = 32
_ENTRY_FILL_CONFIRM_RETRIES = 3
_ENTRY_FILL_CONFIRM_DELAY_SECONDS = 0.25
_PROTECTION_CREATE_RETRIES = 3
_PROTECTION_CREATE_DELAY_SECONDS = 0.25


# ---------------------------------------------------------------------------
# Signal schema validation
# ---------------------------------------------------------------------------

class SignalSchema:
    """Validates the signal payload delivered over WebSocket against the backend contract."""

    REQUIRED_FIELDS: frozenset[str] = frozenset({"signal_id", "action"})

    @staticmethod
    def validate(payload: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate signal payload.

        Returns:
            (is_valid, error_list) — error_list is empty when is_valid is True.
        """
        errors: list[str] = []

        if not isinstance(payload, dict):
            return False, ["Payload must be a dictionary"]

        # Required fields presence check
        for field in SignalSchema.REQUIRED_FIELDS:
            if field not in payload:
                errors.append(f"Missing required field: {field}")
            elif payload[field] in (None, ""):
                errors.append(f"{field} must be non-empty")

        # Resolve symbol from aliases
        symbol = None
        symbol_key_present = False
        for key in ("symbol", "asset_pair", "assetPair"):
            if key in payload:
                symbol_key_present = True
                if payload[key] is not None and payload[key] != "":
                    symbol = payload[key]
                    break

        if not symbol_key_present:
            errors.append("Missing required field: symbol")
        elif symbol is None:
            errors.append("symbol must be non-empty")

        if errors:
            return False, errors

        # --- action ---
        action = str(payload["action"]).strip().upper()
        action = _ACTION_ALIASES.get(action, action)
        if action not in VALID_ACTIONS:
            errors.append(
                f"Invalid action '{action}'. Must be one of: {sorted(VALID_ACTIONS)}"
            )

        # --- market_type ---
        market_type = str(payload.get("market_type") or "SPOT").strip().upper()
        if market_type not in VALID_MARKET_TYPES:
            errors.append(
                f"Invalid market_type '{market_type}'. Must be one of: {sorted(VALID_MARKET_TYPES)}"
            )

        # --- order_type ---
        # Default to MARKET to match minimal valid signals / config default, or default to LIMIT if specified.
        order_type = str(payload.get("order_type") or "MARKET").strip().upper()
        if order_type not in VALID_ORDER_TYPES:
            errors.append(
                f"Invalid order_type '{order_type}'. Must be one of: {sorted(VALID_ORDER_TYPES)}"
            )

        # --- entry: required for LIMIT orders ---
        if order_type == "LIMIT":
            entry = None
            for key in ("entry", "price", "limit_price", "limitPrice"):
                if key in payload and payload[key] is not None and payload[key] != "":
                    entry = payload[key]
                    break
            if entry is None or entry == "":
                errors.append("entry price is required for LIMIT orders")
            else:
                try:
                    float(entry)
                except (ValueError, TypeError):
                    errors.append(f"entry must be numeric, got '{entry}'")

        # --- amount (optional — executor may use env default) ---
        amount = None
        for key in ("amount", "quantity", "size"):
            if key in payload and payload[key] is not None and payload[key] != "":
                amount = payload[key]
                break
        if amount is not None:
            try:
                amt = float(amount)
                if amt <= 0:
                    errors.append(f"amount must be > 0, got {amt}")
            except (ValueError, TypeError):
                errors.append(f"amount must be numeric, got '{amount}'")

        # --- leverage: 1–125 (futures only, optional) ---
        leverage = payload.get("leverage")
        if leverage is not None:
            try:
                lev = int(leverage)
                if not (1 <= lev <= 125):
                    errors.append(f"leverage must be between 1 and 125, got {lev}")
            except (ValueError, TypeError):
                errors.append(f"leverage must be an integer, got '{leverage}'")

        # --- margin_mode (optional) ---
        margin_mode = payload.get("margin_mode")
        if margin_mode is not None:
            if str(margin_mode).strip().upper() not in VALID_MARGIN_MODES:
                errors.append(
                    f"Invalid margin_mode '{margin_mode}'. Must be one of: {sorted(VALID_MARGIN_MODES)}"
                )

        return len(errors) == 0, errors



# ---------------------------------------------------------------------------
# CCXT signal executor
# ---------------------------------------------------------------------------

class CcxtSignalExecutor:
    """
    Translates validated signal payloads into CCXT exchange orders.

    Futures lifecycle (when market_type == FUTURE):
      1. set_leverage(leverage, symbol)  — aborts on failure
      2. set_margin_mode(margin_mode, symbol) — aborts on failure
      3. create_order(...)
    """

    def __init__(self, config: ExecutorConfig, logger: logging.Logger | None = None) -> None:
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._exchanges: dict[str, Any] = {}
        self._exchange_injected = False

    @property
    def _exchange(self) -> Any:
        """Deprecated: Use _get_exchange(market_type) instead. Kept for backward compatibility."""
        default_type = (self._config.exchange_default_type or "FUTURE").upper()
        if default_type not in self._exchanges:
            self._exchanges[default_type] = self._build_exchange(default_type)
        return self._exchanges[default_type]

    @_exchange.setter
    def _exchange(self, value: Any) -> None:
        default_type = (self._config.exchange_default_type or "FUTURE").upper()
        self._exchanges[default_type] = value
        self._exchange_injected = True

    # ── Public API ─────────────────────────────────────────────────────────

    async def fetch_balance(self, market_type: str | None = None) -> dict[str, Any]:
        """Retrieve current account balance (thread-safe async wrapper)."""
        return await asyncio.to_thread(self._fetch_balance_sync, market_type)

    async def execute_signal(self, payload: dict[str, Any]) -> ExecutionResult:
        """Validate and execute a signal payload (thread-safe async wrapper)."""
        return await asyncio.to_thread(self._execute_signal_sync, payload)

    async def sync_exchange(self, signal_id: str, local_store: Any) -> list[Any]:
        """
        Query active positions and open orders from the exchange using CCXT API to recover state.
        
        This queries the exchange for:
        1. The specific order or open orders to resolve order status (placed, filled, canceled, failed).
        2. Open positions to resolve position status (opened, closed).
        
        Returns a list of synthetic ExecutionEvents required to reconcile local state with exchange state.
        """
        # Lấy trạng thái tín hiệu cục bộ sau khi đã Replay (Phase 3)
        state = await local_store.get_signal_state(signal_id)
        if not state:
            self._logger.warning("sync_exchange: No local state found for signal_id=%s", signal_id)
            return []
            
        # Nếu vị thế cục bộ đã đóng (CLOSED), không cần làm gì thêm
        if state.position_state == "CLOSED":
            self._logger.debug("sync_exchange: signal_id=%s is already CLOSED", signal_id)
            return []

        # Phân tích ký hiệu giao dịch (ví dụ: BTC/USDT) từ DB/lịch sử sự kiện/chính sách
        symbol = await self._resolve_symbol(signal_id, state, local_store)
        
        policies = state.policies or {}
        market_type = (
            state.market_type
            or policies.get("market_type")
            or policies.get("marketType")
            or self._config.exchange_default_type
            or "FUTURE"
        )
        market_type = str(market_type).upper()

        # Tìm kiếm ID lệnh của sàn (exchange order_id). Nếu sập trước khi lưu DB cục bộ,
        # ta quét qua các sự kiện cũ để tìm order_id
        order_id = state.order_id
        if not order_id:
            events = await local_store.get_events_for_signal(signal_id, limit=50)
            for e in events:
                oid = e.payload.get("order_id") or e.payload.get("id")
                if oid:
                    order_id = oid
                    break

        # Do các hàm gọi API của thư viện CCXT là đồng bộ (blocking I/O) và có thể gây nghẽn
        # toàn bộ Event Loop của Executor, ta đẩy tác vụ đồng bộ hóa này sang chạy trên Thread Pool.
        return await asyncio.to_thread(
            self._sync_exchange_sync,
            signal_id=signal_id,
            state=state,
            order_id=order_id,
            symbol=symbol,
            market_type=market_type
        )

    async def _resolve_symbol(self, signal_id: str, state: Any, local_store: Any) -> str:
        """Robustly resolve the order symbol from state, events, or policies. Raises ValueError on failure."""
        if state.order_symbol:
            return state.order_symbol
            
        # Try events
        events = await local_store.get_events_for_signal(signal_id, limit=50)
        for event in events:
            for key in ("symbol", "asset_pair", "assetPair", "order_symbol"):
                if key in event.payload and event.payload[key]:
                    return str(event.payload[key])
            if "signal" in event.payload and isinstance(event.payload["signal"], dict):
                sig = event.payload["signal"]
                for k in ("symbol", "asset_pair", "assetPair", "order_symbol"):
                    if k in sig and sig[k]:
                        return str(sig[k])
            if "order" in event.payload and isinstance(event.payload["order"], dict):
                ord_dict = event.payload["order"]
                if "symbol" in ord_dict and ord_dict["symbol"]:
                    return str(ord_dict["symbol"])
                    
        # Try policies
        if state.policies:
            for key in ("symbol", "asset_pair", "assetPair", "order_symbol"):
                if key in state.policies and state.policies[key]:
                    return str(state.policies[key])
                    
        raise ValueError(f"Could not resolve symbol for signal_id={signal_id}")

    # ── Execution ──────────────────────────────────────────────────────────

    def _execute_signal_sync(self, payload: dict[str, Any]) -> ExecutionResult:
        # 1. Schema validation
        is_valid, errors = SignalSchema.validate(payload)
        if not is_valid:
            self._logger.error("Signal validation failed errors=%s", errors)
            return ExecutionResult(mode="error", order_id=None, details={"signal": payload}, errors=errors)

        # 2. Deadline / expiry check (clean boundary)
        cancel_after = payload.get("cancel_after_timestamp")
        if cancel_after is not None:
            try:
                if time.time() > float(cancel_after):
                    self._logger.warning(
                        "Signal expired on arrival signal_id=%s expiry=%s",
                        payload.get("signal_id"), cancel_after,
                    )
                    return ExecutionResult(
                        mode="error",
                        order_id=None,
                        details={"signal": payload, "reason": "EXPIRED_ON_ARRIVAL"},
                        errors=["Signal arrived after cancel_after_timestamp deadline."],
                    )
            except (ValueError, TypeError):
                self._logger.warning("Invalid cancel_after_timestamp format: %s", cancel_after)

        # 3. Special actions — not order-creating
        action = str(payload.get("action", "")).strip().upper()
        if action == "UPDATE_TP_SL":
            try:
                update = self._build_tp_sl_update(payload)
            except ValueError as exc:
                self._logger.error("Failed to build TP/SL update: %s", exc)
                return ExecutionResult(mode="error", order_id=None, details={"signal": payload}, errors=[str(exc)])

            if self._config.execution_mode == "dry-run":
                self._logger.info("Dry-run TP/SL update: %s", update)
                return ExecutionResult(mode="dry-run", order_id=None, details=update)

            try:
                return self._update_tp_sl_sync(update)
            except Exception as exc:
                self._logger.error(
                    "TP/SL update failed signal_id=%s market_type=%s exchange_id=%s update=%s error=%s",
                    payload.get("signal_id"),
                    payload.get("market_type") or "SPOT",
                    self._config.exchange_id,
                    update,
                    exc,
                )
                return ExecutionResult(
                    mode="error",
                    order_id=None,
                    details={
                        "signal": self._summarize_payload(payload),
                        "update": update,
                        "exchange_id": self._config.exchange_id,
                        "execution_mode": self._config.execution_mode,
                    },
                    errors=[str(exc)],
                )

        # 4. Build CCXT order dict
        try:
            order = self._build_order(payload)
        except (ValueError, KeyError) as exc:
            self._logger.error("Failed to build order: %s", exc)
            return ExecutionResult(mode="error", order_id=None, details={"signal": payload}, errors=[str(exc)])

        # 4.5 Pre-flight sizing cap: enforce policies.maxSizePercent if provided
        policies = payload.get("policies") or {}
        maxp = None
        if isinstance(policies, dict):
            maxp = policies.get("maxSizePercent") if policies.get("maxSizePercent") is not None else policies.get("max_size_percent")
        try:
            if maxp is not None:
                maxp = float(maxp)
                # allow 0-100 percent as convenience (convert to 0-1)
                if maxp > 1 and maxp <= 100:
                    maxp = maxp / 100.0
                if not (0.0 <= maxp <= 1.0):
                    raise ValueError("maxSizePercent out of range")

                # We need price to compute notional. If price absent, we log and skip cap.
                price = order.get("price")
                if price is not None and price > 0:
                    bal = self._fetch_balance_sync(payload.get("market_type"))
                    available = float(bal.get("free", bal.get("total", 0.0)))
                    cap_notional = maxp * available
                    requested_amount = float(order.get("amount"))
                    requested_notional = requested_amount * float(price)
                    if requested_notional > cap_notional and cap_notional > 0:
                        new_amount = cap_notional / float(price)
                        # enforce minimal precision
                        self._logger.info("Capping order amount from %.8f to %.8f due to maxSizePercent", requested_amount, new_amount)
                        order["amount"] = new_amount
                else:
                    self._logger.debug("maxSizePercent provided but price missing; skipping cap")
        except Exception as e:
            self._logger.warning("Failed to apply maxSizePercent cap: %s", e)

        # 5. Dry-run short-circuit
        if self._config.execution_mode == "dry-run":
            self._logger.info("Dry-run order: %s", order)
            return ExecutionResult(mode="dry-run", order_id=None, details=order)

        # 6. Live execution — Futures lifecycle + create_order
        try:
            market_type = self._resolve_market_type(payload)
            exchange = self._get_exchange(market_type)

            if market_type == "FUTURE":
                result = self._execute_futures(exchange, order, payload)
                if result is not None:
                    return result  # lifecycle call failed → abort

            created = exchange.create_order(
                order["symbol"],
                order["type"],
                order["side"],
                order["amount"],
                order.get("price"),
                order.get("params") or {},
            )
            return self._finalize_live_entry_execution_sync(
                exchange=exchange,
                payload=payload,
                order=order,
                created=created,
                market_type=market_type,
            )

        except Exception as exc:
            self._logger.error(
                "Live execution failed signal_id=%s market_type=%s exchange_id=%s order=%s error=%s",
                payload.get("signal_id"),
                payload.get("market_type") or "SPOT",
                self._config.exchange_id,
                self._summarize_order(order),
                exc,
            )
            return ExecutionResult(
                mode="error",
                order_id=None,
                details={
                    "signal": self._summarize_payload(payload),
                    "order": self._summarize_order(order),
                    "exchange_id": self._config.exchange_id,
                    "execution_mode": self._config.execution_mode,
                },
                errors=[str(exc)],
            )

    def _execute_futures(
        self, exchange: Any, order: dict[str, Any], payload: dict[str, Any]
    ) -> ExecutionResult | None:
        """
        Run Futures lifecycle calls before create_order.

        Returns an error ExecutionResult if any call fails (caller should abort),
        or None to continue.
        """
        symbol = order["symbol"]
        leverage = int(payload.get("leverage") or 1)
        margin_mode = str(payload.get("margin_mode") or "CROSS").lower()

        # Step 1: set_leverage — abort on failure
        try:
            exchange.set_leverage(leverage, symbol)
            self._logger.debug("set_leverage leverage=%d symbol=%s", leverage, symbol)
        except Exception as exc:
            self._logger.error("set_leverage failed leverage=%d symbol=%s: %s", leverage, symbol, exc)
            return ExecutionResult(
                mode="error",
                order_id=None,
                details={"leverage": leverage, "symbol": symbol},
                errors=[f"set_leverage failed: {exc}"],
            )

        # Step 2: set_margin_mode — abort on failure
        try:
            exchange.set_margin_mode(margin_mode, symbol)
            self._logger.debug("set_margin_mode mode=%s symbol=%s", margin_mode, symbol)
        except Exception as exc:
            self._logger.error("set_margin_mode failed mode=%s symbol=%s: %s", margin_mode, symbol, exc)
            return ExecutionResult(
                mode="error",
                order_id=None,
                details={"margin_mode": margin_mode, "symbol": symbol},
                errors=[f"set_margin_mode failed: {exc}"],
            )

        return None  # success — proceed to create_order

    # ── Order building ─────────────────────────────────────────────────────

    def _resolve_market_type(self, payload: dict[str, Any]) -> str:
        market_type = (
            payload.get("market_type")
            or payload.get("marketType")
            or self._config.exchange_default_type
            or "SPOT"
        )
        return str(market_type).strip().upper()

    def _finalize_live_entry_execution_sync(
        self,
        exchange: Any,
        payload: dict[str, Any],
        order: dict[str, Any],
        created: Any,
        market_type: str,
    ) -> ExecutionResult:
        entry_order = created if isinstance(created, dict) else {"raw": created}
        entry_order_id = self._extract_order_id(entry_order)
        symbol = order["symbol"]
        action = str(payload.get("action", "")).strip().upper()
        requested_amount = float(order.get("amount") or 0.0)
        entry_order = self._confirm_entry_fill_sync(exchange, symbol, entry_order_id, entry_order)
        filled_amount = self._extract_filled_amount(entry_order)
        order_status = self._normalize_order_status(entry_order)
        tp = _first_present(payload, "take_profit", "takeProfit", "tp")
        sl = _first_present(payload, "stop_loss", "stopLoss", "sl")
        tp_value = float(tp) if tp is not None else None
        sl_value = float(sl) if sl is not None else None

        details: dict[str, Any] = {
            "entry_order": entry_order,
            "entry_order_id": entry_order_id,
            "market_type": market_type,
            "action": action,
            "requested_amount": requested_amount,
            "filled_amount": filled_amount,
            "take_profit": tp_value,
            "stop_loss": sl_value,
        }

        warnings: list[str] = []
        errors: list[str] = []

        if not self._should_create_entry_protection(action, market_type, tp_value, sl_value):
            if (tp_value is not None or sl_value is not None) and market_type != "FUTURE":
                warnings.append(f"TP/SL was requested but protection creation is only enabled for FUTURE market_type, got {market_type}.")
            execution_status = "ENTRY_FILLED" if filled_amount > 0 else "ENTRY_OPEN"
            protection_status = "NONE"
            if filled_amount <= 0 and order_status not in {"closed", "filled"}:
                execution_status = "ENTRY_OPEN"
                protection_status = "PENDING"
                warnings.append("Entry order is not confirmed filled yet; TP/SL were not created.")
            return ExecutionResult(
                mode="live",
                order_id=entry_order_id,
                details=details,
                entry_order_id=entry_order_id,
                symbol=symbol,
                market_type=market_type,
                action=action,
                requested_amount=requested_amount,
                filled_amount=filled_amount,
                take_profit=tp_value,
                stop_loss=sl_value,
                execution_status=execution_status,
                protection_status=protection_status,
                raw_orders={"entry": entry_order},
                warnings=warnings or None,
            )

        if filled_amount <= 0:
            warnings.append("Entry order is not confirmed filled yet; TP/SL were not created.")
            return ExecutionResult(
                mode="live",
                order_id=entry_order_id,
                details=details,
                entry_order_id=entry_order_id,
                symbol=symbol,
                market_type=market_type,
                action=action,
                requested_amount=requested_amount,
                filled_amount=filled_amount,
                take_profit=tp_value,
                stop_loss=sl_value,
                execution_status="ENTRY_OPEN",
                protection_status="PENDING",
                raw_orders={"entry": entry_order},
                warnings=warnings,
            )

        protection_orders: dict[str, Any] = {"entry": entry_order}
        sl_order: dict[str, Any] | None = None
        tp_order: dict[str, Any] | None = None

        if sl_value is not None:
            sl_order, sl_error = self._create_protective_order_sync(
                exchange=exchange,
                payload=payload,
                entry_side=str(order.get("side") or entry_order.get("side") or "buy"),
                filled_amount=filled_amount,
                role="stop_loss",
                trigger_price=sl_value,
                market_type=market_type,
            )
            if sl_error is not None:
                errors.append(f"Stop-loss protection failed: {sl_error}")
                details["protection_status"] = "ENTRY_FILLED_UNPROTECTED"
                protection_orders["stop_loss"] = None
                return ExecutionResult(
                    mode="live",
                    order_id=entry_order_id,
                    details=details,
                    errors=errors,
                    entry_order_id=entry_order_id,
                    symbol=symbol,
                    market_type=market_type,
                    action=action,
                    requested_amount=requested_amount,
                    filled_amount=filled_amount,
                    take_profit=tp_value,
                    stop_loss=sl_value,
                    execution_status="ENTRY_FILLED_UNPROTECTED",
                    protection_status="UNPROTECTED",
                    raw_orders=protection_orders,
                    warnings=warnings or None,
                )
            protection_orders["stop_loss"] = sl_order

        if tp_value is not None:
            tp_order, tp_error = self._create_protective_order_sync(
                exchange=exchange,
                payload=payload,
                entry_side=str(order.get("side") or entry_order.get("side") or "buy"),
                filled_amount=filled_amount,
                role="take_profit",
                trigger_price=tp_value,
                market_type=market_type,
            )
            if tp_error is not None:
                errors.append(f"Take-profit protection failed: {tp_error}")
                protection_orders["take_profit"] = None
                execution_status = "PARTIALLY_PROTECTED" if sl_order is not None else "ENTRY_FILLED_UNPROTECTED"
                protection_status = "PARTIALLY_PROTECTED" if sl_order is not None else "UNPROTECTED"
                return ExecutionResult(
                    mode="live",
                    order_id=entry_order_id,
                    details=details,
                    errors=errors,
                    entry_order_id=entry_order_id,
                    symbol=symbol,
                    market_type=market_type,
                    action=action,
                    requested_amount=requested_amount,
                    filled_amount=filled_amount,
                    take_profit=tp_value,
                    stop_loss=sl_value,
                    execution_status=execution_status,
                    protection_status=protection_status,
                    raw_orders=protection_orders,
                    warnings=warnings or None,
                )
            protection_orders["take_profit"] = tp_order

        details["protection_status"] = "PROTECTED"
        return ExecutionResult(
            mode="live",
            order_id=entry_order_id,
            details=details,
            entry_order_id=entry_order_id,
            tp_order_id=self._extract_order_id(tp_order) if tp_order else None,
            sl_order_id=self._extract_order_id(sl_order) if sl_order else None,
            symbol=symbol,
            market_type=market_type,
            action=action,
            requested_amount=requested_amount,
            filled_amount=filled_amount,
            take_profit=tp_value,
            stop_loss=sl_value,
            execution_status="PROTECTED",
            protection_status="PROTECTED",
            raw_orders=protection_orders,
            warnings=warnings or None,
        )

    def _confirm_entry_fill_sync(
        self,
        exchange: Any,
        symbol: str,
        entry_order_id: str | None,
        entry_order: dict[str, Any],
    ) -> dict[str, Any]:
        current = dict(entry_order)
        if self._extract_filled_amount(current) > 0:
            return current
        if self._normalize_order_status(current) in {"closed", "filled"}:
            return current
        if not entry_order_id:
            return current

        for attempt in range(_ENTRY_FILL_CONFIRM_RETRIES):
            try:
                time.sleep(_ENTRY_FILL_CONFIRM_DELAY_SECONDS)
                fetched = exchange.fetch_order(entry_order_id, symbol)
                if isinstance(fetched, dict):
                    current = fetched
                    if self._extract_filled_amount(current) > 0:
                        return current
                    if self._normalize_order_status(current) in {"closed", "filled"}:
                        return current
            except Exception as exc:
                self._logger.debug(
                    "Entry fill confirmation attempt failed attempt=%d order_id=%s symbol=%s error=%s",
                    attempt + 1,
                    entry_order_id,
                    symbol,
                    exc,
                )
        return current

    def _create_protective_order_sync(
        self,
        exchange: Any,
        payload: dict[str, Any],
        entry_side: str,
        filled_amount: float,
        role: str,
        trigger_price: float,
        market_type: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        if market_type != "FUTURE":
            return None, f"TP/SL order creation is not enabled for market_type={market_type}"

        signal_id = str(payload.get("signal_id") or "")
        if not signal_id:
            return None, "Signal is missing signal_id"

        order_side = "sell" if entry_side.lower() == "buy" else "buy"
        order_type = "STOP_MARKET" if role == "stop_loss" else "TAKE_PROFIT_MARKET"
        client_order_id = self._build_signal_client_order_id(signal_id, "sl" if role == "stop_loss" else "tp")
        params: dict[str, Any] = {
            "triggerPrice": float(trigger_price),
            "reduceOnly": True,
            "clientOrderId": client_order_id,
        }

        last_error: str | None = None
        symbol = self._normalize_symbol(
            payload.get("symbol") or payload.get("asset_pair") or payload.get("assetPair")
        )
        if not symbol:
            return None, "Signal is missing symbol"
        for attempt in range(_PROTECTION_CREATE_RETRIES):
            try:
                created = exchange.create_order(
                    symbol,
                    order_type,
                    order_side,
                    filled_amount,
                    None,
                    params,
                )
                if isinstance(created, dict):
                    return created, None
                return {"raw": created, "clientOrderId": client_order_id}, None
            except Exception as exc:
                last_error = str(exc)
                if attempt + 1 < _PROTECTION_CREATE_RETRIES:
                    time.sleep(_PROTECTION_CREATE_DELAY_SECONDS * (attempt + 1))

        return None, last_error or "Unknown protection order failure"

    def _extract_order_id(self, order: dict[str, Any]) -> str | None:
        for key in ("id", "orderId", "algoId", "clientOrderId"):
            value = order.get(key)
            if value not in (None, ""):
                return str(value)
        info = order.get("info")
        if isinstance(info, dict):
            for key in ("orderId", "algoId", "clientOrderId"):
                value = info.get(key)
                if value not in (None, ""):
                    return str(value)
        return None

    def _extract_filled_amount(self, order: dict[str, Any]) -> float:
        for key in ("filled", "executedQty", "cumQty", "filledAmount"):
            value = order.get(key)
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
        info = order.get("info")
        if isinstance(info, dict):
            for key in ("filled", "executedQty", "cumQty"):
                value = info.get(key)
                if value not in (None, ""):
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        pass
        return 0.0

    def _normalize_order_status(self, order: dict[str, Any]) -> str:
        status = order.get("status")
        if status in (None, ""):
            info = order.get("info")
            if isinstance(info, dict):
                status = info.get("status")
        return str(status).strip().lower() if status not in (None, "") else ""

    def _build_signal_client_order_id(self, signal_id: str, suffix: str) -> str:
        raw = f"{signal_id}-{suffix}"
        if len(raw) <= _CLIENT_ORDER_ID_MAX_LEN:
            return raw

        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
        prefix = signal_id[:16]
        compact = f"{prefix}-{suffix[:4]}-{digest}"
        return compact[:_CLIENT_ORDER_ID_MAX_LEN]

    def _entry_client_order_id_matches_signal(self, client_order_id: Any, signal_id: str | None) -> bool:
        if not signal_id or client_order_id in (None, ""):
            return False
        candidate = str(client_order_id)
        return candidate in {
            str(signal_id),
            self._build_signal_client_order_id(str(signal_id), "entry"),
        }

    def _should_create_entry_protection(
        self,
        action: str,
        market_type: str,
        take_profit: float | None,
        stop_loss: float | None,
    ) -> bool:
        return action in {"OPEN_LONG", "OPEN_SHORT"} and market_type == "FUTURE" and (
            take_profit is not None or stop_loss is not None
        )

    def _build_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action", "")).strip().upper()
        action = _ACTION_ALIASES.get(action, action)
        if action == "UPDATE_TP_SL":
            raise ValueError("UPDATE_TP_SL cannot be handled via _build_order")

        raw_symbol = payload.get("symbol") or payload.get("asset_pair") or payload.get("assetPair")
        symbol = self._normalize_symbol(raw_symbol)
        if not symbol:
            raise ValueError("Signal missing symbol")

        order_type = str(
            payload.get("order_type") or self._config.default_order_type
        ).strip().lower()

        # entry is the canonical price field (per Unified Spec)
        # fallback to price/limit_price/limitPrice
        entry = payload.get("entry") or payload.get("price") or payload.get("limit_price") or payload.get("limitPrice")
        if order_type == "limit":
            if entry is None:
                raise ValueError("LIMIT orders require entry price")
            price = float(entry)
        else:
            price = None

        # amount: signal value takes priority, env default is fallback
        # check aliases quantity / size
        amount = payload.get("amount") or payload.get("quantity") or payload.get("size")
        if amount in (None, ""):
            amount = self._config.default_order_amount
        amount = float(amount)
        if amount <= 0:
            raise ValueError(f"Order amount must be > 0, got {amount}")

        # signal-level reduce_only overrides the action default
        reduce_only_raw = payload.get("reduce_only")
        if isinstance(reduce_only_raw, str):
            reduce_only_raw = reduce_only_raw.lower() == "true"

        side, reduce_only = _map_action(action, reduce_only_raw)

        params: dict[str, Any] = {}

        # Idempotency link: stable clientOrderId per signal role.
        signal_id = payload.get("signal_id")
        if signal_id:
            params["clientOrderId"] = self._build_signal_client_order_id(str(signal_id), "entry")

        if reduce_only:
            params["reduceOnly"] = True

        # time_in_force alias support
        time_in_force = payload.get("time_in_force") or payload.get("timeInForce")
        if time_in_force:
            params["timeInForce"] = time_in_force

        return {
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "params": params,
        }

    # ── Balance ────────────────────────────────────────────────────────────

    def _build_tp_sl_update(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw_symbol = payload.get("symbol") or payload.get("asset_pair") or payload.get("assetPair")
        symbol = self._normalize_symbol(raw_symbol)
        if not symbol:
            raise ValueError("Signal missing symbol")

        tp = _first_present(payload, "take_profit", "takeProfit", "tp")
        sl = _first_present(payload, "stop_loss", "stopLoss", "sl")
        if tp is None and sl is None:
            raise ValueError("UPDATE_TP_SL requires at least one take_profit/tp or stop_loss/sl value")

        params: dict[str, Any] = {}
        if tp is not None:
            params["takeProfit"] = float(tp)
        if sl is not None:
            params["stopLoss"] = float(sl)

        signal_id = payload.get("signal_id")
        if signal_id:
            params["clientOrderId"] = self._build_signal_client_order_id(str(signal_id), "update")

        return {
            "symbol": symbol,
            "market_type": str(payload.get("market_type") or "SPOT").upper(),
            "order_id": payload.get("order_id") or payload.get("orderId"),
            "take_profit": float(tp) if tp is not None else None,
            "stop_loss": float(sl) if sl is not None else None,
            "params": params,
        }

    def _update_tp_sl_sync(self, update: dict[str, Any]) -> ExecutionResult:
        exchange = self._get_exchange(update["market_type"])
        open_orders = exchange.fetch_open_orders(update["symbol"])
        matching_orders = self._matching_tp_sl_orders(open_orders, update)
        if not matching_orders:
            raise RuntimeError(f"No active TP/SL orders found for symbol={update['symbol']}")

        updated_orders: list[dict[str, Any]] = []
        has_capabilities = getattr(exchange, "has", {})
        use_modify_order = bool(isinstance(has_capabilities, dict) and has_capabilities.get("modifyOrder"))
        use_modify_order = use_modify_order and hasattr(exchange, "modify_order")

        for existing in matching_orders:
            replacement = self._build_tp_sl_replacement_order(existing, update)
            if use_modify_order:
                modified = exchange.modify_order(
                    existing["id"],
                    existing["symbol"],
                    replacement["type"],
                    replacement["side"],
                    replacement["amount"],
                    replacement["price"],
                    replacement["params"],
                )
                updated_orders.append(modified if isinstance(modified, dict) else {"raw": modified})
            else:
                exchange.cancel_order(existing["id"], existing["symbol"])
                created = exchange.create_order(
                    replacement["symbol"],
                    replacement["type"],
                    replacement["side"],
                    replacement["amount"],
                    replacement["price"],
                    replacement["params"],
                )
                updated_orders.append(created if isinstance(created, dict) else {"raw": created})

        order_id = None
        if len(updated_orders) == 1 and updated_orders[0].get("id") is not None:
            order_id = str(updated_orders[0]["id"])

        return ExecutionResult(
            mode="live",
            order_id=order_id,
            details={
                "action": "UPDATE_TP_SL",
                "strategy": "modify_order" if use_modify_order else "cancel_replace",
                "matched_order_count": len(matching_orders),
                "orders": updated_orders,
            },
        )

    def _matching_tp_sl_orders(self, open_orders: Any, update: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(open_orders, list):
            raise RuntimeError("fetch_open_orders returned an unexpected response")

        requested_order_id = str(update["order_id"]) if update.get("order_id") is not None else None
        matches: list[dict[str, Any]] = []
        for order in open_orders:
            if not isinstance(order, dict):
                continue
            order_id = order.get("id")
            symbol = order.get("symbol")
            if requested_order_id is not None:
                if str(order_id) == requested_order_id:
                    matches.append(order)
                continue
            if symbol == update["symbol"] and self._is_tp_sl_order(order):
                matches.append(order)
        return matches

    def _build_tp_sl_replacement_order(self, existing: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
        order_type = str(existing.get("type") or "limit").lower()
        side = existing.get("side")
        amount = existing.get("amount")
        if side in (None, "") or amount in (None, ""):
            raise RuntimeError(f"Cannot update order {existing.get('id')}: missing side or amount")

        price = existing.get("price")
        params = dict(existing.get("params") or {})
        params.update(update["params"])

        order_role = self._classify_tp_sl_order(existing)
        if order_role == "take_profit" and update.get("take_profit") is not None:
            price = update["take_profit"]
            params["stopPrice"] = update["take_profit"]
        elif order_role == "stop_loss" and update.get("stop_loss") is not None:
            price = update["stop_loss"]
            params["stopPrice"] = update["stop_loss"]

        return {
            "symbol": existing.get("symbol") or update["symbol"],
            "type": order_type,
            "side": side,
            "amount": float(amount),
            "price": float(price) if price is not None else None,
            "params": params,
        }

    def _is_tp_sl_order(self, order: dict[str, Any]) -> bool:
        return self._classify_tp_sl_order(order) is not None

    def _classify_tp_sl_order(self, order: dict[str, Any]) -> str | None:
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        params = order.get("params") if isinstance(order.get("params"), dict) else {}
        values = [
            order.get("type"),
            order.get("trigger"),
            order.get("purpose"),
            order.get("clientOrderId"),
            info.get("type"),
            params.get("type"),
        ]
        text = " ".join(str(value).lower() for value in values if value is not None)
        if "take" in text and "profit" in text:
            return "take_profit"
        if "stop" in text or "loss" in text:
            return "stop_loss"
        return None

    def _fetch_balance_sync(self, market_type: str | None = None) -> dict[str, Any]:
        if self._config.execution_mode == "dry-run":
            val = self._config.dry_run_initial_balance
            return {"total": val, "free": val, "used": 0.0, "currency": "USDT", "unrealizedPnl": 0.0}

        try:
            exchange = self._get_exchange(market_type)
            balance = exchange.fetch_balance()
            unrealized_pnl = self._fetch_unrealized_pnl_sync(exchange)

            for curr in ("USDT", "USDC", "BUSD"):
                if curr in balance and isinstance(balance[curr], dict):
                    details = balance[curr]
                    return {
                        "total": float(details.get("total", 0.0)),
                        "free": float(details.get("free", 0.0)),
                        "used": float(details.get("used", 0.0)),
                        "currency": curr,
                        "unrealizedPnl": unrealized_pnl,
                    }

            if "total" in balance and isinstance(balance["total"], dict):
                for curr, amt in balance["total"].items():
                    if amt and float(amt) > 0:
                        return {
                            "total": float(amt),
                            "free": float(balance.get("free", {}).get(curr, 0.0)),
                            "used": float(balance.get("used", {}).get(curr, 0.0)),
                            "currency": curr,
                            "unrealizedPnl": unrealized_pnl,
                        }

            return {"total": 0.0, "free": 0.0, "used": 0.0, "currency": "NONE", "unrealizedPnl": unrealized_pnl}

        except Exception as exc:
            self._logger.error("Failed to fetch live balance: %s", exc)
            raise

    def _fetch_unrealized_pnl_sync(self, exchange: Any) -> float:
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
                        raw_pnl = info.get("unrealizedPnl") or info.get("unrealizedProfit")
                if raw_pnl is not None:
                    total += float(raw_pnl)
            return total
        except Exception as exc:
            self._logger.debug("Unable to fetch unrealized pnl: %s", exc)
            return 0.0

    # ── Exchange lifecycle ─────────────────────────────────────────────────

    def _get_exchange(self, market_type: str | None = None) -> Any:
        """Return cached exchange instance for the target market type, or build a new one."""
        target_market = (market_type or self._config.exchange_default_type or "FUTURE").upper()
        if target_market not in self._exchanges:
            # For backward compatibility and unit tests that inject a single mock/override exchange,
            # we check if there's exactly one exchange already registered in _exchanges.
            if self._exchange_injected and len(self._exchanges) == 1:
                return list(self._exchanges.values())[0]
            self._exchanges[target_market] = self._build_exchange(target_market)
        return self._exchanges[target_market]


    def _build_exchange(self, market_type: str | None = None) -> Any:
        try:
            import ccxt
        except ImportError as exc:
            raise RuntimeError("Missing dependency: ccxt") from exc

        exchange_id = self._config.exchange_id
        if not hasattr(ccxt, exchange_id):
            raise ValueError(f"Unsupported exchange id: {exchange_id}")

        options: dict[str, Any] = {}
        
        # Configure defaultType based on market_type
        target_market = (market_type or self._config.exchange_default_type or "FUTURE").upper()
        if target_market == "FUTURE":
            options["defaultType"] = "future"
        elif target_market == "SPOT":
            options["defaultType"] = "spot"
        elif target_market == "MARGIN":
            options["defaultType"] = "margin"
        else:
            options["defaultType"] = target_market.lower()

        if exchange_id == "binance":
            options["adjustForTimeDifference"] = True

        exchange = getattr(ccxt, exchange_id)(
            {
                "apiKey": self._config.exchange_api_key,
                "secret": self._config.exchange_api_secret,
                "password": self._config.exchange_api_passphrase,
                "enableRateLimit": True,
                "options": options,
            }
        )
        if self._config.exchange_sandbox:
            if exchange_id == "binance":
                if hasattr(exchange, 'enable_demo_trading'):
                    exchange.enable_demo_trading(True)
                elif hasattr(exchange, 'enableDemoTrading'):
                    exchange.enableDemoTrading(True)
                else:
                    exchange.set_sandbox_mode(True)
            else:
                exchange.set_sandbox_mode(True)
        return exchange

    async def cancel_order(self, order_id: str | None, symbol: str | None = None, market_type: str | None = None) -> bool:
        """Async wrapper to cancel an order on the exchange. Returns True on success."""
        if not order_id:
            self._logger.debug("cancel_order called without order_id")
            return False
        return await asyncio.to_thread(self._cancel_order_sync, order_id, symbol, market_type)

    def _cancel_order_sync(self, order_id: str, symbol: str | None = None, market_type: str | None = None) -> bool:
        try:
            ex = self._get_exchange(market_type)
            # CCXT signature: cancel_order(id, symbol=None, params={})
            if symbol:
                ex.cancel_order(order_id, symbol)
            else:
                ex.cancel_order(order_id)
            self._logger.info("Cancelled order id=%s symbol=%s", order_id, symbol)
            return True
        except Exception as exc:
            self._logger.warning("Failed to cancel order id=%s symbol=%s error=%s", order_id, symbol, exc)
            return False

    async def force_close_position(self, symbol: str | None, market_type: str | None = None) -> bool:
        """Attempt to force-close a position for `symbol`. Best-effort; returns True if invoked."""
        if not symbol:
            self._logger.debug("force_close_position called without symbol")
            return False
        return await asyncio.to_thread(self._force_close_sync, symbol, market_type)

    def _force_close_sync(self, symbol: str, market_type: str | None = None) -> bool:
        try:
            ex = self._get_exchange(market_type)
            # Prefer a dedicated method if provided by exchange wrapper
            if hasattr(ex, "close_position"):
                ex.close_position(symbol)
            else:
                # As a fallback, try to place a market reduce-only order if supported
                if hasattr(ex, "create_order"):
                    # Best-effort: attempt a market order with reduceOnly param
                    ex.create_order(symbol, "market", "sell", 0, None, {"reduceOnly": True})
            self._logger.info("Invoked force-close for symbol=%s", symbol)
            return True
        except Exception as exc:
            self._logger.warning("Failed to force-close position symbol=%s error=%s", symbol, exc)
            return False

    def _summarize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "signal_id": payload.get("signal_id"),
            "action": payload.get("action"),
            "symbol": payload.get("symbol") or payload.get("asset_pair") or payload.get("assetPair"),
            "market_type": payload.get("market_type") or "SPOT",
            "order_type": payload.get("order_type") or self._config.default_order_type,
            "amount": payload.get("amount") or payload.get("quantity") or payload.get("size") or self._config.default_order_amount,
            "leverage": payload.get("leverage"),
            "margin_mode": payload.get("margin_mode"),
        }

    def _summarize_order(self, order: dict[str, Any]) -> dict[str, Any]:
        params = order.get("params") or {}
        return {
            "symbol": order.get("symbol"),
            "type": order.get("type"),
            "side": order.get("side"),
            "amount": order.get("amount"),
            "price": order.get("price"),
            "params": {
                key: params[key]
                for key in ("clientOrderId", "reduceOnly", "timeInForce")
                if key in params
            },
        }

    def _create_synthetic_event(
        self,
        signal_id: str,
        event_type: Any,
        payload: dict[str, Any],
        timestamp: float | None = None
    ) -> Any:
        from datetime import datetime, timezone
        from .execution_event_transport import ExecutionEvent
        import uuid
        
        ts = timestamp or time.time()
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        
        return ExecutionEvent(
            event_id=f"sync-{signal_id}-{event_type.value.lower()}-{uuid.uuid4().hex[:8]}",
            signal_id=signal_id,
            sequence=0,
            event_type=event_type,
            sent_at=dt,
            exchange_time=dt,
            payload=payload,
        )

    def _sync_exchange_sync(
        self,
        signal_id: str,
        state: Any,
        order_id: str | None,
        symbol: str,
        market_type: str
    ) -> list[Any]:
        from .execution_event_transport import ExecutionEventType
        events = []
        exchange = self._get_exchange(market_type)
        
        order = None
        # 1. Truy vấn thông tin lệnh trực tiếp từ sàn bằng order_id nếu có
        if order_id:
            try:
                order = exchange.fetch_order(order_id, symbol)
            except Exception as e:
                self._logger.warning(
                    "sync_exchange: fetch_order failed for order_id=%s symbol=%s error=%s",
                    order_id, symbol, e
                )
                
        # 2. Nếu không tìm thấy hoặc sập trước khi ghi nhận order_id cục bộ:
        # Sử dụng signal_id làm clientOrderId để tìm kiếm lệnh trên sàn.
        # (Tại bước đặt lệnh, Executor luôn đính kèm 'clientOrderId': signal_id để đảm bảo tính khả trùng)
        if not order:
            try:
                all_orders = []
                if hasattr(exchange, 'fetch_orders'):
                    try:
                        all_orders = exchange.fetch_orders(symbol)
                    except Exception:
                        pass
                if not all_orders:
                    if hasattr(exchange, 'fetch_open_orders'):
                        try:
                            all_orders.extend(exchange.fetch_open_orders(symbol))
                        except Exception:
                            pass
                    if hasattr(exchange, 'fetch_closed_orders'):
                        try:
                            all_orders.extend(exchange.fetch_closed_orders(symbol))
                        except Exception:
                            pass
                
                # Quét danh sách lệnh trên sàn để tìm lệnh có clientOrderId khớp với signal_id cục bộ
                for ord_item in all_orders:
                    client_order_id = ord_item.get("clientOrderId") or ord_item.get("info", {}).get("clientOrderId")
                    if self._entry_client_order_id_matches_signal(client_order_id, signal_id) or ord_item.get("id") == order_id:
                        order = ord_item
                        break
            except Exception as e:
                self._logger.warning(
                    "sync_exchange: Failed to search orders for symbol=%s error=%s",
                    symbol, e
                )
                
        # 3. Phân tích trạng thái lệnh trên sàn và sinh các sự kiện giả lập (Synthetic Events)
        # để cập nhật máy trạng thái (State Machine) cục bộ.
        order_state = state.order_state
        order_filled = False
        
        if order:
            ccxt_status = order.get("status")  # open, closed, canceled, rejected
            resolved_order_id = order.get("id") or order_id or signal_id
            
            # Kịch bản A: Lệnh vẫn đang treo trên sàn (open)
            if ccxt_status == "open":
                if order_state == "NONE":
                    # Local chưa biết lệnh đã đặt -> Đưa local lên trạng thái PLACED
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_PLACED,
                        {"order_id": resolved_order_id, "symbol": symbol}
                    ))
            # Kịch bản B: Lệnh đã khớp hoàn toàn trên sàn (closed)
            elif ccxt_status == "closed":
                order_filled = True
                fill_price = order.get("price") or order.get("average") or 0.0
                if order_state == "NONE":
                    # Local chưa lưu gì -> Cho chuyển tiếp: NONE -> PLACED -> FILLED
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_PLACED,
                        {"order_id": resolved_order_id, "symbol": symbol}
                    ))
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_FILLED,
                        {"order_id": resolved_order_id, "symbol": symbol, "fill_price": fill_price}
                    ))
                elif order_state == "PLACED":
                    # Local đã biết lệnh được đặt -> Chỉ cần đẩy lên FILLED
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_FILLED,
                        {"order_id": resolved_order_id, "symbol": symbol, "fill_price": fill_price}
                    ))
            # Kịch bản C: Lệnh bị hủy (canceled / expired)
            elif ccxt_status in ("canceled", "expired"):
                if order_state == "NONE":
                    # Cho chuyển tiếp: NONE -> PLACED -> CANCELED
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_PLACED,
                        {"order_id": resolved_order_id, "symbol": symbol}
                    ))
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_CANCELED,
                        {"order_id": resolved_order_id, "symbol": symbol}
                    ))
                elif order_state == "PLACED":
                    # Đẩy từ PLACED -> CANCELED
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_CANCELED,
                        {"order_id": resolved_order_id, "symbol": symbol}
                    ))
            # Kịch bản D: Lệnh bị sàn từ chối (rejected)
            elif ccxt_status == "rejected":
                if order_state == "NONE":
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_PLACED,
                        {"order_id": resolved_order_id, "symbol": symbol}
                    ))
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_FAILED,
                        {"order_id": resolved_order_id, "symbol": symbol, "error": "Order rejected by exchange"}
                    ))
                elif order_state == "PLACED":
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_FAILED,
                        {"order_id": resolved_order_id, "symbol": symbol, "error": "Order rejected by exchange"}
                    ))
        
        # 4. Đối chiếu trạng thái vị thế (Position) thực tế trên sàn
        position_state = state.position_state
        
        if market_type == "FUTURE":
            pos_size = 0.0
            try:
                # Lấy toàn bộ vị thế hiện tại của symbol này trên sàn
                if hasattr(exchange, "fetch_positions"):
                    positions = exchange.fetch_positions([symbol])
                    for pos in positions:
                        if pos.get("symbol") == symbol:
                            pos_size = abs(float(pos.get("contracts") or pos.get("size") or 0.0))
                            break
            except Exception as e:
                self._logger.warning(
                    "sync_exchange: fetch_positions failed for symbol=%s error=%s",
                    symbol, e
                )
                
            is_position_open = pos_size > 0.0
            
            # Kịch bản A: Sàn đang mở vị thế (vị thế tồn tại thực tế)
            if is_position_open:
                # Nếu local chưa kịp ghi nhận lệnh khớp -> Đồng bộ bắt buộc lên FILLED trước
                if not order_filled and order_state not in ("FILLED",):
                    resolved_order_id = (order.get("id") if order else None) or order_id or signal_id
                    if order_state == "NONE":
                        events.append(self._create_synthetic_event(
                            signal_id,
                            ExecutionEventType.ORDER_PLACED,
                            {"order_id": resolved_order_id, "symbol": symbol}
                        ))
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.ORDER_FILLED,
                        {"order_id": resolved_order_id, "symbol": symbol}
                    ))
                    order_filled = True
                
                # Nếu cục bộ chưa biết vị thế đã mở -> Sinh sự kiện mở vị thế
                if position_state == "NONE":
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.POSITION_OPENED,
                        {"position_size": pos_size, "symbol": symbol}
                    ))
            # Kịch bản B: Sàn không có vị thế mở (vị thế đã đóng hoặc chưa mở)
            else:
                # Nếu cục bộ đang hiểu là vị thế đang mở -> Đồng bộ đóng vị thế
                if position_state in ("OPENED", "UPDATING"):
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.POSITION_CLOSED,
                        {"symbol": symbol}
                    ))
                # Nếu cục bộ hiểu là NONE nhưng thực tế lệnh đã khớp trên sàn
                # (nghĩa là vị thế đã được mở và đóng/hủy xong xuôi trước khi khôi phục)
                elif position_state == "NONE":
                    if order_filled or (order and order.get("status") == "closed"):
                        if not order_filled and order_state not in ("FILLED",):
                            resolved_order_id = order.get("id") or order_id or signal_id
                            if order_state == "NONE":
                                events.append(self._create_synthetic_event(
                                    signal_id,
                                    ExecutionEventType.ORDER_PLACED,
                                    {"order_id": resolved_order_id, "symbol": symbol}
                                ))
                            events.append(self._create_synthetic_event(
                                signal_id,
                                    ExecutionEventType.ORDER_FILLED,
                                {"order_id": resolved_order_id, "symbol": symbol}
                            ))
                        # Sinh cả chuỗi OPENED -> CLOSED để đưa local state về đích an toàn
                        events.append(self._create_synthetic_event(
                            signal_id,
                            ExecutionEventType.POSITION_OPENED,
                            {"symbol": symbol}
                        ))
                        events.append(self._create_synthetic_event(
                            signal_id,
                            ExecutionEventType.POSITION_CLOSED,
                            {"symbol": symbol}
                        ))
        
        # 5. Đối chiếu trạng thái cho tài khoản SPOT
        elif market_type == "SPOT":
            # Giao dịch Spot khớp lệnh là mua đứt bán đoạn ngay lập tức
            if order_filled or (order and order.get("status") == "closed"):
                if position_state == "NONE":
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.POSITION_OPENED,
                        {"symbol": symbol}
                    ))
                    events.append(self._create_synthetic_event(
                        signal_id,
                        ExecutionEventType.POSITION_CLOSED,
                        {"symbol": symbol}
                    ))
                    
        return events

    # ── Symbol normalisation ───────────────────────────────────────────────

    @staticmethod
    def _normalize_symbol(value: Any) -> str | None:
        if value is None:
            return None
        raw = str(value).strip().upper()
        if not raw:
            return None
        # Already in CCXT unified format (e.g. BTC/USDT or ETH/USDT:USDT)
        if "/" in raw:
            return raw
        # Attempt to reconstruct BASE/QUOTE from concatenated form (e.g. BTCUSDT)
        for quote in ("USDT", "USDC", "BUSD"):
            if raw.endswith(quote) and len(raw) > len(quote):
                base = raw[: -len(quote)]
                return f"{base}/{quote}"
        return raw


# ---------------------------------------------------------------------------
# Pure action → CCXT mapping function (module-level for testability)
# ---------------------------------------------------------------------------

def _map_action(action: str, signal_reduce_only: bool | None = None) -> tuple[str, bool]:
    """
    Map a canonical SignalAction to a (ccxt_side, reduce_only) tuple.

    Args:
        action:             One of the 4 canonical action names in _ACTION_MAP.
        signal_reduce_only: Explicit override from the signal payload.
                            When not None, overrides the action-derived default.

    Returns:
        (ccxt_side, reduce_only) where ccxt_side is "buy" or "sell".

    Raises:
        ValueError: If action is not in _ACTION_MAP.
    """
    entry = _ACTION_MAP.get(action)
    if entry is None:
        raise ValueError(
            f"Unsupported action: '{action}'. "
            f"Valid actions: {sorted(_ACTION_MAP)}"
        )
    side, default_reduce = entry
    final_reduce = signal_reduce_only if signal_reduce_only is not None else default_reduce
    return side, final_reduce


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None
