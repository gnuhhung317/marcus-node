from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from .config import ExecutorConfig


@dataclass(slots=True)
class ExecutionResult:
    mode: str
    order_id: str | None
    details: dict[str, Any]
    errors: list[str] | None = None


class SignalSchema:
    """Signal payload contract from backend."""

    REQUIRED_FIELDS = {"signal_id", "action", "symbol"}
    OPTIONAL_FIELDS = {
        "amount",
        "quantity",
        "size",
        "order_type",
        "orderType",
        "price",
        "limit_price",
        "limitPrice",
        "time_in_force",
        "timeInForce",
        "asset_pair",
        "assetPair",
    }

    VALID_ACTIONS = {
        "OPEN_LONG",
        "CLOSE_LONG",
        "OPEN_SHORT",
        "CLOSE_SHORT",
        "BUY",
        "SELL",
        "SELL_SHORT",
        "BUY_TO_COVER",
    }

    @staticmethod
    def validate(payload: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate signal payload against schema.
        
        Returns:
            (is_valid, error_list)
        """
        errors: list[str] = []

        if not isinstance(payload, dict):
            return False, ["Payload must be a dictionary"]

        for field in SignalSchema.REQUIRED_FIELDS:
            if field not in payload or payload[field] in (None, ""):
                errors.append(f"Missing required field: {field}")

        if errors:
            return False, errors

        signal_id = str(payload.get("signal_id", "")).strip()
        if not signal_id:
            errors.append("signal_id must be non-empty string")

        action = str(payload.get("action", "")).strip().upper()
        if action not in SignalSchema.VALID_ACTIONS:
            errors.append(
                f"Invalid action: {action}. Must be one of {SignalSchema.VALID_ACTIONS}"
            )

        symbol = str(
            payload.get("symbol")
            or payload.get("asset_pair")
            or payload.get("assetPair", "")
        ).strip()
        if not symbol:
            errors.append("symbol (or asset_pair/assetPair) must be non-empty string")

        order_type = str(
            payload.get("order_type") or payload.get("orderType", "market")
        ).strip().lower()
        if order_type not in {"market", "limit"}:
            errors.append(f"Invalid order_type: {order_type}. Must be 'market' or 'limit'")

        if order_type == "limit":
            price = (
                payload.get("price")
                or payload.get("limit_price")
                or payload.get("limitPrice")
            )
            if price in (None, ""):
                errors.append("Limit orders require price field")
            else:
                try:
                    float(price)
                except (ValueError, TypeError):
                    errors.append(f"price must be numeric, got {price}")

        amount = (
            payload.get("amount") or payload.get("quantity") or payload.get("size")
        )
        if amount not in (None, ""):
            try:
                amt = float(amount)
                if amt <= 0:
                    errors.append(f"amount must be > 0, got {amt}")
            except (ValueError, TypeError):
                errors.append(f"amount must be numeric, got {amount}")

        return len(errors) == 0, errors


class CcxtSignalExecutor:
    def __init__(self, config: ExecutorConfig, logger: logging.Logger | None = None) -> None:
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._exchange = None

    async def execute_signal(self, payload: dict[str, Any]) -> ExecutionResult:
        return await asyncio.to_thread(self._execute_signal_sync, payload)

    def _execute_signal_sync(self, payload: dict[str, Any]) -> ExecutionResult:
        is_valid, errors = SignalSchema.validate(payload)
        if not is_valid:
            self._logger.error("Signal validation failed: %s", errors)
            return ExecutionResult(
                mode="error",
                order_id=None,
                details={"signal": payload},
                errors=errors,
            )

        try:
            order = self._build_order(payload)
        except (ValueError, KeyError) as exc:
            self._logger.error("Failed to build order from signal: %s", exc)
            return ExecutionResult(
                mode="error",
                order_id=None,
                details={"signal": payload},
                errors=[str(exc)],
            )

        if self._config.execution_mode == "dry-run":
            self._logger.info("Dry-run execution: %s", order)
            return ExecutionResult(mode="dry-run", order_id=None, details=order)

        try:
            exchange = self._exchange or self._build_exchange()
            self._exchange = exchange
            created = exchange.create_order(
                order["symbol"],
                order["type"],
                order["side"],
                order["amount"],
                order.get("price"),
                order.get("params") or {},
            )
            return ExecutionResult(
                mode="live",
                order_id=str(created.get("id")) if isinstance(created, dict) else None,
                details=created if isinstance(created, dict) else {"raw": created},
            )
        except Exception as exc:
            self._logger.error("Live execution failed: %s", exc)
            return ExecutionResult(
                mode="error",
                order_id=None,
                details={"signal": payload, "order": order},
                errors=[str(exc)],
            )

    def _build_exchange(self) -> Any:
        try:
            import ccxt
        except ImportError as exc:
            raise RuntimeError("Missing dependency: ccxt") from exc

        exchange_id = self._config.exchange_id
        if not hasattr(ccxt, exchange_id):
            raise ValueError(f"Unsupported exchange id: {exchange_id}")

        exchange_class = getattr(ccxt, exchange_id)
        options: dict[str, Any] = {}
        if self._config.exchange_default_type:
            options["defaultType"] = self._config.exchange_default_type

        exchange = exchange_class(
            {
                "apiKey": self._config.exchange_api_key,
                "secret": self._config.exchange_api_secret,
                "password": self._config.exchange_api_passphrase,
                "enableRateLimit": True,
                "options": options,
            }
        )
        if self._config.exchange_sandbox:
            exchange.set_sandbox_mode(True)
        return exchange

    def _build_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action", "")).strip().upper()
        symbol = self._normalize_symbol(payload.get("symbol") or payload.get("asset_pair") or payload.get("assetPair"))
        if not symbol:
            raise ValueError("Signal missing symbol")

        order_type = str(payload.get("order_type") or payload.get("orderType") or self._config.default_order_type)
        order_type = order_type.strip().lower() or "market"

        amount = payload.get("amount") or payload.get("quantity") or payload.get("size")
        if amount in (None, ""):
            amount = self._config.default_order_amount
        amount = float(amount)
        if amount <= 0:
            raise ValueError("Order amount must be > 0")

        side, reduce_only = self._map_action(action)

        price = payload.get("price") or payload.get("limit_price") or payload.get("limitPrice")
        if order_type == "limit":
            if price in (None, ""):
                raise ValueError("Limit orders require price")
            price = float(price)
        else:
            price = None

        params: dict[str, Any] = {}
        if reduce_only:
            params["reduceOnly"] = True

        time_in_force = payload.get("time_in_force") or payload.get("timeInForce")
        if time_in_force:
            params["timeInForce"] = str(time_in_force)

        return {
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "params": params,
        }

    @staticmethod
    def _map_action(action: str) -> tuple[str, bool]:
        if action in {"OPEN_LONG", "BUY"}:
            return "buy", False
        if action in {"CLOSE_LONG", "SELL"}:
            return "sell", True
        if action in {"OPEN_SHORT", "SELL_SHORT"}:
            return "sell", False
        if action in {"CLOSE_SHORT", "BUY_TO_COVER"}:
            return "buy", True
        raise ValueError(f"Unsupported action: {action}")

    @staticmethod
    def _normalize_symbol(value: Any) -> str | None:
        if value is None:
            return None
        raw = str(value).strip().upper()
        if not raw:
            return None
        if "/" in raw:
            return raw
        for quote in ("USDT", "USDC", "BUSD"):
            if raw.endswith(quote) and len(raw) > len(quote):
                base = raw[: -len(quote)]
                return f"{base}/{quote}"
        return raw