from __future__ import annotations

# pyright: reportMissingImports=false

import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_executor.config import ExecutorConfig
from local_executor.engine import LocalExecutorEngine
from local_executor.execution import SignalSchema, CcxtSignalExecutor


class LocalExecutorEngineTest(unittest.IsolatedAsyncioTestCase):
    async def test_should_forward_signal_to_handler(self) -> None:
        received: list[dict[str, Any]] = []

        async def on_signal(payload: dict[str, Any]) -> None:
            received.append(payload)

        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=0.01,
        )
        engine = LocalExecutorEngine(config=config, on_signal=on_signal)
        await engine._handle_signal(
            {
                "signal_id": "sig-2",
                "action": "CLOSE",
                "symbol": "BTCUSDT",
            }
        )

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["signal_id"], "sig-2")


class SignalSchemaValidationTest(unittest.TestCase):
    def test_should_accept_minimal_valid_signal(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertTrue(is_valid, f"Expected valid signal but got errors: {errors}")
        self.assertEqual(errors, [])

    def test_should_reject_missing_signal_id(self) -> None:
        signal = {
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("signal_id", str(errors))

    def test_should_reject_empty_signal_id(self) -> None:
        signal = {
            "signal_id": "",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("signal_id must be non-empty", str(errors))

    def test_should_reject_missing_action(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "symbol": "BTCUSDT",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("action", str(errors))

    def test_should_reject_invalid_action(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "INVALID_ACTION",
            "symbol": "BTCUSDT",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("Invalid action", str(errors))

    def test_should_accept_valid_actions(self) -> None:
        for action in [
            "OPEN_LONG",
            "CLOSE_LONG",
            "OPEN_SHORT",
            "CLOSE_SHORT",
            "BUY",
            "SELL",
            "SELL_SHORT",
            "BUY_TO_COVER",
        ]:
            signal = {
                "signal_id": f"sig-{action}",
                "action": action,
                "symbol": "BTCUSDT",
            }
            is_valid, errors = SignalSchema.validate(signal)
            self.assertTrue(
                is_valid, f"Expected {action} to be valid but got errors: {errors}"
            )

    def test_should_reject_missing_symbol(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("symbol", str(errors))

    def test_should_accept_asset_pair_as_symbol_alias(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "asset_pair": "ETHUSDT",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertTrue(is_valid, f"Expected valid signal with asset_pair: {errors}")

    def test_should_accept_assetPair_as_symbol_alias(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "assetPair": "ETHUSDT",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertTrue(is_valid, f"Expected valid signal with assetPair: {errors}")

    def test_should_accept_market_order_type(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "order_type": "market",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertTrue(is_valid, f"Expected valid market order: {errors}")

    def test_should_accept_limit_order_type_with_price(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "order_type": "limit",
            "price": 65000.50,
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertTrue(is_valid, f"Expected valid limit order: {errors}")

    def test_should_reject_limit_order_type_without_price(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "order_type": "limit",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("price", str(errors))

    def test_should_reject_invalid_order_type(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "order_type": "stop_loss",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("order_type", str(errors))

    def test_should_accept_amount_as_quantity(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "quantity": 0.5,
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertTrue(is_valid, f"Expected valid signal with quantity: {errors}")

    def test_should_accept_amount_as_size(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "size": 1.5,
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertTrue(is_valid, f"Expected valid signal with size: {errors}")

    def test_should_reject_negative_amount(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "amount": -0.5,
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("amount must be > 0", str(errors))

    def test_should_reject_zero_amount(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "amount": 0,
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("amount must be > 0", str(errors))

    def test_should_reject_non_numeric_amount(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "amount": "invalid",
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertFalse(is_valid)
        self.assertIn("amount must be numeric", str(errors))

    def test_should_reject_non_dictionary_payload(self) -> None:
        is_valid, errors = SignalSchema.validate("not a dict")
        self.assertFalse(is_valid)
        self.assertIn("dictionary", str(errors))

    def test_should_accept_optional_fields(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "amount": 0.5,
            "order_type": "limit",
            "price": 65000.0,
            "time_in_force": "GTC",
            "metadata": {"strategy": "sma", "confidence": 0.92},
        }
        is_valid, errors = SignalSchema.validate(signal)
        self.assertTrue(is_valid, f"Expected valid signal with all fields: {errors}")


class CcxtSignalExecutorBuildOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=1.0,
            default_order_type="market",
            execution_mode="dry-run",
        )
        self.executor = CcxtSignalExecutor(config)

    def test_should_normalize_symbol_to_ccxt_format(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["symbol"], "BTC/USDT")

    def test_should_handle_already_normalized_symbol(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTC/USDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["symbol"], "BTC/USDT")

    def test_should_use_asset_pair_as_symbol(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "asset_pair": "ETHUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["symbol"], "ETH/USDT")

    def test_should_map_open_long_to_buy(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["side"], "buy")
        self.assertNotIn("reduceOnly", order.get("params", {}))

    def test_should_map_close_long_to_sell_with_reduce_only(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "CLOSE_LONG",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["side"], "sell")
        self.assertTrue(order.get("params", {}).get("reduceOnly"))

    def test_should_map_open_short_to_sell(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_SHORT",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["side"], "sell")
        self.assertNotIn("reduceOnly", order.get("params", {}))

    def test_should_map_close_short_to_buy_with_reduce_only(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "CLOSE_SHORT",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["side"], "buy")
        self.assertTrue(order.get("params", {}).get("reduceOnly"))

    def test_should_map_buy_alias_to_buy(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "BUY",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["side"], "buy")

    def test_should_map_sell_alias_to_sell_with_reduce_only(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "SELL",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["side"], "sell")
        self.assertTrue(order.get("params", {}).get("reduceOnly"))

    def test_should_use_default_order_amount_when_missing(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["amount"], 1.0)

    def test_should_use_provided_amount(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "amount": 0.5,
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["amount"], 0.5)

    def test_should_accept_quantity_as_amount_alias(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "quantity": 2.5,
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["amount"], 2.5)

    def test_should_default_to_market_order_type(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["type"], "market")
        self.assertIsNone(order["price"])

    def test_should_accept_limit_order_with_price(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "order_type": "limit",
            "price": 65000.50,
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order["type"], "limit")
        self.assertEqual(order["price"], 65000.50)

    def test_should_accept_time_in_force_in_params(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "OPEN_LONG",
            "symbol": "BTCUSDT",
            "time_in_force": "GTC",
        }
        order = self.executor._build_order(signal)
        self.assertEqual(order.get("params", {}).get("timeInForce"), "GTC")

    def test_should_reject_invalid_action(self) -> None:
        signal = {
            "signal_id": "sig-001",
            "action": "INVALID_ACTION",
            "symbol": "BTCUSDT",
        }
        with self.assertRaises(ValueError) as ctx:
            self.executor._build_order(signal)
        self.assertIn("Unsupported action", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

