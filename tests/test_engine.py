from __future__ import annotations

# pyright: reportMissingImports=false

import sys
import unittest
from pathlib import Path
from typing import Any
import asyncio

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_executor.config import ExecutorConfig
from local_executor.engine import LocalExecutorEngine
from local_executor.execution import ExecutionResult, SignalSchema, CcxtSignalExecutor


class _FakeNotifier:
    enabled = True

    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> bool:
        self.messages.append(message)
        return True


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

    async def test_sweeper_invokes_cancel(self) -> None:
        from unittest.mock import AsyncMock
        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=0.01,
        )

        store = __import__("local_executor.local_store", fromlist=["LocalExecutionStore"]).LocalExecutionStore(":memory:")
        await store.initialize()

        engine = LocalExecutorEngine(config=config, local_store=store)

        # Prepare a signal with past cancel deadline
        sid = "sig-sweeper-1"
        await store.get_or_create_signal(sid)
        import time
        past = int(time.time()) - 10
        await store.update_signal_state(sid, signal_state="OPEN", policies={"cancelOrderAfter": past}, order_id="ord-xyz", order_symbol="BTC/USDT")

        # Inject async mock cancel_order
        engine._executor.cancel_order = AsyncMock(return_value=True)

        stop_event = asyncio.Event()
        task = asyncio.create_task(engine._deadline_sweeper_loop(stop_event), name="sweeper_test")

        # Let the sweeper run once (it waits 1s then checks)
        await asyncio.sleep(1.6)
        stop_event.set()
        await task

        engine._executor.cancel_order.assert_awaited()

    async def test_should_notify_on_execution_error(self) -> None:
        from unittest.mock import AsyncMock

        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=0.01,
            telegram_bot_token="token",
            telegram_chat_id="chat",
        )
        engine = LocalExecutorEngine(config=config)
        fake_notifier = _FakeNotifier()
        engine._notifier = fake_notifier
        engine._executor.execute_signal = AsyncMock(
            return_value=ExecutionResult(
                mode="error",
                order_id=None,
                details={},
                errors=["order rejected by exchange"],
            )
        )

        await engine._default_signal_handler(
            {
                "signal_id": "sig-error",
                "action": "OPEN_LONG",
                "symbol": "BTCUSDT",
            }
        )

        self.assertEqual(len(fake_notifier.messages), 1)
        self.assertIn("Execution error", fake_notifier.messages[0])
        self.assertIn("sig-error", fake_notifier.messages[0])
        self.assertIn("order rejected by exchange", fake_notifier.messages[0])

    async def test_sweeper_notifies_for_forced_close(self) -> None:
        from unittest.mock import AsyncMock

        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=0.01,
            telegram_bot_token="token",
            telegram_chat_id="chat",
        )

        store = __import__("local_executor.local_store", fromlist=["LocalExecutionStore"]).LocalExecutionStore(":memory:")
        await store.initialize()

        engine = LocalExecutorEngine(config=config, local_store=store)
        fake_notifier = _FakeNotifier()
        engine._notifier = fake_notifier

        sid = "sig-sweeper-close"
        await store.get_or_create_signal(sid)
        import time
        past = int(time.time()) - 10
        await store.update_signal_state(
            sid,
            signal_state="OPEN",
            position_state="OPENED",
            policies={"closePositionAfter": past},
            order_symbol="BTC/USDT",
        )

        engine._executor.force_close_position = AsyncMock(return_value=True)

        stop_event = asyncio.Event()
        task = asyncio.create_task(engine._deadline_sweeper_loop(stop_event), name="sweeper_notify_test")

        await asyncio.sleep(1.6)
        stop_event.set()
        await task

        engine._executor.force_close_position.assert_awaited()
        self.assertEqual(len(fake_notifier.messages), 1)
        self.assertIn("Emergency forced-close sweep", fake_notifier.messages[0])
        self.assertIn(sid, fake_notifier.messages[0])

    async def test_should_persist_entry_and_protection_metadata_without_rebuilding_order(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from local_executor.local_store import LocalExecutionStore

        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=0.01,
            telegram_bot_token="token",
            telegram_chat_id="chat",
            exchange_default_type="FUTURE",
        )

        store = LocalExecutionStore(":memory:")
        await store.initialize()
        engine = LocalExecutorEngine(config=config, local_store=store)
        engine._executor.execute_signal = AsyncMock(
            return_value=ExecutionResult(
                mode="live",
                order_id="entry-1",
                details={"ok": True},
                entry_order_id="entry-1",
                tp_order_id="tp-1",
                sl_order_id="sl-1",
                symbol="BTC/USDT",
                market_type="FUTURE",
                action="OPEN_LONG",
                requested_amount=1.0,
                filled_amount=1.0,
                take_profit=71000.0,
                stop_loss=64000.0,
                execution_status="PROTECTED",
                protection_status="PROTECTED",
            )
        )
        engine._executor._build_order = MagicMock(side_effect=AssertionError("_build_order should not be called"))

        await engine._default_signal_handler(
            {
                "signal_id": "sig-store",
                "action": "OPEN_LONG",
                "symbol": "BTCUSDT",
                "market_type": "FUTURE",
                "policies": {"market_type": "FUTURE"},
            }
        )

        state = await store.get_signal_state("sig-store")
        self.assertIsNotNone(state)
        self.assertEqual(state.order_id, "entry-1")
        self.assertEqual(state.order_state, "FILLED")
        self.assertEqual(state.position_state, "OPENED")
        self.assertEqual(state.order_symbol, "BTC/USDT")
        self.assertEqual(state.market_type, "FUTURE")
        self.assertEqual(state.action, "OPEN_LONG")
        self.assertEqual(state.filled_amount, 1.0)
        self.assertEqual(state.tp_order_id, "tp-1")
        self.assertEqual(state.sl_order_id, "sl-1")
        self.assertEqual(state.take_profit, 71000.0)
        self.assertEqual(state.stop_loss, 64000.0)
        self.assertEqual(state.protection_status, "PROTECTED")
        engine._executor._build_order.assert_not_called()


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


class CcxtSignalExecutorMultiMarketTest(unittest.TestCase):
    def test_should_cache_exchanges_separately_and_set_correct_options(self) -> None:
        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=1.0,
            default_order_type="market",
            execution_mode="live",
        )
        executor = CcxtSignalExecutor(config)
        
        # We can mock the ccxt dependency or check _build_exchange output
        from unittest.mock import patch, MagicMock
        with patch("ccxt.binance") as mock_binance:
            mock_spot = MagicMock()
            mock_future = MagicMock()
            
            # Set side effect to return spot or future based on the options defaultType passed to constructor
            def side_effect(config_dict):
                options = config_dict.get("options", {})
                default_type = options.get("defaultType")
                if default_type == "spot":
                    return mock_spot
                elif default_type == "future":
                    return mock_future
                return MagicMock()
            
            mock_binance.side_effect = side_effect
            
            spot_ex = executor._get_exchange("SPOT")
            future_ex = executor._get_exchange("FUTURE")
            
            self.assertEqual(spot_ex, mock_spot)
            self.assertEqual(future_ex, mock_future)
            self.assertNotEqual(spot_ex, future_ex)
            
            # Ensure calling it again returns the cached instances
            self.assertEqual(executor._get_exchange("SPOT"), mock_spot)
            self.assertEqual(executor._get_exchange("FUTURE"), future_ex)

    def test_backward_compatibility_with_mock_exchange(self) -> None:
        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=1.0,
        )
        executor = CcxtSignalExecutor(config)
        
        mock_ex = "my-mocked-exchange"
        executor._exchange = mock_ex
        
        # When _exchange property is directly injected, _get_exchange should fall back to it
        self.assertEqual(executor._get_exchange("SPOT"), mock_ex)
        self.assertEqual(executor._get_exchange("FUTURE"), mock_ex)


class CcxtSignalExecutorUpdateTpSlTest(unittest.IsolatedAsyncioTestCase):
    def _executor(self, execution_mode: str = "live") -> CcxtSignalExecutor:
        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=1.0,
            default_order_type="market",
            execution_mode=execution_mode,
        )
        return CcxtSignalExecutor(config)

    async def test_should_dry_run_update_tp_sl(self) -> None:
        executor = self._executor(execution_mode="dry-run")

        result = await executor.execute_signal(
            {
                "signal_id": "sig-update",
                "action": "UPDATE_TP_SL",
                "symbol": "BTCUSDT",
                "tp": 71000,
                "sl": 64000,
            }
        )

        self.assertEqual(result.mode, "dry-run")
        self.assertEqual(result.details["symbol"], "BTC/USDT")
        self.assertEqual(result.details["take_profit"], 71000.0)
        self.assertEqual(result.details["stop_loss"], 64000.0)

    async def test_should_modify_matching_tp_sl_orders_when_supported(self) -> None:
        from unittest.mock import MagicMock

        executor = self._executor()
        exchange = MagicMock()
        exchange.has = {"modifyOrder": True}
        exchange.fetch_open_orders.return_value = [
            {
                "id": "tp-1",
                "symbol": "BTC/USDT",
                "type": "take_profit_market",
                "side": "sell",
                "amount": 0.25,
                "price": 70000,
                "params": {"reduceOnly": True},
            }
        ]
        exchange.modify_order.return_value = {"id": "tp-1", "status": "open"}
        executor._exchange = exchange

        result = await executor.execute_signal(
            {
                "signal_id": "sig-update",
                "action": "UPDATE_TP_SL",
                "symbol": "BTCUSDT",
                "order_id": "tp-1",
                "tp": 71000,
            }
        )

        self.assertEqual(result.mode, "live")
        self.assertEqual(result.order_id, "tp-1")
        self.assertEqual(result.details["strategy"], "modify_order")
        exchange.modify_order.assert_called_once()
        call_args = exchange.modify_order.call_args.args
        self.assertEqual(call_args[0], "tp-1")
        self.assertEqual(call_args[1], "BTC/USDT")
        self.assertEqual(call_args[5], 71000.0)
        self.assertEqual(call_args[6]["stopPrice"], 71000.0)

    async def test_should_cancel_and_replace_when_modify_order_unsupported(self) -> None:
        from unittest.mock import MagicMock

        executor = self._executor()
        exchange = MagicMock()
        exchange.has = {"modifyOrder": False}
        exchange.fetch_open_orders.return_value = [
            {
                "id": "sl-1",
                "symbol": "ETH/USDT",
                "type": "stop_loss_market",
                "side": "sell",
                "amount": 1.5,
                "price": 3200,
                "params": {"reduceOnly": True},
            }
        ]
        exchange.create_order.return_value = {"id": "sl-2", "status": "open"}
        executor._exchange = exchange

        result = await executor.execute_signal(
            {
                "signal_id": "sig-update",
                "action": "UPDATE_TP_SL",
                "symbol": "ETHUSDT",
                "sl": 3150,
            }
        )

        self.assertEqual(result.mode, "live")
        self.assertEqual(result.order_id, "sl-2")
        self.assertEqual(result.details["strategy"], "cancel_replace")
        exchange.cancel_order.assert_called_once_with("sl-1", "ETH/USDT")
        exchange.create_order.assert_called_once()
        call_args = exchange.create_order.call_args.args
        self.assertEqual(call_args[0], "ETH/USDT")
        self.assertEqual(call_args[4], 3150.0)
        self.assertEqual(call_args[5]["stopPrice"], 3150.0)


class CcxtSignalExecutorProtectionFlowTest(unittest.IsolatedAsyncioTestCase):
    def _executor(self, execution_mode: str = "live", exchange_default_type: str | None = "FUTURE") -> CcxtSignalExecutor:
        config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=1.0,
            default_order_type="market",
            execution_mode=execution_mode,
            exchange_default_type=exchange_default_type,
        )
        return CcxtSignalExecutor(config)

    async def test_should_create_entry_then_sl_then_tp_for_open_long(self) -> None:
        from unittest.mock import MagicMock

        executor = self._executor()
        exchange = MagicMock()
        exchange.create_order.side_effect = [
            {
                "id": "entry-1",
                "status": "closed",
                "filled": 0.25,
                "symbol": "BTC/USDT",
                "side": "buy",
                "price": 65000.0,
                "average": 65000.0,
            },
            {"id": "sl-1", "status": "open", "symbol": "BTC/USDT"},
            {"id": "tp-1", "status": "open", "symbol": "BTC/USDT"},
        ]
        executor._exchange = exchange

        result = await executor.execute_signal(
            {
                "signal_id": "sig-protect-long",
                "action": "OPEN_LONG",
                "symbol": "BTCUSDT",
                "market_type": "FUTURE",
                "amount": 0.25,
                "take_profit": 71000,
                "stop_loss": 64000,
            }
        )

        self.assertEqual(result.mode, "live")
        self.assertEqual(result.order_id, "entry-1")
        self.assertEqual(result.entry_order_id, "entry-1")
        self.assertEqual(result.sl_order_id, "sl-1")
        self.assertEqual(result.tp_order_id, "tp-1")
        self.assertEqual(result.execution_status, "PROTECTED")
        self.assertEqual(result.protection_status, "PROTECTED")

        self.assertEqual(exchange.create_order.call_count, 3)
        entry_call, sl_call, tp_call = exchange.create_order.call_args_list
        self.assertEqual(entry_call.args[0], "BTC/USDT")
        self.assertEqual(entry_call.args[1], "market")
        self.assertEqual(entry_call.args[2], "buy")
        self.assertEqual(entry_call.args[5]["clientOrderId"], "sig-protect-long-entry")
        self.assertEqual(sl_call.args[1], "STOP_MARKET")
        self.assertEqual(sl_call.args[2], "sell")
        self.assertTrue(sl_call.args[5]["reduceOnly"])
        self.assertEqual(sl_call.args[5]["triggerPrice"], 64000.0)
        self.assertEqual(sl_call.args[5]["clientOrderId"], "sig-protect-long-sl")
        self.assertEqual(tp_call.args[1], "TAKE_PROFIT_MARKET")
        self.assertEqual(tp_call.args[2], "sell")
        self.assertTrue(tp_call.args[5]["reduceOnly"])
        self.assertEqual(tp_call.args[5]["triggerPrice"], 71000.0)
        self.assertEqual(tp_call.args[5]["clientOrderId"], "sig-protect-long-tp")

    async def test_should_retry_sl_without_retrying_entry(self) -> None:
        from unittest.mock import MagicMock

        executor = self._executor()
        exchange = MagicMock()
        exchange.create_order.side_effect = [
            {
                "id": "entry-2",
                "status": "closed",
                "filled": 0.5,
                "symbol": "BTC/USDT",
                "side": "buy",
                "price": 65000.0,
                "average": 65000.0,
            },
            RuntimeError("SL failed"),
            RuntimeError("SL failed"),
            RuntimeError("SL failed"),
        ]
        executor._exchange = exchange

        result = await executor.execute_signal(
            {
                "signal_id": "sig-protect-fail",
                "action": "OPEN_LONG",
                "symbol": "BTCUSDT",
                "market_type": "FUTURE",
                "amount": 0.5,
                "take_profit": 71000,
                "stop_loss": 64000,
            }
        )

        self.assertEqual(result.mode, "live")
        self.assertEqual(result.order_id, "entry-2")
        self.assertEqual(result.entry_order_id, "entry-2")
        self.assertIsNone(result.tp_order_id)
        self.assertIsNone(result.sl_order_id)
        self.assertEqual(result.execution_status, "ENTRY_FILLED_UNPROTECTED")
        self.assertEqual(result.protection_status, "UNPROTECTED")
        self.assertIsNotNone(result.errors)
        self.assertIn("Stop-loss protection failed", result.errors[0])
        self.assertEqual(exchange.create_order.call_count, 4)

    async def test_should_use_config_default_market_type_when_payload_omits_it(self) -> None:
        from unittest.mock import MagicMock

        executor = self._executor(exchange_default_type="FUTURE")
        future_exchange = MagicMock()
        future_exchange.create_order.side_effect = [
            {
                "id": "entry-3",
                "status": "closed",
                "filled": 1.0,
                "symbol": "BTC/USDT",
                "side": "buy",
                "price": 65000.0,
                "average": 65000.0,
            }
        ]
        spot_exchange = MagicMock()
        executor._exchanges["FUTURE"] = future_exchange
        executor._exchanges["SPOT"] = spot_exchange
        executor._exchange_injected = True

        result = await executor.execute_signal(
            {
                "signal_id": "sig-default-market-type",
                "action": "OPEN_LONG",
                "symbol": "BTCUSDT",
                "amount": 1.0,
            }
        )

        self.assertEqual(result.market_type, "FUTURE")
        self.assertEqual(result.execution_status, "ENTRY_FILLED")
        self.assertTrue(future_exchange.create_order.called)
        self.assertFalse(spot_exchange.create_order.called)


if __name__ == "__main__":
    unittest.main()


