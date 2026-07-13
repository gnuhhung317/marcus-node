from __future__ import annotations

import unittest
from unittest.mock import MagicMock
import asyncio
from datetime import datetime, timezone

from local_executor.config import ExecutorConfig
from local_executor.execution import CcxtSignalExecutor
from local_executor.local_store import LocalExecutionStore
from local_executor.execution_event_transport import ExecutionEvent, ExecutionEventType


class TestExchangeSync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.config = ExecutorConfig(
            ws_url="ws://localhost/ws",
            ws_token="ws-token",
            bot_id="bot-01",
            exchange_id="binance",
            exchange_api_key="key",
            exchange_api_secret="secret",
            default_order_amount=0.01,
        )
        self.store = LocalExecutionStore(":memory:")
        await self.store.initialize()
        self.executor = CcxtSignalExecutor(config=self.config)

    async def asyncTearDown(self) -> None:
        await self.store.close()

    async def test_resolve_symbol_from_state(self) -> None:
        signal_id = "sig-1"
        await self.store.get_or_create_signal(signal_id)
        await self.store.update_signal_state(
            signal_id,
            order_symbol="BTC/USDT"
        )
        state = await self.store.get_signal_state(signal_id)
        symbol = await self.executor._resolve_symbol(signal_id, state, self.store)
        self.assertEqual(symbol, "BTC/USDT")

    async def test_resolve_symbol_from_event(self) -> None:
        signal_id = "sig-2"
        await self.store.get_or_create_signal(signal_id)
        # Store an execution event with the symbol inside the payload
        event = ExecutionEvent(
            event_id="evt-1",
            signal_id=signal_id,
            sequence=1,
            event_type=ExecutionEventType.ORDER_PLACED,
            sent_at=datetime.now(timezone.utc),
            exchange_time=None,
            payload={"symbol": "ETH/USDT"}
        )
        await self.store.store_event(event)
        state = await self.store.get_signal_state(signal_id)
        symbol = await self.executor._resolve_symbol(signal_id, state, self.store)
        self.assertEqual(symbol, "ETH/USDT")

    async def test_resolve_symbol_from_policies(self) -> None:
        signal_id = "sig-3"
        await self.store.get_or_create_signal(signal_id)
        await self.store.update_signal_state(
            signal_id,
            policies={"symbol": "SOL/USDT"}
        )
        state = await self.store.get_signal_state(signal_id)
        symbol = await self.executor._resolve_symbol(signal_id, state, self.store)
        self.assertEqual(symbol, "SOL/USDT")

    async def test_resolve_symbol_failure(self) -> None:
        signal_id = "sig-4"
        await self.store.get_or_create_signal(signal_id)
        state = await self.store.get_signal_state(signal_id)
        with self.assertRaises(ValueError):
            await self.executor._resolve_symbol(signal_id, state, self.store)

    async def test_sync_exchange_spot_order_filled(self) -> None:
        # Reconcile SPOT order filled state
        signal_id = "sig-spot-filled"
        await self.store.get_or_create_signal(signal_id)
        await self.store.update_signal_state(
            signal_id,
            signal_state="OPEN",
            order_state="NONE",
            position_state="NONE",
            policies={"market_type": "SPOT"},
            order_id="ord-spot-filled",
            order_symbol="BTC/USDT"
        )
        state = await self.store.get_signal_state(signal_id)

        # Mock the exchange
        mock_exchange = MagicMock()
        mock_exchange.fetch_order.return_value = {
            "id": "ord-spot-filled",
            "status": "closed",
            "symbol": "BTC/USDT",
            "price": 60000.0,
            "average": 60000.0
        }
        self.executor._exchanges["SPOT"] = mock_exchange
        self.executor._exchange_injected = True

        events = await self.executor.sync_exchange(signal_id, self.store)
        
        # We expect: ORDER_PLACED, ORDER_FILLED, POSITION_OPENED, POSITION_CLOSED for SPOT filled
        self.assertEqual(len(events), 4)
        self.assertEqual(events[0].event_type, ExecutionEventType.ORDER_PLACED)
        self.assertEqual(events[1].event_type, ExecutionEventType.ORDER_FILLED)
        self.assertEqual(events[1].payload["fill_price"], 60000.0)
        self.assertEqual(events[2].event_type, ExecutionEventType.POSITION_OPENED)
        self.assertEqual(events[3].event_type, ExecutionEventType.POSITION_CLOSED)

    async def test_sync_exchange_future_open_position(self) -> None:
        signal_id = "sig-future-open"
        await self.store.get_or_create_signal(signal_id)
        await self.store.update_signal_state(
            signal_id,
            signal_state="OPEN",
            order_state="PLACED",
            position_state="NONE",
            policies={"market_type": "FUTURE"},
            order_id="ord-future-open",
            order_symbol="BTC/USDT"
        )
        state = await self.store.get_signal_state(signal_id)

        # Mock the exchange
        mock_exchange = MagicMock()
        mock_exchange.fetch_order.return_value = {
            "id": "ord-future-open",
            "status": "closed",
            "symbol": "BTC/USDT",
            "price": 60000.0,
            "average": 60000.0
        }
        mock_exchange.fetch_positions.return_value = [
            {
                "symbol": "BTC/USDT",
                "contracts": 0.05,
                "size": 0.05
            }
        ]
        self.executor._exchanges["FUTURE"] = mock_exchange
        self.executor._exchange_injected = True

        events = await self.executor.sync_exchange(signal_id, self.store)

        # Since order_state was PLACED:
        # We expect: ORDER_FILLED, POSITION_OPENED
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].event_type, ExecutionEventType.ORDER_FILLED)
        self.assertEqual(events[1].event_type, ExecutionEventType.POSITION_OPENED)
        self.assertEqual(events[1].payload["position_size"], 0.05)

    async def test_sync_exchange_future_position_closed(self) -> None:
        signal_id = "sig-future-closed"
        await self.store.get_or_create_signal(signal_id)
        await self.store.update_signal_state(
            signal_id,
            signal_state="OPEN",
            order_state="FILLED",
            position_state="OPENED",
            policies={"market_type": "FUTURE"},
            order_id="ord-future-closed",
            order_symbol="BTC/USDT"
        )
        state = await self.store.get_signal_state(signal_id)

        # Mock the exchange
        mock_exchange = MagicMock()
        mock_exchange.fetch_order.return_value = {
            "id": "ord-future-closed",
            "status": "closed",
            "symbol": "BTC/USDT",
            "price": 60000.0,
            "average": 60000.0
        }
        mock_exchange.fetch_positions.return_value = [
            {
                "symbol": "BTC/USDT",
                "contracts": 0.0,
                "size": 0.0
            }
        ]
        self.executor._exchanges["FUTURE"] = mock_exchange
        self.executor._exchange_injected = True

        events = await self.executor.sync_exchange(signal_id, self.store)

        # Since position_state was OPENED, but size is 0.0:
        # We expect POSITION_CLOSED event
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, ExecutionEventType.POSITION_CLOSED)

    async def test_sync_exchange_skips_rejected_signal_without_symbol(self) -> None:
        signal_id = "sig-rejected"
        await self.store.get_or_create_signal(signal_id)
        await self.store.update_signal_state(
            signal_id,
            signal_state="REJECTED",
            order_state="NONE",
            position_state="NONE",
        )

        events = await self.executor.sync_exchange(signal_id, self.store)

        self.assertEqual(events, [])

    async def test_get_active_signals_excludes_rejected_signal(self) -> None:
        await self.store.get_or_create_signal("sig-open")
        await self.store.update_signal_state("sig-open", signal_state="OPEN", position_state="OPENED")
        await self.store.get_or_create_signal("sig-rejected")
        await self.store.update_signal_state("sig-rejected", signal_state="REJECTED", position_state="NONE")

        active = await self.store.get_active_signals()

        self.assertEqual(active, ["sig-open"])

    async def test_get_active_signals_excludes_accepted_signal_without_exchange_context(self) -> None:
        await self.store.get_or_create_signal("sig-accepted-empty")
        await self.store.update_signal_state(
            "sig-accepted-empty",
            signal_state="ACCEPTED",
            order_state="NONE",
            position_state="NONE",
        )
        await self.store.get_or_create_signal("sig-open")
        await self.store.update_signal_state(
            "sig-open",
            signal_state="ACCEPTED",
            order_state="FILLED",
            position_state="OPENED",
            order_symbol="BTC/USDT",
        )

        active = await self.store.get_active_signals()

        self.assertEqual(active, ["sig-open"])
