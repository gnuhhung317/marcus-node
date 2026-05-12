import pytest
import time
import asyncio
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import MagicMock

from local_executor.local_store import LocalExecutionStore, SignalState
from local_executor.recovery_manager import ExecutionRecoveryManager, RecoveryPhase
from local_executor.execution import CcxtSignalExecutor
from local_executor.config import ExecutorConfig

@pytest.mark.asyncio
async def test_recovery_discovery_ignores_closed_signals():
    """
    SIMULATED CHAOS: Reboot executor and ensure auto-discovery only picks up 
    dangling (open) signals and ignores fully completed (closed) ones.
    """
    # Setup clean DB
    store = LocalExecutionStore(":memory:")
    await store.initialize()
    
    # Signal 1: Completed and closed (SHOULD BE IGNORED)
    await store.get_or_create_signal("sig_old_done")
    await store.update_signal_state(
        "sig_old_done", 
        signal_state="CLOSED", 
        order_state="FILLED", 
        position_state="CLOSED"
    )
    
    # Signal 2: Half-baked, dangling due to process crash (SHOULD BE RECOVERED)
    await store.get_or_create_signal("sig_crashed_midway")
    await store.update_signal_state(
        "sig_crashed_midway", 
        signal_state="OPEN", 
        order_state="PLACED", 
        position_state="OPENED"
    )
    
    # Mock out state engine
    mock_engine = MagicMock()
    
    # Instantiate Recovery Manager (simulating Reboot)
    mgr = ExecutionRecoveryManager(store, mock_engine)
    
    # Act: Run bootstrap phase
    discovered = await mgr._bootstrap_signals([])
    
    # Assert: Discovery strictly filters active boundaries
    assert "sig_crashed_midway" in discovered
    assert "sig_old_done" not in discovered
    assert len(discovered) == 1


@pytest.mark.asyncio
async def test_clean_boundary_expiry_enforcement():
    """
    Verify the clean boundary deadline enforcer proactively drops expired signals.
    """
    config = ExecutorConfig(
        bot_id="test-bot",
        ws_url="ws://localhost",
        ws_token="token",
        execution_mode="dry-run",
        exchange_id="binance",
        exchange_api_key="dummy",
        exchange_api_secret="dummy"
    )
    executor = CcxtSignalExecutor(config)
    
    # Payload A: Legitimate & fresh
    fresh_payload = {
        "signal_id": "fresh_sig",
        "symbol": "BTC/USDT",
        "action": "BUY",
        "amount": 1.0,
        "cancel_after_timestamp": time.time() + 600 # expires in 10 min
    }
    result_fresh = await executor.execute_signal(fresh_payload)
    assert result_fresh.mode == "dry-run"
    assert result_fresh.errors is None

    # Payload B: Rotted / Delayed arrival (Expires 10s AGO)
    stale_payload = {
        "signal_id": "stale_sig",
        "symbol": "BTC/USDT",
        "action": "BUY",
        "amount": 1.0,
        "cancel_after_timestamp": time.time() - 10 
    }
    result_stale = await executor.execute_signal(stale_payload)
    assert result_stale.mode == "error"
    assert "EXPIRED_ON_ARRIVAL" in result_stale.details["reason"]
    assert len(result_stale.errors) > 0
