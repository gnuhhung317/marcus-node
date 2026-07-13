from __future__ import annotations

import importlib.util
import sys
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "append_binance_replay.py"


@pytest.fixture(scope="module")
def append_script():
    spec = importlib.util.spec_from_file_location("append_binance_replay", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _trade(entry_offset_hours: int = 0, exit_offset_hours: int = 1) -> dict[str, object]:
    entry_ts = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc) + timedelta(hours=entry_offset_hours)
    exit_ts = datetime(2026, 7, 1, 13, 0, tzinfo=timezone.utc) + timedelta(hours=exit_offset_hours)
    return {
        "symbol": "BTC/USDT",
        "side": "LONG",
        "quantity": 0.1,
        "entry_price": 100.0,
        "exit_price": 110.0,
        "pnl": 9.75,
        "fees": 0.25,
        "entry_timestamp": entry_ts,
        "exit_timestamp": exit_ts,
        "trade_id": f"trade-{entry_offset_hours}-{exit_offset_hours}",
    }


def test_build_trade_seed_records_is_deterministic(append_script):
    now = datetime(2026, 7, 2, 8, 30, tzinfo=timezone.utc)
    trade = _trade()

    first = append_script.build_trade_seed_records("bot-1", trade, now)
    second = append_script.build_trade_seed_records("bot-1", trade, now)

    assert first["entry_signal"]["signal_id"] == second["entry_signal"]["signal_id"]
    assert first["exit_signal"]["signal_id"] == second["exit_signal"]["signal_id"]
    assert first["entry_signal"]["signal_id"] != first["exit_signal"]["signal_id"]

    event_ids = [event["event_id"] for event in first["events"]]
    assert len(event_ids) == len(set(event_ids))
    assert [event["sequence"] for event in first["events"]] == [0, 1, 2, 3, 0, 1, 2, 3]
    assert first["entry_signal"]["action"] == "OPEN_LONG"
    assert first["exit_signal"]["action"] == "CLOSE_LONG"
    assert first["events"][0]["payload"]["status"] == "accepted"
    assert first["events"][3]["event_type"] == "POSITION_OPENED"
    assert first["events"][7]["event_type"] == "POSITION_CLOSED"


def test_build_append_points_are_monotonic_and_preserve_realized_pnl(append_script):
    anchor = append_script.AnchorPoint(
        timestamp=datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
        cash=100.0,
        equity=100.0,
        realized_pnl=5.0,
        unrealized_pnl=0.0,
        total_fees=1.0,
    )
    trades = [
        {
            "exit_timestamp": datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
            "pnl": 10.0,
            "fees": 0.5,
        },
        {
            "exit_timestamp": datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
            "pnl": -2.0,
            "fees": 0.25,
        },
    ]
    live_balance = {
        "timestamp": datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
        "total": 120.0,
        "free": 118.0,
        "unrealizedPnl": 2.0,
    }

    points = append_script.build_append_points(anchor, trades, live_balance)

    assert len(points) == 3
    assert points[0]["timestamp"] > anchor.timestamp
    assert points[1]["timestamp"] > points[0]["timestamp"]
    assert points[2]["timestamp"] > points[1]["timestamp"]
    assert points[0]["realized_pnl"] == pytest.approx(15.0)
    assert points[1]["realized_pnl"] == pytest.approx(13.0)
    assert points[2]["realized_pnl"] == pytest.approx(13.0)
    assert points[0]["total_fees"] == pytest.approx(1.5)
    assert points[1]["total_fees"] == pytest.approx(1.75)
    assert points[2]["total_fees"] == pytest.approx(1.75)
    assert points[2]["cash"] == pytest.approx(118.0)
    assert points[2]["equity"] == pytest.approx(120.0)


def test_build_dashboard_history_points_preserve_anchor_continuity(append_script):
    anchor = append_script.AnchorPoint(
        timestamp=datetime(2026, 7, 1, 9, 59, 59, tzinfo=timezone.utc),
        cash=100.0,
        equity=100.0,
        realized_pnl=5.0,
        unrealized_pnl=0.0,
        total_fees=2.0,
    )
    dashboard_points = [
        {
            "timestamp": datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
            "cash": 105.0,
            "equity": 106.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 1.0,
            "total_fees": 0.0,
        },
        {
            "timestamp": datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc),
            "cash": 110.0,
            "equity": 111.0,
            "realized_pnl": 5.0,
            "unrealized_pnl": 1.0,
            "total_fees": 0.0,
        },
    ]
    closed_trades = [
        {"exit_timestamp": datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc), "fees": 0.5},
        {"exit_timestamp": datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc), "fees": 0.25},
    ]

    points = append_script.build_dashboard_history_points(anchor, dashboard_points, closed_trades)

    assert len(points) == 2
    assert points[0]["realized_pnl"] == pytest.approx(5.0)
    assert points[1]["realized_pnl"] == pytest.approx(10.0)
    assert points[0]["total_fees"] == pytest.approx(2.5)
    assert points[1]["total_fees"] == pytest.approx(2.75)
    assert points[1]["equity"] == pytest.approx(111.0)


def test_match_trades_with_state_closes_previous_open_lot(append_script):
    open_lot = {
        "side": "buy",
        "price": 100.0,
        "amount": 1.0,
        "fee": 0.1,
        "timestamp": 1_000,
        "dt": datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
        "trade_id": "entry-1",
        "remaining_amount": 1.0,
    }
    exit_trade = {
        "id": "exit-1",
        "timestamp": int(datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc).timestamp() * 1000),
        "symbol": "BTC/USDT",
        "side": "sell",
        "price": 110.0,
        "amount": 1.0,
        "fee": {"cost": 0.2},
    }

    closed_trades, queues, processed = append_script.match_trades_with_state(
        [exit_trade],
        initial_queues={"BTC/USDT": deque([open_lot])},
        recent_trade_ids={},
    )

    assert len(processed) == 1
    assert len(closed_trades) == 1
    assert closed_trades[0]["trade_id"] == "entry-1_exit-1"
    assert queues["BTC/USDT"] == deque()


def test_compute_since_days_uses_overlap_and_bootstrap(append_script):
    now = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
    assert append_script.compute_since_days(None, bootstrap_days=30, overlap_days=7, now=now) == 30
    last_exit = now - timedelta(days=2)
    assert append_script.compute_since_days(last_exit, bootstrap_days=30, overlap_days=7, now=now) == 9


def test_all_trading_pair_triggers_auto_discovery(append_script):
    class _Cursor:
        def __init__(self, pair):
            self.pair = pair
            self.query = None

        def execute(self, query, params):
            self.query = query

        def fetchone(self):
            return (self.pair,)

        def close(self):
            pass

    class _Conn:
        def __init__(self, pair):
            self.pair = pair

        def cursor(self):
            return _Cursor(self.pair)

    symbols, auto_discover = append_script.resolve_symbols(_Conn("ALL/USDT"), "bot-1", None)
    assert symbols == []
    assert auto_discover is True

    symbols, auto_discover = append_script.resolve_symbols(_Conn("BTC/USDT"), "bot-1", None)
    assert symbols == ["BTC/USDT"]
    assert auto_discover is False


def test_filter_new_closed_trades_skips_existing_ids(append_script):
    trade_old = _trade()
    trade_new = _trade(entry_offset_hours=2, exit_offset_hours=3)

    result = append_script.filter_new_closed_trades(
        [trade_old, trade_new],
        existing_trade_ids={trade_old["trade_id"]},
        watermark=datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc),
        overlap_days=7,
    )

    assert [trade["trade_id"] for trade in result] == [trade_new["trade_id"]]


def test_resolve_fetch_checkpoint_prefers_latest_source(append_script):
    db_checkpoint = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    state_checkpoint = datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc)

    assert append_script.resolve_fetch_checkpoint(db_checkpoint, state_checkpoint) == state_checkpoint
    assert append_script.resolve_fetch_checkpoint(state_checkpoint, db_checkpoint) == state_checkpoint


def test_resolve_fetch_checkpoint_accepts_naive_database_timestamp(append_script):
    db_checkpoint = datetime(2026, 7, 1, 12, 0)
    state_checkpoint = datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc)

    result = append_script.resolve_fetch_checkpoint(db_checkpoint, state_checkpoint)

    assert result == state_checkpoint
    assert result.tzinfo == timezone.utc


def test_match_trades_preserves_binance_source_symbol(append_script):
    trades = [
        {
            "id": "entry-1",
            "timestamp": int(datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc).timestamp() * 1000),
            "symbol": "BTC/USDT",
            "info": {"symbol": "ETHUSDT"},
            "side": "buy",
            "price": 100.0,
            "amount": 1.0,
            "fee": {"cost": 0.1},
        },
        {
            "id": "exit-1",
            "timestamp": int(datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc).timestamp() * 1000),
            "symbol": "BTC/USDT",
            "info": {"symbol": "ETHUSDT"},
            "side": "sell",
            "price": 110.0,
            "amount": 1.0,
            "fee": {"cost": 0.2},
        },
    ]

    closed_trades, _, _ = append_script.match_trades_with_state(trades)

    assert closed_trades[0]["symbol"] == "ETHUSDT"
    seed_rows = append_script.build_trade_seed_records("bot-1", closed_trades[0], datetime.now(timezone.utc))
    assert seed_rows["entry_signal"]["symbol"] == "ETHUSDT"
    assert seed_rows["exit_signal"]["symbol"] == "ETHUSDT"


def test_replay_state_round_trip(append_script, tmp_path):
    state_file = tmp_path / "state.json"
    scope_key = append_script.state_scope_key("binance", "bot-1", "user-1")

    payload = {
        "last_fetch_at": "2026-07-02T08:30:00+00:00",
        "last_trade_seen_at": "2026-07-02T08:00:00+00:00",
        "open_positions": {"BTC/USDT": []},
        "recent_trade_ids": {"BTC/USDT": ["t1", "t2"]},
    }

    assert append_script.load_scoped_replay_state(state_file, scope_key) == {}
    append_script.save_scoped_replay_state(state_file, scope_key, payload)
    loaded = append_script.load_scoped_replay_state(state_file, scope_key)
    assert loaded["last_fetch_at"] == payload["last_fetch_at"]
    assert loaded["recent_trade_ids"]["BTC/USDT"] == ["t1", "t2"]
    assert append_script.load_scoped_replay_state(state_file, "binance::bot-2::user-2") == {}


def test_calculate_metrics_ignores_nonpositive_dashboard_outliers(append_script):
    points = [
        {"timestamp": datetime(2026, 7, 1, tzinfo=timezone.utc), "equity": 100.0},
        {"timestamp": datetime(2026, 7, 2, tzinfo=timezone.utc), "equity": 0.0},
        {"timestamp": datetime(2026, 7, 3, tzinfo=timezone.utc), "equity": 95.0},
    ]

    _, max_drawdown, _, sample_days = append_script.seed.calculate_metrics(points)

    assert max_drawdown == pytest.approx(-0.05)
    assert sample_days == 3
