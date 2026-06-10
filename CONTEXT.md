# CONTEXT — local-executor-client (L1 Service)

> **Parent**: [CONTEXT_MAP.md](../CONTEXT_MAP.md) | **Changelog**: [CONTEXT_CHANGELOG.md](CONTEXT_CHANGELOG.md)
> **Role**: "The Arms" — Trader Runtime: Blind execution engine running on trader's own machine

---

## Service Identity

| Property | Value |
|----------|-------|
| Stack | Python 3.11+, asyncio, CCXT, SQLite, WebSocket |
| Package | `local_executor` |
| Install | `pip install -e .` |
| Test | `python -m pytest tests/ -v` |
| Scope | Trader-side runtime (NOT developer SDK) |

### ⚠️ Scope Clarification
- `local_executor` = **trader runtime** (this service) — blind execution engine
- `quant_signal_sdk` = **developer-side SDK** (separate: `bot-framework-python/`)
- This service receives **Execution Instructions** (NOT raw signals)

---

## Architecture Philosophy

**"Blind Execution"**: The executor does NOT know what RSI, SMA, or candle patterns are. It only works with:
- Real-time price (Tick)
- Absolute time (Unix Epoch)
- Execution Instructions from backend

This ensures the executor is lightweight, secure, and doesn't need market data feeds.

---

## Module Structure

```
local-executor-client/
├── src/local_executor/
│   ├── execution.py          # Core execution engine
│   ├── local_store.py        # SQLite local journal (position state)
│   └── ...
├── tests/
│   ├── test_execution_events.py
│   └── ...
├── scripts/                  # Utility scripts
├── local_executor.py         # Entry point / CLI
├── pyproject.toml            # Package metadata
├── ARCHITECTURE.md           # Detailed architecture doc
├── executor_state.db         # SQLite state database (runtime artifact)
└── Dockerfile.executor       # Docker image for executor
    Dockerfile.provisioner    # Docker image for provisioner
```

---

## Key Concepts

### Execution Instruction (Input)
Received from `signal-core-backend` via WebSocket:
```json
{
  "signal_id": "uuid",
  "symbol": "BTC/USDT",
  "timeframe": "1h",
  "action": "OPEN_LONG",
  "params": {
    "order_type": "LIMIT",
    "limit_price": 44850.0,
    "quantity": 0.01,
    "cancel_after_timestamp": 1684323400
  },
  "protection": {
    "sl_pct": 0.02,
    "tp_pct": 0.06,
    "trailing_callback": 0.01
  }
}
```

### Position Lifecycle (State Machine)
```
RECEIVED → VALIDATING → SUBMITTED → PARTIAL_FILL → FILLED
    → MONITORING → CLOSING → CLOSED
    → CANCELLED (if timeout)
    → ERROR (if exchange failure)
```

### Bar-to-Timestamp Translation
- Bot/Backend decides: "Limit order valid for 2 candles (1H timeframe)"
- Backend translates: `cancel_after_timestamp = current_time + (2 * 3600)`
- Executor only compares: `current_time > cancel_after_timestamp` → cancel
- This keeps executor free of "candle" logic leakage

### ATR Offset (Entry Lùi)
- Final limit price calculated at Executor using real-time Ticker
- Formula parameters (`multiplier`, `offset`) come from Bot/Backend
- Ensures precision without executor needing market data analysis

---

## Infrastructure

- **Exchange**: Via CCXT (Binance, etc.)
- **State**: SQLite local journal (position tracking, event log)
- **Communication**: WebSocket to signal-core-backend
- **Deploy**: Docker or direct Python on trader's machine

---

## Important Gotchas

1. **State database**: `executor_state.db` is a runtime artifact — do not commit changes to it
2. **Event sync**: See `docs/architecture/ADR-001-executor-event-sync.md` for event synchronization design
3. **Recovery**: See `docs/architecture/executor-recovery-contract.md` for crash recovery behavior
4. **Event schema**: See `docs/architecture/executor-event-schema.md` for full event types
5. **State machine**: See `docs/architecture/executor-event-state-machine.md` for state transitions

---

## How to Run

```bash
# Install
pip install -e .

# Run executor
python local_executor.py

# Tests
python -m pytest tests/ -v
```

---

## Docker

```bash
# Build executor image
docker build -f Dockerfile.executor -t marcus-executor .

# Build provisioner image
docker build -f Dockerfile.provisioner -t marcus-provisioner .
```

---

> **Update Trigger**: When changing execution logic, state machine, event schema, or exchange integration → update this CONTEXT.md and append to CONTEXT_CHANGELOG.md