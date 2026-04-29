# Marcus Local Executor Client

This repository contains only the trader-side local executor runtime.

## Responsibilities
- Maintain a resilient WebSocket connection to the dispatcher.
- Perform subscribe handshake and ack validation.
- Auto-reconnect with exponential backoff.
- Monitor protocol heartbeat with ping/pong fallback.
- Execute incoming signals via the configured exchange account.

## Runtime Contract
- Envelope types:
  - signal
  - heartbeat
  - ack
  - system
- Unsupported or malformed frames are ignored and counted.

## Signal Payload Contract

The executor processes signal payloads with the following schema:

### Required Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `signal_id` | string | Unique signal identifier | `"sig-20260407-001"` |
| `action` | string | Trading action | `"OPEN_LONG"` |
| `symbol` | string | Trading pair (base or base/quote format) | `"BTCUSDT"` or `"BTC/USDT"` |

### Optional Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `amount` / `quantity` / `size` | float | Order amount (coins to trade) | `0.5` |
| `order_type` | string | Order type: `"market"` or `"limit"` | `"market"` |
| `price` / `limit_price` / `limitPrice` | float | Limit order price (required if `order_type="limit"`) | `65000.50` |
| `time_in_force` | string | Time in force: `"GTC"`, `"IOC"`, `"FOK"`, etc. | `"GTC"` |
| `asset_pair` / `assetPair` | string | Alternative symbol field | `"ETHUSDT"` |
| `metadata` | object | Additional context | `{"strategy": "sma"}` |

### Valid Actions

- `OPEN_LONG` - Open long position (buy)
- `CLOSE_LONG` - Close long position (sell, reduce_only=true)
- `OPEN_SHORT` - Open short position (sell)
- `CLOSE_SHORT` - Close short position (buy, reduce_only=true)
- `BUY` - Alias for OPEN_LONG
- `SELL` - Alias for CLOSE_LONG
- `SELL_SHORT` - Alias for OPEN_SHORT
- `BUY_TO_COVER` - Alias for CLOSE_SHORT

### Symbol Normalization

The executor normalizes symbols to CCXT format (`BASE/QUOTE`):

```
BTCUSDT     → BTC/USDT
ethusdt     → ETH/USDT
BTC/USDT    → BTC/USDT (unchanged)
eth/usdt    → ETH/USDT
```

Supported quote currencies: USDT, USDC, BUSD

### Examples

**Minimal signal (market order):**
```json
{
  "signal_id": "sig-001",
  "action": "OPEN_LONG",
  "symbol": "BTCUSDT"
}
```

**Market order with amount:**
```json
{
  "signal_id": "sig-002",
  "action": "OPEN_LONG",
  "symbol": "BTCUSDT",
  "amount": 0.5
}
```

**Limit order:**
```json
{
  "signal_id": "sig-003",
  "action": "OPEN_LONG",
  "symbol": "BTCUSDT",
  "order_type": "limit",
  "price": 65000.50,
  "amount": 0.5,
  "time_in_force": "GTC"
}
```

**Close position:**
```json
{
  "signal_id": "sig-004",
  "action": "CLOSE_LONG",
  "symbol": "BTCUSDT",
  "amount": 0.5
}
```

### Validation Rules

1. **Required fields must be present** and non-empty
2. **Action must be one of the valid actions** (case-insensitive, normalized to uppercase)
3. **Symbol must resolve to a valid trading pair** (min 2 chars)
4. **Amount (if provided) must be > 0**
5. **Limit orders require price field** with numeric value
6. **Price (if provided) must be numeric and > 0**
7. **Order type must be "market" or "limit"** (case-insensitive)

### Error Responses

When validation fails, ExecutionResult contains:
- `mode`: `"error"`
- `errors`: List of validation error messages
- `details`: Original signal and attempted order

Example error response:
```python
ExecutionResult(
    mode="error",
    order_id=None,
    errors=["Invalid action: INVALID_ACTION. Must be one of ..."],
    details={"signal": {...}}
)
```

## Requirements
- Python 3.10+

## Setup
1. Create `.env` from `.env.example` and fill values.
2. Install:

```bash
pip install -e .
```

## Run
```bash
python local_executor.py
```

or

```bash
local-executor
```

## Test
```bash
python -m unittest discover -s tests -v
```

## Execution Mode
Set `EXECUTION_MODE` to `dry-run` for safe logging, or `live` to place orders via CCXT.

## Contract Fixtures
- WebSocket contract fixtures live in `tests/fixtures/contracts`.
- Compatibility checks live in `tests/test_contract_compatibility.py`.
- Signal validation tests live in `tests/test_engine.py` (SignalSchemaValidationTest and CcxtSignalExecutorBuildOrderTest)
