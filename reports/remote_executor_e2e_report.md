# Remote Executor E2E Report

Date: 2026-07-04

## Scope

Validated the `local-executor-client` against the remote Marcus backend with Binance sandbox credentials loaded from the local `.env` file. The run used the current wired executor path: provisioning, websocket handshake, signed signal delivery, delivery ACK, sandbox execution attempt, and local persistence.

## Environment

- HTTP API: `https://marcus-api.tromoi.xyz`
- WebSocket: `ws://171.244.195.150:8081/ws/executor`
- Executor mode: `live`
- Exchange: `binance`
- Sandbox: `true`
- Default order amount: `0.001`

## Command

```powershell
python scripts\remote_executor_e2e.py --env-file .env
```

## Results

| Scenario | Status | Evidence |
| --- | --- | --- |
| Provisioning smoke | Pass | State file reused successfully and contained `bot_id`, `bot_api_key`, `bot_signer_secret`, and `ws_token`. |
| WebSocket handshake | Pass | Backend accepted `/ws/executor` and returned an ACK with `status=ok` and `ack_type=handshake`. |
| Signal delivery | Pass | Signed `POST /api/v1/signals` returned `200`. Executor logged delivery ACK for the signal. |
| Local execution state | Pass | Executor connected, recovered cleanly from an empty local DB, and persisted a signal row checked by the runner. |
| Sandbox exchange contact | Pass | Executor reached Binance demo endpoints through CCXT and completed the exchange-info request path. |
| Idempotency | Pass | Resending the same `signalId` returned `409`. |
| Invalid payload rejection | Pass | Invalid signal payload returned `422`. |
| Balance audit push | Observed | Executor started the balance sync loop and remained connected; the runner does not hard-fail on missing audit-log text. |
| Restart / reconnect | Not exercised in this run | Not part of the current automated runner path. |

## Notes

- The remote HTTP API accepted requests at `https://marcus-api.tromoi.xyz`; the literal IP URL did not behave as a usable API base in this environment.
- The runner removes the local executor SQLite DB before each execution so stale recovery state does not interfere with the test.
- The known `execution_event` transport gap remains outside this run.

## Cleanup Performed

- Unsubscribed the previous `demo-trader` subscription for the bot.
- Rebound the active subscription to `trader+e2e@example.com`.
- Deleted the latest local executor signal record from `execution_signals` and cleared related rows in `execution_events`, `execution_acks`, and `recovery_state`.
- Removed the newest portfolio equity snapshot for `usr_253511f8f4144c3fa4cb278872870654` and rolled the live aggregate/account back to the prior equity value.
- Removed residual `4999` equity points from `demo-trader` in `portfolio_aggregate_history`, `portfolio_balance_history`, `portfolio_accounts`, and rolled `user_portfolios` back to `89.43588492`.
