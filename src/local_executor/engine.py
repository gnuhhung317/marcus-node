from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .config import ExecutorConfig
from .execution import CcxtSignalExecutor
from .local_store import LocalExecutionStore
from .ws_client import ResilientWebSocketClient
from .recovery_manager import ExecutionRecoveryManager
from .execution_state_engine import ExecutionStateEngine
from .execution_event_transport import ExecutionEvent
from .notifications import TelegramNotifier, build_executor_alert

SignalHandler = Callable[[dict[str, Any]], Awaitable[None]]


class LocalExecutorEngine:
    """
    Core execution loop for the local executor.

    Responsibilities:
    - Maintain a resilient WebSocket connection to the Marcus backend.
    - Receive signal payloads and execute them via CcxtSignalExecutor.
    - Deduplicate signals using the SQLite-backed LocalExecutionStore.
    - Periodically emit account balance snapshots for the dashboard.
    """

    def __init__(
        self,
        config: ExecutorConfig,
        on_signal: SignalHandler | None = None,
        local_store: LocalExecutionStore | None = None,
    ) -> None:
        self._config = config
        self._logger = logging.getLogger(__name__)
        self._executor = CcxtSignalExecutor(config=config, logger=self._logger)
        self._notifier = TelegramNotifier(
            bot_token=config.telegram_bot_token,
            chat_id=config.telegram_chat_id,
            logger=self._logger,
        )
        self._local_store = local_store
        # Created once local_store is initialized in `run()`
        self._state_engine: ExecutionStateEngine | None = None
        self._recovery_manager: ExecutionRecoveryManager | None = None
        # Futures waiting for replay responses keyed by signal_id
        self._replay_waiters: dict[str, asyncio.Future] = {}
        self._on_signal = on_signal or self._default_signal_handler
        self._ws_client = ResilientWebSocketClient(
            ws_url=config.ws_url,
            ws_token=config.ws_token,
            heartbeat_interval_seconds=config.heartbeat_interval_seconds,
            heartbeat_timeout_seconds=config.heartbeat_timeout_seconds,
            reconnect_initial_delay_seconds=config.reconnect_initial_delay_seconds,
            reconnect_max_delay_seconds=config.reconnect_max_delay_seconds,
            on_signal=self._handle_signal,
            on_resync=self._on_resync,
            on_replay_response=self._handle_replay_response,
            on_connection_loss=self._notify_connection_loss,
            bot_id=config.bot_id,
            protocol_version=config.protocol_version,
            handshake_ack_required=config.handshake_ack_required,
            handshake_ack_timeout_seconds=config.handshake_ack_timeout_seconds,
            heartbeat_stale_seconds=config.heartbeat_stale_seconds,
            logger=self._logger,
        )

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()

        # Initialise the local store if provided but not yet open
        if self._local_store is not None:
            await self._local_store.initialize()

        # Instantiate state engine and recovery manager once store is ready
        if self._local_store is not None and self._state_engine is None:
            self._state_engine = ExecutionStateEngine(self._local_store, logger=self._logger)
            self._recovery_manager = ExecutionRecoveryManager(
                store=self._local_store, state_engine=self._state_engine, logger=self._logger
            )

        self._logger.info("Starting LocalExecutorEngine.")
        tasks = [
            asyncio.create_task(self._ws_client.run(stop_event=stop_event), name="ws_client_run"),
            asyncio.create_task(self._balance_sync_loop(stop_event), name="balance_sync_loop"),
            asyncio.create_task(self._deadline_sweeper_loop(stop_event), name="deadline_sweeper_loop"),
        ]

        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if not t.cancelled() and t.exception():
                    exc = t.exception()
                    self._logger.error(
                        "Task fatal error: %s repr=%s",
                        exc.__class__.__name__,
                        repr(exc),
                    )
                    self._logger.debug("Full traceback:", exc_info=exc)
                    await self._notify(
                        "Fatal executor task error",
                        task=t.get_name(),
                        error=f"{exc.__class__.__name__}: {exc}",
                    )
                    raise exc
        finally:
            self._logger.info("Shutting down engine tasks.")
            stop_event.set()
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            if self._local_store is not None:
                await self._local_store.close()

    # ── Signal handling ────────────────────────────────────────────────────

    async def _handle_signal(self, payload: dict[str, Any]) -> None:
        await self._on_signal(payload)

    async def _default_signal_handler(self, payload: dict[str, Any]) -> None:
        """
        Default signal handler: dedup check → execute → update local state.

        Dedup logic uses the execution_signals table in LocalExecutionStore.
        A signal is considered already-processed if its local state is OPEN or CLOSED.
        Signals in ACCEPTED state (created but not yet executed) are re-attempted
        — this can happen if the process crashed after WS delivery but before execution.
        """
        signal_id = payload.get("signal_id")

        # --- Dedup check (SQLite, idempotent on reconnect) ---
        if signal_id and self._local_store is not None:
            existing = await self._local_store.get_signal_state(signal_id)
            if existing and existing.signal_state in {"OPEN", "CLOSED"}:
                self._logger.info(
                    "Skipping duplicate signal signal_id=%s state=%s",
                    signal_id,
                    existing.signal_state,
                )
                return

        # --- Execute ---
        result = await self._executor.execute_signal(payload)

        action = payload.get("action", "unknown")
        symbol = payload.get("symbol", "unknown")
        self._logger.info(
            "Signal executed signal_id=%s action=%s symbol=%s mode=%s order_id=%s",
            signal_id or "unknown",
            action,
            symbol,
            result.mode,
            result.order_id or "n/a",
        )

        if result.errors:
            self._logger.warning(
                "Signal execution errors signal_id=%s errors=%s",
                signal_id or "unknown",
                result.errors,
            )
            await self._notify(
                "Execution error",
                bot_id=self._config.bot_id,
                signal_id=signal_id or "unknown",
                action=action,
                symbol=symbol,
                mode=result.mode,
                errors="; ".join(str(error) for error in result.errors),
            )

        # --- Update local state ---
        if signal_id and self._local_store is not None:
            if result.mode != "error":
                # Mark as OPEN — downstream execution events will close it
                await self._local_store.get_or_create_signal(signal_id)
                # Persist order id / symbol when present
                order_symbol = None
                try:
                    built = self._executor._build_order(payload)
                    order_symbol = built.get("symbol")
                except Exception:
                    order_symbol = None

                await self._local_store.update_signal_state(
                    signal_id,
                    signal_state="OPEN",
                    policies=payload.get("policies"),
                    order_id=result.order_id,
                    order_symbol=order_symbol,
                )
            else:
                # Record the signal but mark as REJECTED so dedup still protects
                await self._local_store.get_or_create_signal(signal_id)
                await self._local_store.update_signal_state(signal_id, signal_state="REJECTED", policies=payload.get("policies"))

    async def _deadline_sweeper_loop(self, stop_event: asyncio.Event) -> None:
        """Periodic sweeper that enforces cancel/close deadlines recorded in signal policies."""
        interval = 5.0
        self._logger.info("Deadline sweeper loop started interval=%.1fs", interval)

        # Wait briefly before starting
        await asyncio.sleep(1.0)

        while not stop_event.is_set():
            try:
                if self._local_store is None:
                    await asyncio.sleep(interval)
                    continue

                active = await self._local_store.get_active_signals()
                now_epoch = int(__import__("time").time())
                for sid in active:
                    state = await self._local_store.get_signal_state(sid)
                    if not state or not state.policies:
                        continue
                    policies = state.policies
                    # Support both camelCase and snake_case keys
                    cancel_ts = policies.get("cancelOrderAfter") or policies.get("cancel_order_after")
                    close_ts = policies.get("closePositionAfter") or policies.get("close_position_after")

                    try:
                        if cancel_ts is not None and int(cancel_ts) > 0 and now_epoch > int(cancel_ts):
                            # Cancel outstanding orders: attempt exchange-level cancel then append sweeper event
                            self._logger.info("Sweeper cancelling signal_id=%s at %d", sid, now_epoch)
                            try:
                                if state.order_id:
                                    await self._executor.cancel_order(state.order_id, state.order_symbol)
                            except Exception as e:
                                self._logger.warning("Executor cancel_order failed for signal_id=%s error=%s", sid, e)
                            await self._local_store.append_sweeper_event(sid, "CANCEL", "deadline_expired")
                            await self._local_store.update_signal_state(sid, order_state="CANCELED")
                            await self._notify(
                                "Emergency order cancel sweep",
                                bot_id=self._config.bot_id,
                                signal_id=sid,
                                order_id=state.order_id,
                                symbol=state.order_symbol,
                                reason="deadline_expired",
                            )

                        if close_ts is not None and int(close_ts) > 0 and now_epoch > int(close_ts):
                            # Force-close position locally and append sweeper event
                            # Only act if position is not already CLOSED
                            if state.position_state != "CLOSED":
                                self._logger.info("Sweeper forcing close for signal_id=%s at %d", sid, now_epoch)
                                try:
                                    if state.order_symbol:
                                        await self._executor.force_close_position(state.order_symbol)
                                except Exception as e:
                                    self._logger.warning("Executor force_close_position failed for signal_id=%s error=%s", sid, e)
                                await self._local_store.append_sweeper_event(sid, "FORCED_CLOSE", "deadline_expired")
                                await self._local_store.update_signal_state(
                                    sid,
                                    position_state="CLOSED",
                                    order_state="CANCELED",
                                    closed_at=__import__("datetime").datetime.utcnow(),
                                )
                                await self._notify(
                                    "Emergency forced-close sweep",
                                    bot_id=self._config.bot_id,
                                    signal_id=sid,
                                    symbol=state.order_symbol,
                                    reason="deadline_expired",
                                )
                    except Exception as e:
                        self._logger.warning("Sweeper failed for signal_id=%s error=%s", sid, e)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._logger.warning("Deadline sweeper encountered error: %s", exc)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                continue

    async def _on_resync(self, reason: str) -> None:
        self._logger.info("Resync triggered reason=%s", reason)

        if self._local_store is None or self._recovery_manager is None:
            self._logger.debug("Skipping recovery - local store or recovery manager not configured.")
            return

        # Define fetch_history_func that requests replay via websocket and waits for a response
        async def fetch_history_func(signal_id: str, from_sequence: int = 0) -> list[ExecutionEvent]:
            # Prepare a future and send replay-request frame
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self._replay_waiters[signal_id] = fut

            frame = {
                "type": "replay-request",
                "botId": self._config.bot_id,
                "payload": {"signalId": signal_id, "fromSequence": from_sequence},
            }
            try:
                await self._ws_client.send_frame(frame)
            except Exception as e:
                self._logger.warning("Failed to send replay request for %s: %s", signal_id, e)
                self._replay_waiters.pop(signal_id, None)
                return []

            try:
                events = await asyncio.wait_for(fut, timeout=10.0)
                return events or []
            except asyncio.TimeoutError:
                self._logger.warning("Timed out waiting for replay response for signal_id=%s", signal_id)
                self._replay_waiters.pop(signal_id, None)
                return []

        # CCXT-based exchange sync to reconcile local state with exchange state
        async def sync_exchange_func(signal_id: str) -> list[ExecutionEvent]:
            return await self._executor.sync_exchange(signal_id, self._local_store)

        try:
            status = await self._recovery_manager.recover(fetch_history_func=fetch_history_func, sync_exchange_func=sync_exchange_func)
            self._logger.info("Recovery status: phase=%s errors=%s", status.phase.value, status.errors)
        except Exception as e:
            self._logger.error("Recovery run failed: %s", e, exc_info=True)

    async def _notify_connection_loss(self, reason: str) -> None:
        await self._notify(
            "WebSocket connection lost",
            bot_id=self._config.bot_id,
            reason=reason,
        )

    async def _notify(self, title: str, **fields: Any) -> None:
        if not self._notifier.enabled:
            return
        await self._notifier.send(build_executor_alert(title, **fields))

    async def _handle_replay_response(self, payload: dict[str, Any]) -> None:
        """Handle `replay-response` frames from backend and resolve waiting futures."""
        try:
            signal_id = payload.get("signalId") or payload.get("signal_id")
            events_raw = payload.get("events") or []
            if not signal_id:
                self._logger.warning("Received replay-response without signalId")
                return

            fut = self._replay_waiters.pop(signal_id, None)
            if fut is None:
                self._logger.debug("No waiter for replay-response signal_id=%s", signal_id)
                return

            events: list[ExecutionEvent] = []
            for item in events_raw:
                try:
                    events.append(ExecutionEvent.from_json(item))
                except Exception as e:
                    self._logger.warning("Failed to parse replay event for signal_id=%s error=%s", signal_id, e)

            if not fut.done():
                fut.set_result(events)

        except Exception as e:
            self._logger.error("Error handling replay-response: %s", e, exc_info=True)

    # ── Balance sync loop ──────────────────────────────────────────────────

    async def _balance_sync_loop(self, stop_event: asyncio.Event) -> None:
        """Periodically emits account equity to backend for dashboard views."""
        interval = max(3600.0, self._config.balance_sync_interval_seconds)
        self._logger.info("Balance sync loop started interval=%.1fs", interval)

        # Wait briefly to let the handshake settle on startup
        await asyncio.sleep(5.0)

        while not stop_event.is_set():
            try:
                bal = await self._executor.fetch_balance()
                frame = {
                    "type": "audit-push",
                    "botId": self._config.bot_id,
                    "payload": {
                        "kind": "balance_snapshot",
                        "total": bal["total"],
                        "free": bal["free"],
                        "used": bal["used"],
                        "currency": bal["currency"],
                        "exchange": self._config.exchange_id,
                        "unrealizedPnl": bal.get("unrealizedPnl", 0.0),
                        "mode": self._config.execution_mode,
                        "executionMode": self._config.execution_mode,
                        "timestamp": self._ws_client._now_utc_iso(),
                    },
                }
                success = await self._ws_client.send_frame(frame)
                if success:
                    self._logger.debug(
                        "Balance synced total=%.2f currency=%s",
                        bal["total"],
                        bal["currency"],
                    )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._logger.warning("Balance sync failed: %s", exc)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                break  # stop_event was set
            except asyncio.TimeoutError:
                continue  # normal cycle
