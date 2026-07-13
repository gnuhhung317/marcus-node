from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import replace
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from .config import ExecutorConfig
from .execution import CcxtSignalExecutor, ExecutionResult
from .local_store import LocalExecutionStore
from .ws_client import ResilientWebSocketClient
from .recovery_manager import ExecutionRecoveryManager
from .execution_state_engine import ExecutionStateEngine
from .execution_event_transport import ExecutionEvent, ExecutionEventType, stable_backend_event_id
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
        self._outbound_execution_flush_lock = asyncio.Lock()
        self._outbound_execution_waiting_ack_event_id: str | None = None
        self._blocked_outbound_signal_sequences: dict[str, int] = {}
        self._scheduled_execution_flush_task: asyncio.Task[None] | None = None
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
            on_execution_ack=self._handle_execution_ack,
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
            asyncio.create_task(self._execution_sync_loop(stop_event), name="execution_sync_loop"),
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
            await self._cancel_scheduled_execution_flush()

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
                await self._local_store.get_or_create_signal(signal_id)
                order_symbol = result.symbol or payload.get("symbol") or payload.get("asset_pair") or payload.get("assetPair")
                market_type = result.market_type or payload.get("market_type") or payload.get("marketType") or self._config.exchange_default_type
                order_id = result.entry_order_id or result.order_id
                filled_amount = result.filled_amount if result.filled_amount is not None else 0.0
                order_state = "FILLED" if filled_amount > 0 else "PLACED"
                position_state = "OPENED" if filled_amount > 0 else "NONE"
                backend_events = self._build_initial_execution_events(
                    signal_id=signal_id,
                    payload=payload,
                    result=result,
                    order_symbol=order_symbol,
                    order_id=order_id,
                    market_type=market_type,
                    filled_amount=filled_amount,
                )
                backend_last_sequence = backend_events[-1].sequence if backend_events else None

                await self._local_store.update_signal_state(
                    signal_id,
                    signal_state="OPEN",
                    order_state=order_state,
                    position_state=position_state,
                    last_sequence=backend_last_sequence,
                    policies=payload.get("policies"),
                    order_id=order_id,
                    order_symbol=order_symbol,
                    market_type=market_type,
                    action=result.action or payload.get("action"),
                    filled_amount=result.filled_amount,
                    tp_order_id=result.tp_order_id,
                    sl_order_id=result.sl_order_id,
                    take_profit=result.take_profit,
                    stop_loss=result.stop_loss,
                    protection_status=result.protection_status,
                )
                for event in backend_events:
                    await self._queue_execution_event(event)
                await self._flush_pending_execution_events()
                if result.execution_status in {"ENTRY_FILLED_UNPROTECTED", "PARTIALLY_PROTECTED"}:
                    await self._notify(
                        "Protection warning",
                        bot_id=self._config.bot_id,
                        signal_id=signal_id,
                        action=action,
                        symbol=order_symbol or symbol,
                        mode=result.mode,
                        execution_status=result.execution_status,
                        protection_status=result.protection_status,
                        entry_order_id=order_id,
                        tp_order_id=result.tp_order_id,
                        sl_order_id=result.sl_order_id,
                        warnings="; ".join(result.warnings or []) if result.warnings else None,
                    )
            else:
                # Record the signal but mark as REJECTED so dedup still protects
                await self._local_store.get_or_create_signal(signal_id)
                await self._local_store.update_signal_state(
                    signal_id,
                    signal_state="REJECTED",
                    policies=payload.get("policies"),
                    market_type=result.market_type or payload.get("market_type") or payload.get("marketType") or self._config.exchange_default_type,
                    action=result.action or payload.get("action"),
                )

    def _build_initial_execution_events(
        self,
        signal_id: str,
        payload: dict[str, Any],
        result: ExecutionResult,
        order_symbol: str | None,
        order_id: str | None,
        market_type: str | None,
        filled_amount: float,
    ) -> list[ExecutionEvent]:
        symbol = order_symbol or payload.get("symbol") or payload.get("asset_pair") or payload.get("assetPair")
        events = [
            self._make_execution_event(
                signal_id,
                0,
                ExecutionEventType.SIGNAL_ACCEPTED,
                {
                    "symbol": symbol,
                    "action": result.action or payload.get("action"),
                    "market_type": market_type,
                    "policies": payload.get("policies"),
                },
            ),
            self._make_execution_event(
                signal_id,
                1,
                ExecutionEventType.ORDER_PLACED,
                {
                    "order_id": order_id,
                    "symbol": symbol,
                    "market_type": market_type,
                },
            ),
        ]

        if filled_amount > 0:
            events.append(
                self._make_execution_event(
                    signal_id,
                    2,
                    ExecutionEventType.ORDER_FILLED,
                    {
                        "order_id": order_id,
                        "symbol": symbol,
                        "fill_price": self._extract_entry_fill_price(result),
                        "filled_amount": filled_amount,
                    },
                )
            )
            events.append(
                self._make_execution_event(
                    signal_id,
                    3,
                    ExecutionEventType.POSITION_OPENED,
                    {
                        "symbol": symbol,
                        "position_size": filled_amount,
                        "market_type": market_type,
                    },
                )
            )

        return events

    def _make_execution_event(
        self,
        signal_id: str,
        sequence: int,
        event_type: ExecutionEventType,
        payload: dict[str, Any],
    ) -> ExecutionEvent:
        now = datetime.now(timezone.utc)
        return ExecutionEvent(
            event_id=stable_backend_event_id("sync", signal_id, sequence, event_type.value),
            signal_id=signal_id,
            sequence=sequence,
            event_type=event_type,
            sent_at=now,
            exchange_time=now,
            payload={key: value for key, value in payload.items() if value is not None},
        )

    def _extract_entry_fill_price(self, result: ExecutionResult) -> float | None:
        candidates: list[Any] = []
        if isinstance(result.raw_orders, dict):
            entry = result.raw_orders.get("entry")
            if isinstance(entry, dict):
                candidates.extend([entry.get("average"), entry.get("price"), entry.get("fill_price")])
        entry_detail = result.details.get("entry_order") if isinstance(result.details, dict) else None
        if isinstance(entry_detail, dict):
            candidates.extend([entry_detail.get("average"), entry_detail.get("price"), entry_detail.get("fill_price")])

        for value in candidates:
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
        return None

    async def _queue_execution_event(self, event: ExecutionEvent) -> None:
        if self._local_store is None:
            return
        await self._local_store.store_event(event)
        await self._local_store.store_outbound_execution_event(event)

    async def _send_execution_event(self, event: ExecutionEvent) -> bool:
        if self._local_store is not None:
            await self._local_store.increment_outbound_execution_event_attempts(event.event_id)
        frame = {
            "type": "execution_event",
            "botId": self._config.bot_id,
            "payload": event.to_backend_json(),
        }
        return await self._ws_client.send_frame(frame)

    async def _flush_pending_execution_events(self) -> None:
        if self._local_store is None:
            return
        async with self._outbound_execution_flush_lock:
            if self._outbound_execution_waiting_ack_event_id is not None:
                return

            pending = await self._local_store.get_pending_outbound_execution_events(limit=100)
            if not pending:
                return

            item, retry_delay = self._select_next_outbound_execution_event(pending)
            if item is None:
                if retry_delay is not None:
                    self._schedule_execution_event_flush(retry_delay)
                return

            event = item.event
            if len(event.event_id) > 36:
                normalized_event_id = stable_backend_event_id(
                    "sync",
                    event.signal_id,
                    event.sequence,
                    event.event_type.value,
                )
                await self._local_store.normalize_outbound_execution_event_id(event.event_id, normalized_event_id)
                event = replace(event, event_id=normalized_event_id)

            sent = await self._send_execution_event(event)
            if sent:
                self._outbound_execution_waiting_ack_event_id = event.event_id
                return

            self._schedule_execution_event_flush(self._retry_delay_seconds(item.delivery_attempts + 1))

    def _select_next_outbound_execution_event(
        self,
        pending: list[Any],
    ) -> tuple[Any | None, float | None]:
        now = datetime.now(timezone.utc)
        lowest_sequence_by_signal: dict[str, int] = {}
        next_retry_delay: float | None = None

        for item in pending:
            signal_id = item.event.signal_id
            current = lowest_sequence_by_signal.get(signal_id)
            if current is None or item.event.sequence < current:
                lowest_sequence_by_signal[signal_id] = item.event.sequence

        for item in pending:
            signal_id = item.event.signal_id
            expected_sequence = self._blocked_outbound_signal_sequences.get(signal_id)
            if expected_sequence is not None and item.event.sequence != expected_sequence:
                continue
            if item.event.sequence != lowest_sequence_by_signal.get(signal_id):
                continue

            retry_delay = self._remaining_retry_delay_seconds(item, now)
            if retry_delay > 0:
                next_retry_delay = retry_delay if next_retry_delay is None else min(next_retry_delay, retry_delay)
                continue

            return item, None

        return None, next_retry_delay

    def _remaining_retry_delay_seconds(self, item: Any, now: datetime) -> float:
        if item.last_delivery_attempt is None or item.delivery_attempts <= 0:
            return 0.0
        retry_at = item.last_delivery_attempt.replace(tzinfo=timezone.utc).timestamp() + self._retry_delay_seconds(
            item.delivery_attempts
        )
        return max(0.0, retry_at - now.timestamp())

    def _retry_delay_seconds(self, attempts: int) -> float:
        capped_attempts = max(0, attempts - 1)
        return min(30.0, 0.5 * (2 ** capped_attempts))

    def _schedule_execution_event_flush(self, delay_seconds: float) -> None:
        if self._scheduled_execution_flush_task is not None and not self._scheduled_execution_flush_task.done():
            return

        async def _delayed_flush() -> None:
            try:
                await asyncio.sleep(max(0.0, delay_seconds))
                await self._flush_pending_execution_events()
            except asyncio.CancelledError:
                return

        self._scheduled_execution_flush_task = asyncio.create_task(_delayed_flush(), name="execution_event_flush_retry")

    async def _cancel_scheduled_execution_flush(self) -> None:
        if self._scheduled_execution_flush_task is None or self._scheduled_execution_flush_task.done():
            return
        self._scheduled_execution_flush_task.cancel()
        await asyncio.gather(self._scheduled_execution_flush_task, return_exceptions=True)
        self._scheduled_execution_flush_task = None

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
        self._outbound_execution_waiting_ack_event_id = None

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

        try:
            status = await self._recovery_manager.recover(fetch_history_func=fetch_history_func)
            self._logger.info("Recovery status: phase=%s errors=%s", status.phase.value, status.errors)
            await self._flush_pending_execution_events()
        except Exception as e:
            self._logger.error("Recovery run failed: %s", e, exc_info=True)

    async def _execution_sync_loop(self, stop_event: asyncio.Event) -> None:
        """Periodically reconcile active signals with exchange state and publish backend events."""
        interval = max(1.0, self._config.execution_sync_interval_seconds)
        self._logger.info("Execution sync loop started interval=%.1fs", interval)

        await asyncio.sleep(2.0)

        while not stop_event.is_set():
            try:
                await self._flush_pending_execution_events()
                if (
                    self._config.execution_mode == "live"
                    and self._local_store is not None
                    and self._state_engine is not None
                ):
                    signal_ids = await self._local_store.get_active_signals()
                    for signal_id in signal_ids:
                        await self._sync_signal_execution_state(signal_id)
                    await self._flush_pending_execution_events()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._logger.warning("Execution sync loop failed: %s", exc)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                continue

    async def _sync_signal_execution_state(self, signal_id: str) -> None:
        if self._local_store is None or self._state_engine is None:
            return

        exchange_events = await self._executor.sync_exchange(signal_id, self._local_store)
        if not exchange_events:
            return

        last_sequence = await self._local_store.get_last_sequence(signal_id)
        for exchange_event in exchange_events:
            last_sequence += 1
            event = ExecutionEvent(
                event_id=exchange_event.event_id,
                signal_id=exchange_event.signal_id,
                sequence=last_sequence,
                event_type=exchange_event.event_type,
                sent_at=exchange_event.sent_at,
                exchange_time=exchange_event.exchange_time,
                payload=exchange_event.payload,
                received_at=datetime.now(timezone.utc),
            )
            try:
                await self._state_engine.process_event(event)
                await self._queue_execution_event(event)
            except Exception as exc:
                self._logger.warning(
                    "Failed to process execution sync event signal_id=%s event_id=%s type=%s error=%s",
                    signal_id,
                    event.event_id,
                    event.event_type.value,
                    exc,
                )

    async def _handle_execution_ack(self, payload: dict[str, Any]) -> None:
        event_id = payload.get("eventId") or payload.get("event_id")
        signal_id = payload.get("signalId") or payload.get("signal_id")
        status = str(payload.get("status") or "").strip().upper()
        if not event_id:
            self._logger.warning("Received execution_ack without eventId")
            return

        if str(event_id) == self._outbound_execution_waiting_ack_event_id:
            self._outbound_execution_waiting_ack_event_id = None

        if status == "OK":
            if self._local_store is not None:
                await self._local_store.mark_outbound_execution_event_delivered(str(event_id))
                if signal_id is not None:
                    self._blocked_outbound_signal_sequences.pop(str(signal_id), None)
                await self._flush_pending_execution_events()
            return

        error_code = str(payload.get("errorCode") or payload.get("error_code") or "").strip().upper()
        error_message = str(payload.get("errorMessage") or payload.get("error_message") or "")
        self._logger.warning(
            "Backend rejected execution event event_id=%s status=%s error_code=%s message=%s",
            event_id,
            status or "UNKNOWN",
            error_code or None,
            error_message or None,
        )
        if self._local_store is not None and error_code == "OUT_OF_ORDER" and signal_id:
            expected_sequence = self._extract_expected_sequence(error_message)
            if expected_sequence is not None:
                restored = await self._local_store.requeue_outbound_execution_events(str(signal_id), expected_sequence)
                self._blocked_outbound_signal_sequences[str(signal_id)] = expected_sequence
                self._logger.info(
                    "Requeued outbound execution events for out-of-order recovery signal_id=%s expected_sequence=%s restored=%s",
                    signal_id,
                    expected_sequence,
                    restored,
                )
                await self._flush_pending_execution_events()
                return

        self._schedule_execution_event_flush(self._retry_delay_seconds(1))

    def _extract_expected_sequence(self, error_message: str) -> int | None:
        match = re.search(r"Expected sequence\s+(\d+),\s+received\s+\d+", error_message)
        if not match:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

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
