"""
Recovery Flow (EVT-14)

Bootstrap execution state after client redeploy by:
1. Query backend recovery endpoint for last sequences
2. Pull full event history from backend
3. Replay events to rebuild local state
4. Synchronize with exchange state (open orders, positions)
5. Resume event consumption from recovered sequence
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from datetime import datetime
from enum import Enum

from .execution_event_transport import ExecutionEvent
from .local_store import LocalExecutionStore
from .execution_state_engine import ExecutionStateEngine


class RecoveryPhase(str, Enum):
    """Recovery process phases."""
    BOOTSTRAP = "bootstrap"          # Query backend for last sequence per signal
    PULL_HISTORY = "pull_history"    # Get full event history from backend
    REPLAY = "replay"                # Replay events to rebuild local state
    EXCHANGE_SYNC = "exchange_sync"  # Sync with exchange (orders, positions)
    COMPLETE = "complete"            # Recovery done


@dataclass(slots=True)
class RecoveryStatus:
    """Status of recovery process."""
    phase: RecoveryPhase
    signals_to_recover: list[str]
    signals_recovered: int
    events_replayed: int
    errors: list[str]
    completed_at: datetime | None = None


class ExecutionRecoveryManager:
    """
    Manages recovery flow after client redeploy.
    
    Process:
    1. Bootstrap: Ask backend "what's the last sequence I had for each signal?"
    2. PullHistory: Fetch all events for those signals
    3. Replay: Re-apply those events to local store using state engine
    4. ExchangeSync: Check exchange for any fills/updates since last event
    5. Complete: Ready to resume normal event consumption
    """

    def __init__(
        self,
        store: LocalExecutionStore,
        state_engine: ExecutionStateEngine,
        logger: logging.Logger | None = None,
    ):
        self._store = store
        self._state_engine = state_engine
        self._logger = logger or logging.getLogger(__name__)
        self._recovery_status = RecoveryStatus(
            phase=RecoveryPhase.BOOTSTRAP,
            signals_to_recover=[],
            signals_recovered=0,
            events_replayed=0,
            errors=[],
        )
        self._replay_page_size = 1000

    async def recover(
        self,
        signal_ids: list[str] | None = None,
        fetch_history_func: Any | None = None,
        sync_exchange_func: Any | None = None,
    ) -> RecoveryStatus:
        """
        Execute full recovery flow.
        
        Args:
            signal_ids: Signals to recover. If None, recover all known signals.
            fetch_history_func: Async callable to fetch event history from backend.
                               Should accept (signal_id, from_sequence) and return list[ExecutionEvent]
            sync_exchange_func: Async callable to sync with exchange.
                               Should accept (signal_id) and return list[ExecutionEvent] for fills/updates
        
        Returns:
            RecoveryStatus with results
        """
        self._logger.info(
            "Starting recovery phase=%s signals=%s",
            self._recovery_status.phase.value,
            len(signal_ids or []),
        )

        try:
            # Phase 1: Bootstrap - determine which signals need recovery
            signals_to_recover = await self._bootstrap_signals(signal_ids or [])
            self._recovery_status.signals_to_recover = signals_to_recover
            self._logger.info(
                "Bootstrap phase complete signals_to_recover=%d",
                len(signals_to_recover),
            )

            # Phase 2: Pull History - fetch events from backend
            if fetch_history_func:
                await self._pull_history_phase(signals_to_recover, fetch_history_func)

            # Phase 3: Replay - apply events to local state
            await self._replay_phase(signals_to_recover)

            # Phase 4: Exchange Sync - sync with exchange
            if sync_exchange_func:
                await self._exchange_sync_phase(signals_to_recover, sync_exchange_func)

            # Phase 5: Mark recovered
            for signal_id in signals_to_recover:
                bootstrap_seq = await self._store.get_last_sequence(signal_id)
                await self._store.set_recovery_state(signal_id, bootstrap_seq)

            self._recovery_status.phase = RecoveryPhase.COMPLETE
            self._recovery_status.completed_at = datetime.utcnow()
            self._logger.info(
                "Recovery complete phase=%s signals_recovered=%d events_replayed=%d",
                self._recovery_status.phase.value,
                self._recovery_status.signals_recovered,
                self._recovery_status.events_replayed,
            )

        except Exception as e:
            self._logger.error(
                "Recovery failed phase=%s error=%s",
                self._recovery_status.phase.value,
                str(e),
                exc_info=True,
            )
            self._recovery_status.errors.append(str(e))
            raise

        return self._recovery_status

    async def _bootstrap_signals(self, signal_ids: list[str]) -> list[str]:
        """
        Phase 1: Bootstrap
        
        Determine which signals need recovery:
        - If signal_ids provided, use those
        - Otherwise, check store for signals already in progress
        """
        self._recovery_status.phase = RecoveryPhase.BOOTSTRAP
        
        if signal_ids:
            # Explicit signals provided
            to_recover = signal_ids
        else:
            # Auto-discover signals from store (those with sequence > 0)
            to_recover = []
            # TODO: Add discovery method to LocalExecutionStore
        
        self._logger.info(
            "Phase: BOOTSTRAP signals_to_recover=%d",
            len(to_recover),
        )
        return to_recover

    async def _pull_history_phase(
        self,
        signal_ids: list[str],
        fetch_history_func: Any,
    ) -> None:
        """
        Phase 2: Pull History from backend
        
        Fetch all events for each signal from the recovery endpoint.
        """
        self._recovery_status.phase = RecoveryPhase.PULL_HISTORY
        self._logger.info("Phase: PULL_HISTORY signals=%d", len(signal_ids))

        for signal_id in signal_ids:
            try:
                # Ensure FK target exists before persisting events
                await self._store.get_or_create_signal(signal_id)

                # Pull and persist history incrementally from backend
                from_sequence = 0
                total_fetched = 0
                total_stored = 0
                while True:
                    events = await fetch_history_func(signal_id, from_sequence=from_sequence)
                    if not events:
                        break

                    events = sorted(events, key=lambda e: e.sequence)

                    max_sequence_in_page = from_sequence
                    for event in events:
                        max_sequence_in_page = max(max_sequence_in_page, event.sequence)
                        if await self._store.store_event(event):
                            total_stored += 1

                    total_fetched += len(events)

                    # Prevent infinite loops if backend ignores from_sequence.
                    if max_sequence_in_page < from_sequence:
                        break

                    from_sequence = max_sequence_in_page + 1

                self._logger.info(
                    "Fetched and persisted history signal_id=%s fetched=%d stored=%d",
                    signal_id,
                    total_fetched,
                    total_stored,
                )
            except Exception as e:
                self._logger.warning(
                    "Failed to fetch history for signal_id=%s error=%s",
                    signal_id,
                    str(e),
                )
                self._recovery_status.errors.append(
                    f"Failed to fetch history for {signal_id}: {str(e)}"
                )

    async def _replay_phase(self, signal_ids: list[str]) -> None:
        """
        Phase 3: Replay
        
        Re-apply all stored events to rebuild local state.
        """
        self._recovery_status.phase = RecoveryPhase.REPLAY
        self._logger.info("Phase: REPLAY signals=%d", len(signal_ids))

        for signal_id in signal_ids:
            try:
                # Ensure signal row exists before resetting state fields.
                await self._store.get_or_create_signal(signal_id)

                # Reset state to initial for replay
                await self._store.update_signal_state(
                    signal_id,
                    signal_state="ACCEPTED",
                    order_state="NONE",
                    position_state="NONE",
                    last_sequence=0,
                )

                events_for_signal = 0
                next_sequence = 0
                while True:
                    events = await self._store.get_events_for_signal(
                        signal_id,
                        from_sequence=next_sequence,
                        limit=self._replay_page_size,
                    )

                    if not events:
                        break

                    for event in events:
                        try:
                            await self._state_engine.replay_event(event)
                            self._recovery_status.events_replayed += 1
                            events_for_signal += 1
                        except Exception as e:
                            self._logger.error(
                                "Failed to replay event signal_id=%s event_id=%s error=%s",
                                signal_id,
                                event.event_id,
                                str(e),
                            )
                            # Continue replay despite individual event errors
                            pass

                    next_sequence = events[-1].sequence + 1

                if events_for_signal == 0:
                    self._logger.debug("No events to replay for signal_id=%s", signal_id)
                    continue

                self._recovery_status.signals_recovered += 1
                self._logger.info(
                    "Replayed events signal_id=%s count=%d",
                    signal_id,
                    events_for_signal,
                )

            except Exception as e:
                self._logger.error(
                    "Failed to replay signal_id=%s error=%s",
                    signal_id,
                    str(e),
                    exc_info=True,
                )
                self._recovery_status.errors.append(
                    f"Failed to replay {signal_id}: {str(e)}"
                )

    async def _exchange_sync_phase(
        self,
        signal_ids: list[str],
        sync_exchange_func: Any,
    ) -> None:
        """
        Phase 4: Exchange Sync
        
        Check exchange for any fills/updates since last event.
        Emit synthetic events for exchange updates.
        """
        self._recovery_status.phase = RecoveryPhase.EXCHANGE_SYNC
        self._logger.info("Phase: EXCHANGE_SYNC signals=%d", len(signal_ids))

        for signal_id in signal_ids:
            try:
                # Get current sequence from store
                last_sequence = await self._store.get_last_sequence(signal_id)

                # Ask exchange for any updates
                exchange_events = await sync_exchange_func(signal_id)

                for exc_event in exchange_events:
                    try:
                        # Exchange events should continue sequence from last
                        last_sequence += 1
                        exc_event_with_seq = ExecutionEvent(
                            event_id=exc_event.event_id,
                            signal_id=exc_event.signal_id,
                            sequence=last_sequence,
                            event_type=exc_event.event_type,
                            sent_at=exc_event.sent_at,
                            exchange_time=exc_event.exchange_time,
                            payload=exc_event.payload,
                            received_at=datetime.utcnow(),
                        )

                        await self._state_engine.process_event(exc_event_with_seq)
                        self._recovery_status.events_replayed += 1

                    except Exception as e:
                        self._logger.warning(
                            "Failed to process exchange event signal_id=%s error=%s",
                            signal_id,
                            str(e),
                        )

                self._logger.info(
                    "Synced exchange updates signal_id=%s count=%d",
                    signal_id,
                    len(exchange_events),
                )

            except Exception as e:
                self._logger.error(
                    "Failed to sync exchange for signal_id=%s error=%s",
                    signal_id,
                    str(e),
                )
                self._recovery_status.errors.append(
                    f"Failed to sync exchange for {signal_id}: {str(e)}"
                )

    def get_status(self) -> RecoveryStatus:
        """Get current recovery status."""
        return self._recovery_status
