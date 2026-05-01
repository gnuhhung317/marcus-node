"""
Execution State Engine (EVT-11/12)

Orchestrates event validation and state transitions.

Responsibilities:
- Validate sequence numbers
- Enforce state machine transitions
- Update local state in store
- Detect duplicates and late events
- Enforce business rules (no events after position closed)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .execution_event_transport import (
    ExecutionEvent,
    ExecutionEventType,
    EventSequenceError,
    PositionClosedError,
    InvalidStateTransitionError,
    DuplicateEventError,
)
from .local_store import LocalExecutionStore, SignalState


class ExecutionStateEngine:
    """
    Validates and applies execution events to local state.
    
    Enforces:
    - Per-signal sequence ordering (1, 2, 3, ...)
    - State machine transitions (Signal → Order → Position)
    - Late event rejection (no events after position closed)
    - Duplicate detection (event_id uniqueness)
    """

    def __init__(
        self,
        store: LocalExecutionStore,
        logger: logging.Logger | None = None,
    ):
        self._store = store
        self._logger = logger or logging.getLogger(__name__)

    async def process_event(self, event: ExecutionEvent) -> None:
        """
        Process execution event with full validation.
        
        Steps:
        1. Check for duplicate (raise DuplicateEventError if found)
        2. Get/create signal state
        3. Check if position closed (raise PositionClosedError)
        4. Validate sequence (raise EventSequenceError)
        5. Validate state transition (raise InvalidStateTransitionError)
        6. Update local state in store
        
        Raises:
            DuplicateEventError: Event already processed
            PositionClosedError: Position already closed
            EventSequenceError: Sequence number out of order
            InvalidStateTransitionError: Invalid state transition
        """
        self._logger.debug(
            "Processing event event_id=%s signal_id=%s type=%s sequence=%d",
            event.event_id,
            event.signal_id,
            event.event_type.value,
            event.sequence,
        )

        # Step 1: Check for duplicate
        stored = await self._store.get_events_for_signal(
            event.signal_id,
            from_sequence=0,
            limit=10000,  # Large limit to check all
        )
        for existing_event in stored:
            if existing_event.event_id == event.event_id:
                self._logger.debug(
                    "Duplicate event detected event_id=%s",
                    event.event_id,
                )
                raise DuplicateEventError(event.event_id)

        # Step 2: Get/create signal state
        signal_state = await self._store.get_or_create_signal(event.signal_id)

        # Step 3: Check if position already closed
        if signal_state.position_state == "CLOSED":
            self._logger.warning(
                "Event received after position closed signal_id=%s event_type=%s",
                event.signal_id,
                event.event_type.value,
            )
            raise PositionClosedError(event.signal_id)

        # Step 4: Validate sequence
        expected_sequence = signal_state.last_sequence + 1
        if event.sequence != expected_sequence:
            self._logger.warning(
                "Sequence mismatch signal_id=%s expected=%d got=%d",
                event.signal_id,
                expected_sequence,
                event.sequence,
            )
            raise EventSequenceError(expected_sequence, event.sequence)

        # Step 5: Validate state transition
        self._validate_state_transition(signal_state, event)

        # Step 6: Store event in local store
        await self._store.store_event(event)

        # Step 7: Update signal state based on event type
        await self._apply_event_to_state(signal_state, event)

        self._logger.info(
            "Event processed successfully signal_id=%s event_type=%s sequence=%d",
            event.signal_id,
            event.event_type.value,
            event.sequence,
        )

    def _validate_state_transition(
        self,
        current_state: SignalState,
        event: ExecutionEvent,
    ) -> None:
        """
        Validate state machine transitions.
        
        State machines:
        - Signal: ACCEPTED → OPEN | REJECTED → CLOSED
        - Order: NONE → PLACED → FILLED|FAILED|CANCELED
        - Position: NONE → OPENED → UPDATING → CLOSED
        """
        event_type = event.event_type

        # Signal acceptance
        if event_type == ExecutionEventType.SIGNAL_ACCEPTED:
            if current_state.signal_state not in ("ACCEPTED", "OPEN"):
                raise InvalidStateTransitionError(
                    current_state.signal_state,
                    "OPEN",
                    event_type.value,
                )

        elif event_type == ExecutionEventType.SIGNAL_REJECTED:
            if current_state.signal_state not in ("ACCEPTED", "REJECTED"):
                raise InvalidStateTransitionError(
                    current_state.signal_state,
                    "REJECTED",
                    event_type.value,
                )

        # Order transitions
        elif event_type == ExecutionEventType.ORDER_PLACED:
            if current_state.order_state not in ("NONE", "PLACED"):
                raise InvalidStateTransitionError(
                    current_state.order_state,
                    "PLACED",
                    event_type.value,
                )

        elif event_type == ExecutionEventType.ORDER_FILLED:
            if current_state.order_state not in ("PLACED", "FILLED"):
                raise InvalidStateTransitionError(
                    current_state.order_state,
                    "FILLED",
                    event_type.value,
                )

        elif event_type == ExecutionEventType.ORDER_FAILED:
            if current_state.order_state not in ("PLACED", "FAILED"):
                raise InvalidStateTransitionError(
                    current_state.order_state,
                    "FAILED",
                    event_type.value,
                )

        elif event_type == ExecutionEventType.ORDER_CANCELED:
            if current_state.order_state not in ("PLACED", "CANCELED"):
                raise InvalidStateTransitionError(
                    current_state.order_state,
                    "CANCELED",
                    event_type.value,
                )

        # Position transitions
        elif event_type == ExecutionEventType.POSITION_OPENED:
            if current_state.position_state not in ("NONE", "OPENED"):
                raise InvalidStateTransitionError(
                    current_state.position_state,
                    "OPENED",
                    event_type.value,
                )

        elif event_type == ExecutionEventType.POSITION_UPDATED:
            if current_state.position_state not in ("OPENED", "UPDATING"):
                raise InvalidStateTransitionError(
                    current_state.position_state,
                    "UPDATING",
                    event_type.value,
                )

        elif event_type == ExecutionEventType.POSITION_CLOSED:
            if current_state.position_state not in ("OPENED", "UPDATING", "CLOSED"):
                raise InvalidStateTransitionError(
                    current_state.position_state,
                    "CLOSED",
                    event_type.value,
                )

    async def _apply_event_to_state(
        self,
        current_state: SignalState,
        event: ExecutionEvent,
    ) -> None:
        """Update signal state based on event type."""
        event_type = event.event_type
        now = event.exchange_time or event.sent_at

        # Signal state
        if event_type == ExecutionEventType.SIGNAL_ACCEPTED:
            await self._store.update_signal_state(
                event.signal_id,
                signal_state="OPEN",
                last_sequence=event.sequence,
            )

        elif event_type == ExecutionEventType.SIGNAL_REJECTED:
            await self._store.update_signal_state(
                event.signal_id,
                signal_state="REJECTED",
                last_sequence=event.sequence,
            )

        # Order state
        elif event_type == ExecutionEventType.ORDER_PLACED:
            await self._store.update_signal_state(
                event.signal_id,
                order_state="PLACED",
                last_sequence=event.sequence,
            )

        elif event_type == ExecutionEventType.ORDER_FILLED:
            await self._store.update_signal_state(
                event.signal_id,
                order_state="FILLED",
                last_sequence=event.sequence,
            )

        elif event_type == ExecutionEventType.ORDER_FAILED:
            await self._store.update_signal_state(
                event.signal_id,
                order_state="FAILED",
                last_sequence=event.sequence,
            )

        elif event_type == ExecutionEventType.ORDER_CANCELED:
            await self._store.update_signal_state(
                event.signal_id,
                order_state="CANCELED",
                last_sequence=event.sequence,
            )

        # Position state
        elif event_type == ExecutionEventType.POSITION_OPENED:
            await self._store.update_signal_state(
                event.signal_id,
                position_state="OPENED",
                last_sequence=event.sequence,
            )

        elif event_type == ExecutionEventType.POSITION_UPDATED:
            await self._store.update_signal_state(
                event.signal_id,
                position_state="UPDATING",
                last_sequence=event.sequence,
            )

        elif event_type == ExecutionEventType.POSITION_CLOSED:
            await self._store.update_signal_state(
                event.signal_id,
                position_state="CLOSED",
                last_sequence=event.sequence,
                closed_at=now,
            )
