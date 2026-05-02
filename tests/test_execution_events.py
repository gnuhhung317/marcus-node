"""
Tests for Phase 3 (EVT-11-14) execution event transport and state management.

Coverage:
- ExecutionEventTransport: frame parsing, ACK generation, error handling
- LocalExecutionStore: SQLite persistence, state tracking, outbox pattern
- ExecutionStateEngine: sequence validation, state transitions
- ExecutionRecoveryManager: bootstrap, history pull, replay, exchange sync
"""

import pytest
import pytest_asyncio
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from local_executor.execution_event_transport import (
    ExecutionEvent,
    ExecutionEventType,
    ExecutionACK,
    ACKStatus,
    ACKErrorCode,
    ExecutionEventTransport,
    EventSequenceError,
    PositionClosedError,
    InvalidStateTransitionError,
    DuplicateEventError,
)
from local_executor.local_store import LocalExecutionStore
from local_executor.execution_state_engine import ExecutionStateEngine
from local_executor.recovery_manager import ExecutionRecoveryManager, RecoveryPhase


# ============ Fixtures ============

@pytest.fixture
def event_transport():
    """Create ExecutionEventTransport for testing."""
    events_received = []

    async def on_event(event: ExecutionEvent):
        events_received.append(event)

    return ExecutionEventTransport(on_event=on_event), events_received


@pytest_asyncio.fixture
async def local_store():
    """Create in-memory SQLite store for testing."""
    store = LocalExecutionStore(db_path=":memory:")
    await store.initialize()
    yield store
    await store.close()


@pytest_asyncio.fixture
async def state_engine(local_store):
    """Create ExecutionStateEngine for testing."""
    return ExecutionStateEngine(store=local_store)


@pytest_asyncio.fixture
async def recovery_manager(local_store, state_engine):
    """Create ExecutionRecoveryManager for testing."""
    return ExecutionRecoveryManager(store=local_store, state_engine=state_engine)


# ============ ExecutionEvent Tests ============

class TestExecutionEvent:
    """Test ExecutionEvent value object."""

    def test_from_json_success(self):
        """Test parsing ExecutionEvent from backend JSON."""
        frame = {
            "eventId": "evt-001",
            "signalId": "sig-001",
            "sequence": 1,
            "eventType": "ORDER_PLACED",
            "sentAt": "2026-05-01T10:00:00",
            "exchangeTime": "2026-05-01T10:00:01",
            "payload": {"order_id": "oid-001", "symbol": "BTCUSDT"},
        }

        event = ExecutionEvent.from_json(frame)

        assert event.event_id == "evt-001"
        assert event.signal_id == "sig-001"
        assert event.sequence == 1
        assert event.event_type == ExecutionEventType.ORDER_PLACED
        assert event.payload["order_id"] == "oid-001"
        assert event.received_at is not None

    def test_is_position_closing(self):
        """Test position-closing event detection."""
        # POSITION_CLOSED
        event1 = ExecutionEvent(
            event_id="e1",
            signal_id="s1",
            sequence=5,
            event_type=ExecutionEventType.POSITION_CLOSED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )
        assert event1.is_position_closing()

        # ORDER_FAILED
        event2 = ExecutionEvent(
            event_id="e2",
            signal_id="s1",
            sequence=5,
            event_type=ExecutionEventType.ORDER_FAILED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )
        assert event2.is_position_closing()

        # ORDER_PLACED (not closing)
        event3 = ExecutionEvent(
            event_id="e3",
            signal_id="s1",
            sequence=2,
            event_type=ExecutionEventType.ORDER_PLACED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )
        assert not event3.is_position_closing()

    def test_ack_ok(self):
        """Test creating success ACK."""
        ack = ExecutionACK.ok("evt-001", "sig-001")

        assert ack.event_id == "evt-001"
        assert ack.signal_id == "sig-001"
        assert ack.status == ACKStatus.OK
        assert ack.error_code is None
        assert ack.error_message is None

    def test_ack_error(self):
        """Test creating error ACK."""
        ack = ExecutionACK.error(
            "evt-001",
            "sig-001",
            ACKErrorCode.OUT_OF_ORDER,
            "Sequence out of order",
        )

        assert ack.status == ACKStatus.ERROR
        assert ack.error_code == ACKErrorCode.OUT_OF_ORDER
        assert ack.error_message == "Sequence out of order"


# ============ ExecutionEventTransport Tests ============

class TestExecutionEventTransport:
    """Test ExecutionEventTransport."""

    @pytest.mark.asyncio
    async def test_handle_execution_event_success(self, event_transport):
        """Test handling successful execution event."""
        transport, events_received = event_transport

        frame = {
            "eventId": "evt-001",
            "signalId": "sig-001",
            "sequence": 1,
            "eventType": "ORDER_PLACED",
            "sentAt": "2026-05-01T10:00:00",
            "exchangeTime": "2026-05-01T10:00:01",
            "payload": {"order_id": "oid-001"},
        }

        ack = await transport.handle_execution_event_frame(frame)

        assert ack.status == ACKStatus.OK
        assert len(events_received) == 1
        assert events_received[0].event_id == "evt-001"

    @pytest.mark.asyncio
    async def test_handle_execution_event_error(self, event_transport):
        """Test handling event that raises error."""
        async def failing_handler(event: ExecutionEvent):
            raise EventSequenceError(2, 1)

        transport = ExecutionEventTransport(on_event=failing_handler)

        frame = {
            "eventId": "evt-001",
            "signalId": "sig-001",
            "sequence": 1,
            "eventType": "ORDER_PLACED",
            "sentAt": "2026-05-01T10:00:00",
            "payload": {},
        }

        ack = await transport.handle_execution_event_frame(frame)

        assert ack.status == ACKStatus.ERROR
        assert ack.error_code == ACKErrorCode.OUT_OF_ORDER

    @pytest.mark.asyncio
    async def test_get_pending_acks(self, event_transport):
        """Test retrieving pending ACKs."""
        transport, _ = event_transport

        # Generate some ACKs
        for i in range(3):
            frame = {
                "eventId": f"evt-{i:03d}",
                "signalId": f"sig-001",
                "sequence": i + 1,
                "eventType": "ORDER_PLACED",
                "sentAt": "2026-05-01T10:00:00",
                "payload": {},
            }
            await transport.handle_execution_event_frame(frame)

        pending = await transport.get_pending_acks()
        assert len(pending) == 3

        # Mark one as delivered
        await transport.mark_ack_delivered("evt-000")
        pending = await transport.get_pending_acks()
        assert len(pending) == 2


# ============ LocalExecutionStore Tests ============

class TestLocalExecutionStore:
    """Test LocalExecutionStore."""

    @pytest.mark.asyncio
    async def test_get_or_create_signal(self, local_store):
        """Test creating new signal state."""
        state = await local_store.get_or_create_signal("sig-001")

        assert state.signal_id == "sig-001"
        assert state.signal_state == "ACCEPTED"
        assert state.order_state == "NONE"
        assert state.position_state == "NONE"
        assert state.last_sequence == 0

    @pytest.mark.asyncio
    async def test_store_event(self, local_store):
        """Test storing execution event."""
        event = ExecutionEvent(
            event_id="evt-001",
            signal_id="sig-001",
            sequence=1,
            event_type=ExecutionEventType.ORDER_PLACED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={"order_id": "oid-001"},
        )

        stored = await local_store.store_event(event)
        assert stored

        # Try to store duplicate
        stored_again = await local_store.store_event(event)
        assert not stored_again

    @pytest.mark.asyncio
    async def test_get_events_for_signal(self, local_store):
        """Test retrieving events for signal."""
        # Store some events
        for i in range(3):
            event = ExecutionEvent(
                event_id=f"evt-{i:03d}",
                signal_id="sig-001",
                sequence=i + 1,
                event_type=ExecutionEventType.ORDER_PLACED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={},
            )
            await local_store.store_event(event)

        events = await local_store.get_events_for_signal("sig-001", from_sequence=0)
        assert len(events) == 3
        assert events[0].sequence == 1
        assert events[2].sequence == 3

    @pytest.mark.asyncio
    async def test_update_signal_state(self, local_store):
        """Test updating signal state."""
        await local_store.get_or_create_signal("sig-001")

        state = await local_store.update_signal_state(
            "sig-001",
            signal_state="OPEN",
            order_state="PLACED",
            last_sequence=1,
        )

        assert state.signal_state == "OPEN"
        assert state.order_state == "PLACED"
        assert state.last_sequence == 1

    @pytest.mark.asyncio
    async def test_is_position_closed(self, local_store):
        """Test checking if position is closed."""
        await local_store.get_or_create_signal("sig-001")

        # Initially not closed
        is_closed = await local_store.is_position_closed("sig-001")
        assert not is_closed

        # Update to closed
        await local_store.update_signal_state(
            "sig-001",
            position_state="CLOSED",
        )

        is_closed = await local_store.is_position_closed("sig-001")
        assert is_closed

    @pytest.mark.asyncio
    async def test_ack_outbox(self, local_store):
        """Test ACK outbox pattern."""
        ack1 = ExecutionACK.ok("evt-001", "sig-001")
        ack2 = ExecutionACK.error(
            "evt-002",
            "sig-001",
            ACKErrorCode.OUT_OF_ORDER,
            "Out of order",
        )

        # Store ACKs
        await local_store.store_ack(ack1)
        await local_store.store_ack(ack2)

        # Get pending
        pending = await local_store.get_pending_acks()
        assert len(pending) == 2

        # Mark one as delivered
        await local_store.mark_ack_delivered("evt-001")
        pending = await local_store.get_pending_acks()
        assert len(pending) == 1
        assert pending[0].event_id == "evt-002"


# ============ ExecutionStateEngine Tests ============

class TestExecutionStateEngine:
    """Test ExecutionStateEngine."""

    @pytest.mark.asyncio
    async def test_process_signal_accepted(self, state_engine, local_store):
        """Test processing SIGNAL_ACCEPTED event."""
        event = ExecutionEvent(
            event_id="evt-001",
            signal_id="sig-001",
            sequence=1,
            event_type=ExecutionEventType.SIGNAL_ACCEPTED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )

        await state_engine.process_event(event)

        state = await local_store.get_signal_state("sig-001")
        assert state.signal_state == "OPEN"
        assert state.last_sequence == 1

    @pytest.mark.asyncio
    async def test_process_order_placed(self, state_engine, local_store):
        """Test processing ORDER_PLACED event."""
        # Must accept signal first
        sig_event = ExecutionEvent(
            event_id="evt-001",
            signal_id="sig-001",
            sequence=1,
            event_type=ExecutionEventType.SIGNAL_ACCEPTED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )
        await state_engine.process_event(sig_event)

        # Then place order
        order_event = ExecutionEvent(
            event_id="evt-002",
            signal_id="sig-001",
            sequence=2,
            event_type=ExecutionEventType.ORDER_PLACED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={"order_id": "oid-001"},
        )
        await state_engine.process_event(order_event)

        state = await local_store.get_signal_state("sig-001")
        assert state.order_state == "PLACED"
        assert state.last_sequence == 2

    @pytest.mark.asyncio
    async def test_duplicate_event_rejected(self, state_engine, local_store):
        """Test that duplicate events are rejected."""
        event = ExecutionEvent(
            event_id="evt-001",
            signal_id="sig-001",
            sequence=1,
            event_type=ExecutionEventType.SIGNAL_ACCEPTED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )

        # Process first
        await state_engine.process_event(event)

        # Try to process duplicate
        with pytest.raises(DuplicateEventError):
            await state_engine.process_event(event)

    @pytest.mark.asyncio
    async def test_sequence_out_of_order(self, state_engine, local_store):
        """Test sequence validation."""
        event1 = ExecutionEvent(
            event_id="evt-001",
            signal_id="sig-001",
            sequence=1,
            event_type=ExecutionEventType.SIGNAL_ACCEPTED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )

        # Process first event
        await state_engine.process_event(event1)

        # Try to process with wrong sequence (should be 2)
        event3 = ExecutionEvent(
            event_id="evt-003",
            signal_id="sig-001",
            sequence=3,
            event_type=ExecutionEventType.ORDER_PLACED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )

        with pytest.raises(EventSequenceError) as exc_info:
            await state_engine.process_event(event3)
        assert exc_info.value.expected_sequence == 2
        assert exc_info.value.got_sequence == 3

    @pytest.mark.asyncio
    async def test_late_event_after_position_closed(self, state_engine, local_store):
        """Test late event rejection."""
        # Create signal and close it
        signal_id = "sig-001"
        await local_store.get_or_create_signal(signal_id)
        await local_store.update_signal_state(
            signal_id,
            position_state="CLOSED",
        )

        # Try to process event
        event = ExecutionEvent(
            event_id="evt-001",
            signal_id=signal_id,
            sequence=1,
            event_type=ExecutionEventType.ORDER_PLACED,
            sent_at=datetime.utcnow(),
            exchange_time=None,
            payload={},
        )

        with pytest.raises(PositionClosedError):
            await state_engine.process_event(event)

    @pytest.mark.asyncio
    async def test_full_lifecycle(self, state_engine, local_store):
        """Test full event lifecycle: signal → order → position → close."""
        signal_id = "sig-001"

        events = [
            ExecutionEvent(
                event_id="evt-001",
                signal_id=signal_id,
                sequence=1,
                event_type=ExecutionEventType.SIGNAL_ACCEPTED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={},
            ),
            ExecutionEvent(
                event_id="evt-002",
                signal_id=signal_id,
                sequence=2,
                event_type=ExecutionEventType.ORDER_PLACED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={"order_id": "oid-001"},
            ),
            ExecutionEvent(
                event_id="evt-003",
                signal_id=signal_id,
                sequence=3,
                event_type=ExecutionEventType.ORDER_FILLED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={"order_id": "oid-001", "fill_price": 65000.0},
            ),
            ExecutionEvent(
                event_id="evt-004",
                signal_id=signal_id,
                sequence=4,
                event_type=ExecutionEventType.POSITION_OPENED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={"position_size": 0.1},
            ),
            ExecutionEvent(
                event_id="evt-005",
                signal_id=signal_id,
                sequence=5,
                event_type=ExecutionEventType.POSITION_CLOSED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={"pnl": 100.0},
            ),
        ]

        for event in events:
            await state_engine.process_event(event)

        state = await local_store.get_signal_state(signal_id)
        assert state.signal_state == "OPEN"
        assert state.order_state == "FILLED"
        assert state.position_state == "CLOSED"
        assert state.last_sequence == 5
        assert state.closed_at is not None


# ============ ExecutionRecoveryManager Tests ============

class TestExecutionRecoveryManager:
    """Test ExecutionRecoveryManager."""

    @pytest.mark.asyncio
    async def test_recovery_bootstrap(self, recovery_manager, local_store):
        """Test recovery bootstrap phase."""
        # Create some signals in the store
        await local_store.get_or_create_signal("sig-001")
        await local_store.get_or_create_signal("sig-002")

        status = await recovery_manager.recover(
            signal_ids=["sig-001", "sig-002"],
        )

        assert status.phase == RecoveryPhase.COMPLETE
        assert "sig-001" in status.signals_to_recover
        assert "sig-002" in status.signals_to_recover

    @pytest.mark.asyncio
    async def test_recovery_replay(self, recovery_manager, local_store, state_engine):
        """Test recovery replay phase."""
        signal_id = "sig-001"

        # Store some events
        events = [
            ExecutionEvent(
                event_id=f"evt-{i:03d}",
                signal_id=signal_id,
                sequence=i + 1,
                event_type=ExecutionEventType.SIGNAL_ACCEPTED if i == 0 else ExecutionEventType.ORDER_PLACED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={},
            )
            for i in range(3)
        ]

        for event in events:
            await local_store.store_event(event)

        # Run recovery
        status = await recovery_manager.recover(signal_ids=[signal_id])

        assert status.phase == RecoveryPhase.COMPLETE
        assert status.signals_recovered == 1
        assert status.events_replayed == 3

    @pytest.mark.asyncio
    async def test_recovery_pull_history_persists_events(self, recovery_manager, local_store):
        """Test that pull-history phase persists fetched events into local store."""
        signal_id = "sig-hist-001"

        all_events = [
            ExecutionEvent(
                event_id="evt-h-001",
                signal_id=signal_id,
                sequence=1,
                event_type=ExecutionEventType.SIGNAL_ACCEPTED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={},
            ),
            ExecutionEvent(
                event_id="evt-h-002",
                signal_id=signal_id,
                sequence=2,
                event_type=ExecutionEventType.ORDER_PLACED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={},
            ),
            ExecutionEvent(
                event_id="evt-h-003",
                signal_id=signal_id,
                sequence=3,
                event_type=ExecutionEventType.ORDER_PLACED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={},
            ),
        ]

        async def fetch_history_func(sig_id: str, from_sequence: int = 0):
            assert sig_id == signal_id
            return [event for event in all_events if event.sequence >= from_sequence][:2]

        status = await recovery_manager.recover(
            signal_ids=[signal_id],
            fetch_history_func=fetch_history_func,
        )

        stored_events = await local_store.get_events_for_signal(signal_id, from_sequence=0, limit=100)
        assert len(stored_events) == 3
        assert status.phase == RecoveryPhase.COMPLETE
        assert status.events_replayed == 3

    @pytest.mark.asyncio
    async def test_recovery_replay_uses_pagination(self, recovery_manager, local_store):
        """Test replay can process more events than one replay page."""
        signal_id = "sig-page-001"

        events = [
            ExecutionEvent(
                event_id="evt-p-001",
                signal_id=signal_id,
                sequence=1,
                event_type=ExecutionEventType.SIGNAL_ACCEPTED,
                sent_at=datetime.utcnow(),
                exchange_time=None,
                payload={},
            )
        ]
        for i in range(2, 8):
            events.append(
                ExecutionEvent(
                    event_id=f"evt-p-{i:03d}",
                    signal_id=signal_id,
                    sequence=i,
                    event_type=ExecutionEventType.ORDER_PLACED,
                    sent_at=datetime.utcnow(),
                    exchange_time=None,
                    payload={},
                )
            )

        for event in events:
            await local_store.store_event(event)

        recovery_manager._replay_page_size = 2
        status = await recovery_manager.recover(signal_ids=[signal_id])

        assert status.phase == RecoveryPhase.COMPLETE
        assert status.signals_recovered == 1
        assert status.events_replayed == len(events)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
