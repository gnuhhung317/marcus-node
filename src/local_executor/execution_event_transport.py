"""
Execution Event Transport Layer (EVT-11)

Handles bidirectional communication of execution events between:
- Backend: sends execution_event frames (SIGNAL_ACCEPTED, ORDER_PLACED, etc.)
- Python Client: receives events, manages local state, sends execution_ack

Protocol:
- Inbound: execution_event {eventId, signalId, sequence, eventType, sentAt, exchangeTime, payload}
- Outbound: execution_ack {eventId, signalId, status, errorCode, errorMessage, receivedAt}
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from collections.abc import Awaitable, Callable


class ExecutionEventType(str, Enum):
    """9 event types from backend contract."""
    SIGNAL_ACCEPTED = "SIGNAL_ACCEPTED"
    SIGNAL_REJECTED = "SIGNAL_REJECTED"
    ORDER_PLACED = "ORDER_PLACED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_FAILED = "ORDER_FAILED"
    ORDER_CANCELED = "ORDER_CANCELED"
    POSITION_OPENED = "POSITION_OPENED"
    POSITION_UPDATED = "POSITION_UPDATED"
    POSITION_CLOSED = "POSITION_CLOSED"


class ACKStatus(str, Enum):
    """ACK response status."""
    OK = "OK"
    ERROR = "ERROR"


class ACKErrorCode(str, Enum):
    """ACK error codes from backend contract."""
    OUT_OF_ORDER = "OUT_OF_ORDER"
    POSITION_CLOSED = "POSITION_CLOSED"
    INVALID_STATE = "INVALID_STATE"
    UNAUTHORIZED = "UNAUTHORIZED"
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    INVALID_SEQUENCE = "INVALID_SEQUENCE"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """
    Immutable execution event from backend.
    
    Represents state transition in execution lifecycle:
    - Signal acceptance/rejection
    - Order state changes (placed, filled, failed, canceled)
    - Position state changes (opened, updated, closed)
    """
    event_id: str
    signal_id: str
    sequence: int
    event_type: ExecutionEventType
    sent_at: datetime
    exchange_time: datetime | None  # nullable
    payload: dict[str, Any]
    received_at: datetime | None = None  # set on client side

    def is_position_closing(self) -> bool:
        """Returns True if this event closes a position."""
        return self.event_type in (
            ExecutionEventType.POSITION_CLOSED,
            ExecutionEventType.ORDER_FAILED,
            ExecutionEventType.ORDER_CANCELED,
        )

    @staticmethod
    def from_json(data: dict[str, Any]) -> ExecutionEvent:
        """Parse ExecutionEvent from backend JSON envelope."""
        return ExecutionEvent(
            event_id=data["eventId"],
            signal_id=data["signalId"],
            sequence=data["sequence"],
            event_type=ExecutionEventType(data["eventType"]),
            sent_at=_parse_datetime(data["sentAt"]),
            exchange_time=_parse_datetime(data.get("exchangeTime")) if data.get("exchangeTime") else None,
            payload=data.get("payload", {}),
            received_at=datetime.utcnow(),
        )

    def to_json(self) -> dict[str, Any]:
        """Serialize ExecutionEvent to JSON."""
        return {
            "eventId": self.event_id,
            "signalId": self.signal_id,
            "sequence": self.sequence,
            "eventType": self.event_type.value,
            "sentAt": self.sent_at.isoformat(),
            "exchangeTime": self.exchange_time.isoformat() if self.exchange_time else None,
            "payload": self.payload,
            "receivedAt": self.received_at.isoformat() if self.received_at else None,
        }


@dataclass(frozen=True, slots=True)
class ExecutionACK:
    """
    Immutable execution ACK response to backend.
    
    Sent by client in response to execution_event frame.
    """
    event_id: str
    signal_id: str
    status: ACKStatus
    error_code: ACKErrorCode | None = None
    error_message: str | None = None
    received_at: datetime | None = None

    @staticmethod
    def ok(event_id: str, signal_id: str) -> ExecutionACK:
        """Create success ACK."""
        return ExecutionACK(
            event_id=event_id,
            signal_id=signal_id,
            status=ACKStatus.OK,
            received_at=datetime.utcnow(),
        )

    @staticmethod
    def error(
        event_id: str,
        signal_id: str,
        error_code: ACKErrorCode,
        error_message: str,
    ) -> ExecutionACK:
        """Create error ACK."""
        return ExecutionACK(
            event_id=event_id,
            signal_id=signal_id,
            status=ACKStatus.ERROR,
            error_code=error_code,
            error_message=error_message,
            received_at=datetime.utcnow(),
        )

    def to_json(self) -> dict[str, Any]:
        """Serialize ExecutionACK to JSON envelope."""
        return {
            "eventId": self.event_id,
            "signalId": self.signal_id,
            "status": self.status.value,
            "errorCode": self.error_code.value if self.error_code else None,
            "errorMessage": self.error_message,
            "receivedAt": self.received_at.isoformat() if self.received_at else None,
        }


EventHandler = Callable[[ExecutionEvent], Awaitable[None]]


class ExecutionEventTransport:
    """
    Bidirectional transport for execution events.
    
    Responsibilities:
    - Parse execution_event frames from WebSocket
    - Invoke event handler (local state engine)
    - Collect ACK responses from handler
    - Send execution_ack frames back to backend
    - Retry failed ACKs with exponential backoff
    - Track ACK delivery status
    """

    def __init__(
        self,
        on_event: EventHandler,
        logger: logging.Logger | None = None,
        retry_max_attempts: int = 3,
        retry_initial_delay: float = 0.5,
        retry_max_delay: float = 10.0,
    ):
        self._on_event = on_event
        self._logger = logger or logging.getLogger(__name__)
        self._retry_max_attempts = retry_max_attempts
        self._retry_initial_delay = retry_initial_delay
        self._retry_max_delay = retry_max_delay
        self._pending_acks: dict[str, ExecutionACK] = {}
        self._ack_lock = asyncio.Lock()

    async def handle_execution_event_frame(self, frame: dict[str, Any]) -> ExecutionACK:
        """
        Process inbound execution_event frame.
        
        Steps:
        1. Parse ExecutionEvent from frame
        2. Invoke event handler (which may throw validation errors)
        3. If handler succeeds, return OK ACK
        4. If handler fails, return ERROR ACK with error code
        
        Args:
            frame: Raw JSON frame from backend
            
        Returns:
            ExecutionACK to send back to backend
        """
        try:
            event = ExecutionEvent.from_json(frame)
            self._logger.debug(
                "Received execution_event event_id=%s signal_id=%s type=%s sequence=%d",
                event.event_id,
                event.signal_id,
                event.event_type.value,
                event.sequence,
            )

            # Invoke event handler (state engine)
            try:
                await self._on_event(event)
                ack = ExecutionACK.ok(event.event_id, event.signal_id)
                self._logger.info(
                    "Event processed successfully event_id=%s signal_id=%s type=%s",
                    event.event_id,
                    event.signal_id,
                    event.event_type.value,
                )
            except EventSequenceError as e:
                ack = ExecutionACK.error(
                    event.event_id,
                    event.signal_id,
                    ACKErrorCode.OUT_OF_ORDER,
                    str(e),
                )
                self._logger.warning(
                    "Event out of order event_id=%s signal_id=%s expected_seq=%d got_seq=%d",
                    event.event_id,
                    event.signal_id,
                    e.expected_sequence,
                    e.got_sequence,
                )
            except PositionClosedError as e:
                ack = ExecutionACK.error(
                    event.event_id,
                    event.signal_id,
                    ACKErrorCode.POSITION_CLOSED,
                    str(e),
                )
                self._logger.warning(
                    "Event rejected - position already closed event_id=%s signal_id=%s",
                    event.event_id,
                    event.signal_id,
                )
            except InvalidStateTransitionError as e:
                ack = ExecutionACK.error(
                    event.event_id,
                    event.signal_id,
                    ACKErrorCode.INVALID_STATE,
                    str(e),
                )
                self._logger.warning(
                    "Event rejected - invalid state transition event_id=%s signal_id=%s reason=%s",
                    event.event_id,
                    event.signal_id,
                    str(e),
                )
            except DuplicateEventError as e:
                ack = ExecutionACK.ok(event.event_id, event.signal_id)
                self._logger.info(
                    "Event already processed (idempotent) event_id=%s signal_id=%s",
                    event.event_id,
                    event.signal_id,
                )
            except Exception as e:
                ack = ExecutionACK.error(
                    event.event_id,
                    event.signal_id,
                    ACKErrorCode.INTERNAL_ERROR,
                    f"Internal error: {str(e)}",
                )
                self._logger.error(
                    "Unexpected error processing event event_id=%s signal_id=%s error=%s",
                    event.event_id,
                    event.signal_id,
                    str(e),
                    exc_info=True,
                )

        except Exception as e:
            self._logger.error(
                "Failed to parse execution_event frame error=%s",
                str(e),
                exc_info=True,
            )
            # Return generic error ACK
            ack = ExecutionACK.error(
                event_id=frame.get("eventId", "unknown"),
                signal_id=frame.get("signalId", "unknown"),
                error_code=ACKErrorCode.INTERNAL_ERROR,
                error_message=f"Failed to parse event: {str(e)}",
            )

        # Track pending ACK for retry
        async with self._ack_lock:
            self._pending_acks[ack.event_id] = ack

        return ack

    async def get_pending_acks(self) -> list[ExecutionACK]:
        """Get list of ACKs pending delivery."""
        async with self._ack_lock:
            return list(self._pending_acks.values())

    async def mark_ack_delivered(self, event_id: str) -> None:
        """Mark ACK as successfully delivered (remove from pending)."""
        async with self._ack_lock:
            self._pending_acks.pop(event_id, None)


# Exceptions for event validation

class EventValidationError(Exception):
    """Base exception for event validation errors."""
    pass


class EventSequenceError(EventValidationError):
    """Event sequence number is out of order."""
    def __init__(self, expected_sequence: int, got_sequence: int):
        self.expected_sequence = expected_sequence
        self.got_sequence = got_sequence
        super().__init__(
            f"Out of sequence: expected {expected_sequence}, got {got_sequence}"
        )


class PositionClosedError(EventValidationError):
    """Received event after position already closed."""
    def __init__(self, signal_id: str):
        super().__init__(f"Position already closed for signal {signal_id}")


class InvalidStateTransitionError(EventValidationError):
    """Event represents invalid state transition."""
    def __init__(self, from_state: str, to_state: str, event_type: str):
        super().__init__(
            f"Invalid transition from {from_state} to {to_state} via {event_type}"
        )


class DuplicateEventError(EventValidationError):
    """Event was already processed."""
    def __init__(self, event_id: str):
        super().__init__(f"Event already processed: {event_id}")


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse ISO datetime string."""
    if not value:
        return None
    try:
        # Handle ISO format with or without microseconds/timezone
        if "T" in value:
            # Remove timezone info for simplicity (assume UTC)
            if "+" in value:
                value = value.split("+")[0]
            elif value.endswith("Z"):
                value = value[:-1]
            return datetime.fromisoformat(value)
        return None
    except (ValueError, AttributeError):
        return None
