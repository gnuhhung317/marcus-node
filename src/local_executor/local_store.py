"""
Local Execution State Store (EVT-12)

Persistent local store for executor state using SQLite.

Schema:
- signals: track per-signal state (ACCEPTED, REJECTED, OPEN, CLOSED)
- orders: track order state (PLACED, FILLED, FAILED, CANCELED)
- positions: track position state (OPENED, UPDATING, CLOSED)
- execution_events: append-only log of all received events
- execution_acks: outbox pattern - ACKs pending delivery to backend
- last_sync: track recovery state per signal

Features:
- Per-signal sequence validation
- Idempotent event storage (event_id uniqueness)
- Late event rejection (no events after position closed)
- Outbox pattern for ACK delivery reliability
- State machine enforcement
"""

from __future__ import annotations

import asyncio
import sqlite3
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .execution_event_transport import (
    ExecutionEvent,
    ExecutionEventType,
    ExecutionACK,
)


@dataclass(slots=True)
class SignalState:
    """Per-signal execution state."""
    signal_id: str
    signal_state: str  # ACCEPTED, REJECTED, OPEN, CLOSED
    order_state: str  # NONE, PLACED, FILLED, FAILED, CANCELED
    position_state: str  # NONE, OPENED, UPDATING, CLOSED
    last_sequence: int
    last_event_time: datetime | None
    closed_at: datetime | None
    created_at: datetime
    updated_at: datetime


class LocalExecutionStore:
    """
    SQLite-backed local execution state store.
    
    Thread-safe async wrapper around SQLite with:
    - Per-signal state tracking
    - Event deduplication
    - Outbox pattern for ACK delivery
    - Sequence validation and late event rejection
    """

    def __init__(
        self,
        db_path: Path | str = ":memory:",
        logger: logging.Logger | None = None,
    ):
        self._db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self._logger = logger or logging.getLogger(__name__)
        self._lock = asyncio.Lock()
        self._conn: sqlite3.Connection | None = None

    async def initialize(self) -> None:
        """Create schema and initialize database."""
        async with self._lock:
            self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._create_schema()
            self._logger.info("Local store initialized db_path=%s", self._db_path)

    async def close(self) -> None:
        """Close database connection."""
        async with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None

    def _create_schema(self) -> None:
        """Create SQLite schema."""
        cursor = self._conn.cursor()

        # Signals table: track per-signal state machine
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS execution_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL UNIQUE,
                signal_state TEXT NOT NULL DEFAULT 'ACCEPTED',
                order_state TEXT NOT NULL DEFAULT 'NONE',
                position_state TEXT NOT NULL DEFAULT 'NONE',
                last_sequence INTEGER NOT NULL DEFAULT 0,
                last_event_time TEXT,
                closed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_id ON execution_signals(signal_id)")

        # Events table: append-only log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS execution_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                signal_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                exchange_time TEXT,
                payload TEXT NOT NULL,
                received_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (signal_id) REFERENCES execution_signals(signal_id),
                UNIQUE (signal_id, sequence)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_signal_id ON execution_events(signal_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_signal_sequence ON execution_events(signal_id, sequence)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_id ON execution_events(event_id)")

        # ACK outbox: pending ACK delivery
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS execution_acks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                signal_id TEXT NOT NULL,
                status TEXT NOT NULL,
                error_code TEXT,
                error_message TEXT,
                received_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                last_delivery_attempt TEXT,
                FOREIGN KEY (signal_id) REFERENCES execution_signals(signal_id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ack_pending ON execution_acks(status)")

        # Recovery tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recovery_state (
                signal_id TEXT PRIMARY KEY,
                bootstrap_sequence INTEGER NOT NULL DEFAULT 0,
                recovered_at TEXT,
                FOREIGN KEY (signal_id) REFERENCES execution_signals(signal_id)
            )
        """)

        self._conn.commit()

    # ============ Signal State Management ============

    async def get_or_create_signal(self, signal_id: str) -> SignalState:
        """Get signal state, creating if doesn't exist."""
        async with self._lock:
            cursor = self._conn.cursor()
            
            # Try to fetch existing
            cursor.execute(
                "SELECT * FROM execution_signals WHERE signal_id = ?",
                (signal_id,),
            )
            row = cursor.fetchone()
            
            if row:
                return self._row_to_signal_state(row)
            
            # Create new signal with ACCEPTED state
            now = datetime.utcnow().isoformat()
            cursor.execute("""
                INSERT INTO execution_signals
                (signal_id, signal_state, order_state, position_state, 
                 last_sequence, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (signal_id, "ACCEPTED", "NONE", "NONE", 0, now, now))
            self._conn.commit()
            
            self._logger.debug("Created new signal state signal_id=%s", signal_id)
            
            cursor.execute(
                "SELECT * FROM execution_signals WHERE signal_id = ?",
                (signal_id,),
            )
            row = cursor.fetchone()
            return self._row_to_signal_state(row)

    async def get_signal_state(self, signal_id: str) -> SignalState | None:
        """Get current signal state."""
        async with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT * FROM execution_signals WHERE signal_id = ?",
                (signal_id,),
            )
            row = cursor.fetchone()
            return self._row_to_signal_state(row) if row else None

    async def is_position_closed(self, signal_id: str) -> bool:
        """Check if position is closed for signal."""
        state = await self.get_signal_state(signal_id)
        return state.position_state == "CLOSED" if state else False

    async def get_last_sequence(self, signal_id: str) -> int:
        """Get last processed sequence for signal."""
        state = await self.get_signal_state(signal_id)
        return state.last_sequence if state else 0

    async def update_signal_state(
        self,
        signal_id: str,
        signal_state: str | None = None,
        order_state: str | None = None,
        position_state: str | None = None,
        last_sequence: int | None = None,
        closed_at: datetime | None = None,
    ) -> SignalState:
        """Update signal state fields."""
        async with self._lock:
            cursor = self._conn.cursor()
            now = datetime.utcnow().isoformat()
            
            # Build dynamic UPDATE
            updates = ["updated_at = ?"]
            params: list[Any] = [now]
            
            if signal_state is not None:
                updates.append("signal_state = ?")
                params.append(signal_state)
            if order_state is not None:
                updates.append("order_state = ?")
                params.append(order_state)
            if position_state is not None:
                updates.append("position_state = ?")
                params.append(position_state)
            if last_sequence is not None:
                updates.append("last_sequence = ?")
                params.append(last_sequence)
                updates.append("last_event_time = ?")
                params.append(now)
            if closed_at is not None:
                updates.append("closed_at = ?")
                params.append(closed_at.isoformat() if closed_at else None)
            
            params.append(signal_id)
            
            sql = f"UPDATE execution_signals SET {', '.join(updates)} WHERE signal_id = ?"
            cursor.execute(sql, params)
            self._conn.commit()
            
            cursor.execute(
                "SELECT * FROM execution_signals WHERE signal_id = ?",
                (signal_id,),
            )
            row = cursor.fetchone()
            return self._row_to_signal_state(row)

    # ============ Event Storage ============

    async def store_event(self, event: ExecutionEvent) -> bool:
        """
        Store execution event.
        
        Returns:
            True if stored, False if already exists (duplicate)
        """
        async with self._lock:
            cursor = self._conn.cursor()
            
            # Check if duplicate
            cursor.execute(
                "SELECT id FROM execution_events WHERE event_id = ?",
                (event.event_id,),
            )
            if cursor.fetchone():
                self._logger.debug(
                    "Event already stored (duplicate) event_id=%s",
                    event.event_id,
                )
                return False

            # Insert new event
            now = datetime.utcnow().isoformat()
            received_at = event.received_at.isoformat() if event.received_at else now
            
            try:
                cursor.execute("""
                    INSERT INTO execution_events
                    (event_id, signal_id, sequence, event_type, sent_at,
                     exchange_time, payload, received_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event.event_id,
                    event.signal_id,
                    event.sequence,
                    event.event_type.value,
                    event.sent_at.isoformat(),
                    event.exchange_time.isoformat() if event.exchange_time else None,
                    _serialize_payload(event.payload),
                    received_at,
                    now,
                ))
                self._conn.commit()
                return True
            except sqlite3.IntegrityError as e:
                self._logger.warning(
                    "Failed to store event (integrity) event_id=%s error=%s",
                    event.event_id,
                    str(e),
                )
                return False

    async def event_exists(self, event_id: str) -> bool:
        """Return True if an event with `event_id` exists in the store."""
        async with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT 1 FROM execution_events WHERE event_id = ? LIMIT 1",
                (event_id,),
            )
            return cursor.fetchone() is not None

    async def get_events_for_signal(
        self,
        signal_id: str,
        from_sequence: int = 0,
        limit: int = 100,
    ) -> list[ExecutionEvent]:
        """Get events for signal in sequence order."""
        async with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                SELECT * FROM execution_events
                WHERE signal_id = ? AND sequence >= ?
                ORDER BY sequence ASC
                LIMIT ?
            """, (signal_id, from_sequence, limit))
            
            events = []
            for row in cursor.fetchall():
                events.append(self._row_to_event(row))
            return events

    # ============ ACK Outbox (Delivery Reliability) ============

    async def store_ack(self, ack: ExecutionACK) -> bool:
        """
        Store ACK in outbox for delivery.
        
        Returns:
            True if stored, False if already exists
        """
        async with self._lock:
            cursor = self._conn.cursor()
            
            # Check if duplicate
            cursor.execute(
                "SELECT id FROM execution_acks WHERE event_id = ?",
                (ack.event_id,),
            )
            if cursor.fetchone():
                return False
            
            now = datetime.utcnow().isoformat()
            # Ensure received_at is not NULL to satisfy schema
            received_at_val = ack.received_at.isoformat() if ack.received_at else now
            try:
                cursor.execute("""
                    INSERT INTO execution_acks
                    (event_id, signal_id, status, error_code, error_message,
                     received_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    ack.event_id,
                    ack.signal_id,
                    ack.status.value,
                    ack.error_code.value if ack.error_code else None,
                    ack.error_message,
                    received_at_val,
                    now,
                ))
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    async def get_pending_acks(self) -> list[ExecutionACK]:
        """Get ACKs pending delivery."""
        async with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                SELECT * FROM execution_acks
                WHERE status = 'OK' OR status = 'ERROR'
                ORDER BY created_at ASC
            """)
            
            acks = []
            for row in cursor.fetchall():
                acks.append(self._row_to_ack(row))
            return acks

    async def mark_ack_delivered(self, event_id: str) -> None:
        """Mark ACK as delivered."""
        async with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "DELETE FROM execution_acks WHERE event_id = ?",
                (event_id,),
            )
            self._conn.commit()
            self._logger.debug("ACK marked delivered event_id=%s", event_id)

    async def increment_ack_delivery_attempts(self, event_id: str) -> None:
        """Increment delivery attempt counter for ACK."""
        async with self._lock:
            cursor = self._conn.cursor()
            now = datetime.utcnow().isoformat()
            cursor.execute("""
                UPDATE execution_acks
                SET delivery_attempts = delivery_attempts + 1,
                    last_delivery_attempt = ?
                WHERE event_id = ?
            """, (now, event_id))
            self._conn.commit()

    # ============ Recovery Support ============

    async def set_recovery_state(self, signal_id: str, bootstrap_sequence: int) -> None:
        """Mark signal as recovered from bootstrap."""
        async with self._lock:
            cursor = self._conn.cursor()
            now = datetime.utcnow().isoformat()
            cursor.execute("""
                INSERT OR REPLACE INTO recovery_state
                (signal_id, bootstrap_sequence, recovered_at)
                VALUES (?, ?, ?)
            """, (signal_id, bootstrap_sequence, now))
            self._conn.commit()

    async def get_recovery_state(self, signal_id: str) -> int:
        """Get last bootstrap sequence for recovery."""
        async with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT bootstrap_sequence FROM recovery_state WHERE signal_id = ?",
                (signal_id,),
            )
            row = cursor.fetchone()
            return row[0] if row else 0

    # ============ Helper Methods ============

    def _row_to_signal_state(self, row: sqlite3.Row) -> SignalState:
        """Convert SQLite row to SignalState."""
        return SignalState(
            signal_id=row["signal_id"],
            signal_state=row["signal_state"],
            order_state=row["order_state"],
            position_state=row["position_state"],
            last_sequence=row["last_sequence"],
            last_event_time=_parse_datetime(row["last_event_time"]),
            closed_at=_parse_datetime(row["closed_at"]),
            created_at=_parse_datetime(row["created_at"]),
            updated_at=_parse_datetime(row["updated_at"]),
        )

    def _row_to_event(self, row: sqlite3.Row) -> ExecutionEvent:
        """Convert SQLite row to ExecutionEvent."""
        return ExecutionEvent(
            event_id=row["event_id"],
            signal_id=row["signal_id"],
            sequence=row["sequence"],
            event_type=ExecutionEventType(row["event_type"]),
            sent_at=_parse_datetime(row["sent_at"]),
            exchange_time=_parse_datetime(row["exchange_time"]),
            payload=_deserialize_payload(row["payload"]),
            received_at=_parse_datetime(row["received_at"]),
        )

    def _row_to_ack(self, row: sqlite3.Row) -> ExecutionACK:
        """Convert SQLite row to ExecutionACK."""
        from .execution_event_transport import ACKStatus, ACKErrorCode
        
        error_code = None
        if row["error_code"]:
            error_code = ACKErrorCode(row["error_code"])
        
        return ExecutionACK(
            event_id=row["event_id"],
            signal_id=row["signal_id"],
            status=ACKStatus(row["status"]),
            error_code=error_code,
            error_message=row["error_message"],
            received_at=_parse_datetime(row["received_at"]),
        )


def _serialize_payload(payload: dict[str, Any]) -> str:
    """Serialize payload to JSON string."""
    import json
    return json.dumps(payload, default=str)


def _deserialize_payload(payload_str: str) -> dict[str, Any]:
    """Deserialize payload from JSON string."""
    import json
    try:
        return json.loads(payload_str)
    except (json.JSONDecodeError, TypeError):
        return {}


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse ISO datetime string."""
    if not value:
        return None
    try:
        if "T" in value:
            if "+" in value:
                value = value.split("+")[0]
            elif value.endswith("Z"):
                value = value[:-1]
            return datetime.fromisoformat(value)
        return None
    except (ValueError, AttributeError):
        return None
