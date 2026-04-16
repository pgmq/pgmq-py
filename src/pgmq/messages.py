# src/pgmq/messages.py
"""
Dataclasses representing PGMQ database types.

These classes map directly to PostgreSQL composite types defined by the PGMQ
extension, ensuring type safety and IDE autocomplete support.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, Callable, Mapping, Union


def _get_value(
    row: Union[tuple, Mapping], key: str, index: Optional[int] = None
) -> Any:
    """
    Safely get a value from a row by name (preferred) or index (fallback).

    This allows us to switch from brittle index-based access to robust
    name-based access while maintaining backward compatibility with raw tuples.
    """
    if isinstance(row, Mapping):
        # Try exact key match first (psycopg/asyncpg usually return lowercase keys)
        if key in row:
            return row[key]
        # Fallback for potential capitalization issues (e.g. 'Queue_name')
        # Though PGMQ standard is lowercase snake_case
        for k in row.keys():
            if k.lower() == key.lower():
                return row[k]

    # Fallback to index if provided and row is a sequence (tuple/list)
    if (
        index is not None
        and hasattr(row, "__getitem__")
        and not isinstance(row, Mapping)
    ):
        try:
            return row[index]
        except IndexError:
            pass

    raise KeyError(
        f"Could not find column '{key}' in row. Row type: {type(row)}, Content: {row}"
    )


@dataclass
class Message:
    """
    Complete message record matching pgmq.message_record type.
    """

    msg_id: int
    read_ct: int
    enqueued_at: datetime
    last_read_at: Optional[datetime]
    vt: datetime
    message: Dict[str, Any]
    headers: Optional[Dict[str, Any]]

    @classmethod
    def from_row(
        cls,
        row: Union[tuple, Mapping],
        json_parser: Optional[Callable[[Any], Dict[str, Any]]] = None,
    ) -> "Message":
        def _identity(x: Any) -> Any:
            return x

        if json_parser is None:
            json_parser = _identity

        # Using names makes this resilient to SQL column reordering
        return cls(
            msg_id=_get_value(row, "msg_id", 0),
            read_ct=_get_value(row, "read_ct", 1),
            enqueued_at=_get_value(row, "enqueued_at", 2),
            last_read_at=_get_value(row, "last_read_at", 3),
            vt=_get_value(row, "vt", 4),
            message=json_parser(_get_value(row, "message", 5)),
            headers=json_parser(_get_value(row, "headers", 6))
            if _get_value(row, "headers", 6) is not None
            else None,
        )

    def __repr__(self) -> str:
        return (
            f"Message(msg_id={self.msg_id}, read_ct={self.read_ct}, "
            f"enqueued_at={self.enqueued_at.isoformat()}, "
            f"message={self.message!r})"
        )


@dataclass
class QueueRecord:
    """
    Queue metadata matching pgmq.queue_record type.
    """

    queue_name: str
    is_partitioned: bool
    is_unlogged: bool
    created_at: datetime

    @classmethod
    def from_row(cls, row: Union[tuple, Mapping]) -> "QueueRecord":
        return cls(
            queue_name=_get_value(row, "queue_name", 0),
            created_at=_get_value(row, "created_at", 1),
            is_partitioned=_get_value(row, "is_partitioned", 2),
            is_unlogged=_get_value(row, "is_unlogged", 3),
        )

    def __str__(self) -> str:
        return self.queue_name


@dataclass
class QueueMetrics:
    """
    Queue statistics matching pgmq.metrics_result type.
    """

    queue_name: str
    queue_length: int
    newest_msg_age_sec: Optional[int]
    oldest_msg_age_sec: Optional[int]
    total_messages: int
    scrape_time: datetime
    queue_visible_length: int

    @classmethod
    def from_row(cls, row: Union[tuple, Mapping]) -> "QueueMetrics":
        # Handle both old (6 columns) and new (7 columns) schema versions
        # We check length only if it's a sequence, otherwise rely on key existence
        has_visible_length = False
        if not isinstance(row, Mapping):
            has_visible_length = len(row) >= 7
        else:
            has_visible_length = "queue_visible_length" in row

        visible_length_val = (
            _get_value(row, "queue_visible_length", 6)
            if has_visible_length
            else _get_value(row, "queue_length", 1)
        )

        return cls(
            queue_name=_get_value(row, "queue_name", 0),
            queue_length=_get_value(row, "queue_length", 1),
            newest_msg_age_sec=_get_value(row, "newest_msg_age_sec", 2),
            oldest_msg_age_sec=_get_value(row, "oldest_msg_age_sec", 3),
            total_messages=_get_value(row, "total_messages", 4),
            scrape_time=_get_value(row, "scrape_time", 5),
            queue_visible_length=visible_length_val,
        )


@dataclass
class TopicBinding:
    """
    Topic routing binding record.
    """

    pattern: str
    queue_name: str
    bound_at: datetime
    compiled_regex: str

    @classmethod
    def from_row(cls, row: Union[tuple, Mapping]) -> "TopicBinding":
        return cls(
            pattern=_get_value(row, "pattern", 0),
            queue_name=_get_value(row, "queue_name", 1),
            bound_at=_get_value(row, "bound_at", 2),
            compiled_regex=_get_value(row, "compiled_regex", 3),
        )


@dataclass
class RoutingResult:
    """
    Result from test_routing function.
    """

    pattern: str
    queue_name: str
    compiled_regex: str

    @classmethod
    def from_row(cls, row: Union[tuple, Mapping]) -> "RoutingResult":
        return cls(
            pattern=_get_value(row, "pattern", 0),
            queue_name=_get_value(row, "queue_name", 1),
            compiled_regex=_get_value(row, "compiled_regex", 2),
        )


@dataclass
class BatchTopicResult:
    """
    Result from send_batch_topic function.
    """

    queue_name: str
    msg_id: int

    @classmethod
    def from_row(cls, row: Union[tuple, Mapping]) -> "BatchTopicResult":
        return cls(
            queue_name=_get_value(row, "queue_name", 0),
            msg_id=_get_value(row, "msg_id", 1),
        )


@dataclass
class NotificationThrottle:
    """
    Notification throttle configuration.
    """

    queue_name: str
    throttle_interval_ms: int
    last_notified_at: datetime

    @classmethod
    def from_row(cls, row: Union[tuple, Mapping]) -> "NotificationThrottle":
        return cls(
            queue_name=_get_value(row, "queue_name", 0),
            throttle_interval_ms=_get_value(row, "throttle_interval_ms", 1),
            last_notified_at=_get_value(row, "last_notified_at", 2),
        )
