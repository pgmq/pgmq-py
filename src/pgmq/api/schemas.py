# src/pgmq/api/schemas.py
"""Pydantic request and response models for the PGMQ HTTP API."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, field_validator

from pgmq.messages import Message, QueueMetrics, QueueRecord


class HealthResponse(BaseModel):
    """Liveness payload. The process is up; the database is not checked."""

    status: str
    version: str
    auth: str


class ReadyResponse(BaseModel):
    """Readiness payload after a successful init and ping."""

    status: str


class ErrorResponse(BaseModel):
    """Stable application error body. Keys stay frozen after ship."""

    error: str
    detail: str


class QueueRecordResponse(BaseModel):
    """Queue metadata. Field names match ``QueueRecord``."""

    queue_name: str
    is_partitioned: bool
    is_unlogged: bool
    created_at: datetime


class QueueMetricsResponse(BaseModel):
    """Queue statistics. Field names match ``QueueMetrics``."""

    queue_name: str
    queue_length: int
    newest_msg_age_sec: Optional[int]
    oldest_msg_age_sec: Optional[int]
    total_messages: int
    scrape_time: datetime
    queue_visible_length: int


class QueuesResponse(BaseModel):
    """List of queue records."""

    queues: List[QueueRecordResponse]


class MetricsListResponse(BaseModel):
    """List of queue metrics."""

    queues: List[QueueMetricsResponse]


class CreateQueueResponse(BaseModel):
    """PUT create response. ``unlogged`` echoes the request flag."""

    queue_name: str
    unlogged: bool


class PurgeResponse(BaseModel):
    """Purge result."""

    purged: int


def _validate_delay(value: Any) -> Any:
    """Reject a negative integer delay. Datetime values pass through."""
    if isinstance(value, bool) or (isinstance(value, int) and value < 0):
        raise ValueError("delay must be a non-negative integer or datetime")
    return value


class SendRequest(BaseModel):
    """Single send body. ``message`` must be a JSON object."""

    message: Dict[str, Any]
    headers: Optional[Dict[str, Any]] = None
    delay: Optional[Union[int, datetime]] = None

    @field_validator("delay")
    @classmethod
    def delay_must_be_non_negative(cls, value: Any) -> Any:
        return _validate_delay(value)


class SendBatchRequest(BaseModel):
    """Batch send body. Each item in ``messages`` must be a JSON object."""

    messages: List[Dict[str, Any]]
    headers: Optional[List[Dict[str, Any]]] = None
    delay: Optional[Union[int, datetime]] = None

    @field_validator("delay")
    @classmethod
    def delay_must_be_non_negative(cls, value: Any) -> Any:
        return _validate_delay(value)


class SendResponse(BaseModel):
    """Single send result."""

    msg_id: int


class SendBatchResponse(BaseModel):
    """Batch send result."""

    msg_ids: List[int]


class MessageResponse(BaseModel):
    """Message object. ``message`` is Any so non-object payloads serialize."""

    msg_id: int
    read_ct: int
    enqueued_at: datetime
    last_read_at: Optional[datetime]
    vt: datetime
    message: Any
    headers: Optional[Dict[str, Any]] = None


class MessagesResponse(BaseModel):
    """Read, poll, and pop envelope. Always a list."""

    messages: List[MessageResponse]


class ReadRequest(BaseModel):
    """Read body. Omitted vt is None so the mixin applies self.vt."""

    vt: Optional[int] = None
    qty: int = 1
    conditional: Optional[Dict[str, Any]] = None


class ReadPollRequest(BaseModel):
    """Long-poll read body."""

    vt: Optional[int] = None
    qty: int = 1
    max_poll_seconds: int = 5
    poll_interval_ms: int = 100
    conditional: Optional[Dict[str, Any]] = None


class PopRequest(BaseModel):
    """Pop body. qty is not a query param."""

    qty: int = 1


class SetVtRequest(BaseModel):
    """Single-id visibility timeout body."""

    vt: Union[int, datetime]


class BatchIdsRequest(BaseModel):
    """Batch delete or archive body."""

    msg_ids: List[int]


def queue_record_to_response(record: QueueRecord) -> QueueRecordResponse:
    """Map a mixin QueueRecord to the HTTP model."""
    return QueueRecordResponse(
        queue_name=record.queue_name,
        is_partitioned=record.is_partitioned,
        is_unlogged=record.is_unlogged,
        created_at=record.created_at,
    )


def message_to_response(msg: Message) -> MessageResponse:
    """Map a mixin Message to the HTTP model."""
    return MessageResponse(
        msg_id=msg.msg_id,
        read_ct=msg.read_ct,
        enqueued_at=msg.enqueued_at,
        last_read_at=msg.last_read_at,
        vt=msg.vt,
        message=msg.message,
        headers=msg.headers,
    )


def queue_metrics_to_response(metrics: QueueMetrics) -> QueueMetricsResponse:
    """Map a mixin QueueMetrics to the HTTP model."""
    return QueueMetricsResponse(
        queue_name=metrics.queue_name,
        queue_length=metrics.queue_length,
        newest_msg_age_sec=metrics.newest_msg_age_sec,
        oldest_msg_age_sec=metrics.oldest_msg_age_sec,
        total_messages=metrics.total_messages,
        scrape_time=metrics.scrape_time,
        queue_visible_length=metrics.queue_visible_length,
    )
