"""REST API for interacting with PGMQ queues."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from pgmq.messages import Message, QueueMetrics
from pgmq.queue import PGMQueue


class SendMessagePayload(BaseModel):
    """Payload for sending a single message."""

    message: Dict[str, Any] = Field(..., description="Arbitrary JSON payload to enqueue")
    delay: Optional[int] = Field(
        default=None,
        ge=0,
        description="Optional delay (in seconds) before the message becomes visible.",
    )
    tz: Optional[datetime] = Field(
        default=None,
        description="Optional timestamp when the message becomes visible.",
    )


class SendBatchPayload(BaseModel):
    """Payload for sending a batch of messages."""

    messages: List[Dict[str, Any]] = Field(
        ..., description="List of JSON payloads to enqueue as a batch"
    )
    delay: Optional[int] = Field(
        default=None,
        ge=0,
        description="Optional delay (in seconds) applied to the batch before visibility.",
    )
    tz: Optional[datetime] = Field(
        default=None,
        description="Optional timestamp when the batch becomes visible.",
    )


class ReadMessagesPayload(BaseModel):
    """Payload for reading messages from a queue."""

    batch_size: int = Field(
        default=1,
        ge=1,
        le=1024,
        description="Number of messages to retrieve in a single request.",
    )
    vt: Optional[int] = Field(
        default=None,
        ge=0,
        description="Optional override of the queue's default visibility timeout (seconds).",
    )


class DeleteBatchPayload(BaseModel):
    """Payload for deleting multiple messages."""

    msg_ids: List[int] = Field(..., description="List of message ids to delete from the queue.")


class ArchiveBatchPayload(BaseModel):
    """Payload for archiving multiple messages."""

    msg_ids: List[int] = Field(..., description="List of message ids to archive from the queue.")


def _serialize_message(message: Message) -> Dict[str, Any]:
    return {
        "msg_id": message.msg_id,
        "read_ct": message.read_ct,
        "enqueued_at": message.enqueued_at.isoformat(),
        "vt": message.vt.isoformat(),
        "message": message.message,
    }


def _serialize_metrics(metrics: QueueMetrics) -> Dict[str, Any]:
    return {
        "queue_name": metrics.queue_name,
        "queue_length": metrics.queue_length,
        "newest_msg_age_sec": metrics.newest_msg_age_sec,
        "oldest_msg_age_sec": metrics.oldest_msg_age_sec,
        "total_messages": metrics.total_messages,
        "scrape_time": metrics.scrape_time.isoformat(),
    }


def _get_queue(request: Request) -> PGMQueue:
    queue: PGMQueue = request.app.state.queue
    return queue


def create_app(queue: Optional[PGMQueue] = None) -> FastAPI:
    """Create a FastAPI application configured to interact with a PGMQ queue."""

    app = FastAPI(
        title="PGMQ API",
        version="1.0.0",
        description="REST API for interacting with the PGMQ extension.",
    )

    app.state.queue = queue or PGMQueue()

    @app.get("/queues", summary="List available queues")
    def list_queues(request: Request) -> Dict[str, List[str]]:
        try:
            queues = _get_queue(request).list_queues()
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"queues": queues}

    @app.post("/queues/{queue_name}/messages", summary="Enqueue a message")
    def send_message(
        queue_name: str, payload: SendMessagePayload, request: Request
    ) -> Dict[str, int]:
        queue = _get_queue(request)
        try:
            msg_id = queue.send(queue_name, payload.message, delay=payload.delay or 0, tz=payload.tz)
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"msg_id": msg_id}

    @app.post("/queues/{queue_name}/messages/batch", summary="Enqueue a batch of messages")
    def send_batch(
        queue_name: str, payload: SendBatchPayload, request: Request
    ) -> Dict[str, List[int]]:
        queue = _get_queue(request)
        try:
            msg_ids = queue.send_batch(
                queue_name,
                payload.messages,
                delay=payload.delay or 0,
                tz=payload.tz,
            )
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"msg_ids": msg_ids}

    @app.post(
        "/queues/{queue_name}/messages/read",
        summary="Read (peek) messages from a queue",
    )
    def read_messages(
        queue_name: str, payload: ReadMessagesPayload, request: Request
    ) -> Dict[str, List[Dict[str, Any]]]:
        queue = _get_queue(request)
        try:
            messages = queue.read_batch(
                queue_name,
                vt=payload.vt,
                batch_size=payload.batch_size,
            )
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        serialized = [] if not messages else [_serialize_message(message) for message in messages]
        return {"messages": serialized}

    @app.post(
        "/queues/{queue_name}/messages/pop",
        summary="Pop a message from a queue",
    )
    def pop_message(queue_name: str, request: Request) -> Dict[str, Any]:
        queue = _get_queue(request)
        try:
            message = queue.pop(queue_name)
        except IndexError:
            raise HTTPException(status_code=404, detail="No messages available to pop")
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"message": _serialize_message(message)}

    @app.delete(
        "/queues/{queue_name}/messages/{msg_id}",
        summary="Delete a single message by id",
    )
    def delete_message(queue_name: str, msg_id: int, request: Request) -> Dict[str, bool]:
        queue = _get_queue(request)
        try:
            deleted = queue.delete(queue_name, msg_id)
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not deleted:
            raise HTTPException(status_code=404, detail="Message not found")
        return {"deleted": True}

    @app.delete(
        "/queues/{queue_name}/messages",
        summary="Delete a batch of messages by id",
    )
    def delete_batch(
        queue_name: str, payload: DeleteBatchPayload, request: Request
    ) -> Dict[str, List[int]]:
        queue = _get_queue(request)
        try:
            deleted = queue.delete_batch(queue_name, payload.msg_ids)
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"deleted": deleted}

    @app.post(
        "/queues/{queue_name}/archive/{msg_id}",
        summary="Archive a single message by id",
    )
    def archive_message(
        queue_name: str, msg_id: int, request: Request
    ) -> Dict[str, bool]:
        queue = _get_queue(request)
        try:
            archived = queue.archive(queue_name, msg_id)
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not archived:
            raise HTTPException(status_code=404, detail="Message not found")
        return {"archived": True}

    @app.post(
        "/queues/{queue_name}/archive/batch",
        summary="Archive a batch of messages",
    )
    def archive_batch(
        queue_name: str, payload: ArchiveBatchPayload, request: Request
    ) -> Dict[str, List[int]]:
        queue = _get_queue(request)
        try:
            archived = queue.archive_batch(queue_name, payload.msg_ids)
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"archived": archived}

    @app.get(
        "/queues/{queue_name}/metrics",
        summary="Retrieve metrics for a specific queue",
    )
    def queue_metrics(queue_name: str, request: Request) -> Dict[str, Any]:
        queue = _get_queue(request)
        try:
            metrics = queue.metrics(queue_name)
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"metrics": _serialize_metrics(metrics)}

    @app.get("/queues/metrics", summary="Retrieve metrics for all queues")
    def all_queue_metrics(request: Request) -> Dict[str, List[Dict[str, Any]]]:
        queue = _get_queue(request)
        try:
            metrics = queue.metrics_all()
        except Exception as exc:  # pragma: no cover - unexpected errors
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"metrics": [_serialize_metrics(item) for item in metrics]}

    return app


__all__ = ["create_app"]
