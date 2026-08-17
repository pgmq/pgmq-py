# src/pgmq/api/routes.py
"""HTTP route handlers for the PGMQ API."""

from typing import Any, Dict
import asyncio
import logging

from fastapi import APIRouter
from starlette.responses import Response

from pgmq import __version__
from pgmq.api.config import PGMQAPIConfig
from pgmq.api.factory import (
    APIError,
    call_queue,
    ensure_initialized,
    ping_queue,
    route_guard,
)
from pgmq.api.schemas import (
    BatchIdsRequest,
    CreateQueueResponse,
    HealthResponse,
    MessageResponse,
    MessagesResponse,
    MetricsListResponse,
    PopRequest,
    PurgeResponse,
    QueueMetricsResponse,
    QueuesResponse,
    ReadPollRequest,
    ReadRequest,
    ReadyResponse,
    SendBatchRequest,
    SendBatchResponse,
    SendRequest,
    SendResponse,
    SetVtRequest,
    message_to_response,
    queue_metrics_to_response,
    queue_record_to_response,
)
from pgmq.logger import LoggingManager, log_with_context

logger = LoggingManager.get_logger("pgmq.api")


def register_public_routes(
    router: APIRouter,
    *,
    queue: Any,
    api_config: PGMQAPIConfig,
    init_lock: asyncio.Lock,
    init_ok: Dict[str, bool],
    own_queue: bool,
    auth_status: str,
) -> None:
    """Register public /health and /ready routes. No API key required."""

    @router.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Return process liveness. Do not touch the database or call init."""
        return HealthResponse(
            status="ok",
            version=__version__,
            auth=auth_status,
        )

    @router.get("/ready", response_model=ReadyResponse)
    @route_guard
    async def ready() -> ReadyResponse:
        """Initialize the client if needed and ping the database."""
        from pgmq.api.auth import ensure_auth_table

        await ensure_initialized(queue, init_lock, init_ok, own_queue)
        await ping_queue(queue)
        await ensure_auth_table(queue, api_config)
        return ReadyResponse(status="ok")


def register_protected_routes(
    router: APIRouter,
    *,
    queue: Any,
    api_config: PGMQAPIConfig,
    init_lock: asyncio.Lock,
    init_ok: Dict[str, bool],
    own_queue: bool,
) -> None:
    """Register queue, send, and metrics routes. Auth lives on the router."""
    client = queue

    async def _ready() -> None:
        await ensure_initialized(client, init_lock, init_ok, own_queue)

    @router.put("/queues/{queue}", response_model=CreateQueueResponse, status_code=201)
    @route_guard
    async def put_queue(queue: str, unlogged: bool = False) -> CreateQueueResponse:
        """Ensure a queue exists. Always return 201. Echo the unlogged flag."""
        await _ready()
        log_with_context(
            logger,
            logging.DEBUG,
            "HTTP create queue",
            queue=queue,
            unlogged=unlogged,
        )
        await call_queue(client.create_queue, queue, unlogged=unlogged)
        return CreateQueueResponse(queue_name=queue, unlogged=unlogged)

    @router.get("/queues", response_model=QueuesResponse)
    @route_guard
    async def list_queues() -> QueuesResponse:
        """List queues. The mixin may emit a UserWarning."""
        await _ready()
        records = await call_queue(client.list_queues)
        return QueuesResponse(
            queues=[queue_record_to_response(item) for item in records]
        )

    @router.delete("/queues/{queue}", status_code=204)
    @route_guard
    async def drop_queue(queue: str) -> Response:
        """Drop a queue. Return 204 if dropped, 404 if missing."""
        await _ready()
        dropped = await call_queue(client.drop_queue, queue)
        if not dropped:
            raise APIError(404, "queue_not_found", f"Queue '{queue}' does not exist")
        return Response(status_code=204)

    @router.post("/queues/{queue}/purge", response_model=PurgeResponse)
    @route_guard
    async def purge_queue(queue: str) -> PurgeResponse:
        """Purge all messages from a queue."""
        await _ready()
        purged = await call_queue(client.purge, queue)
        return PurgeResponse(purged=int(purged))

    @router.get("/metrics", response_model=MetricsListResponse)
    @route_guard
    async def metrics_all() -> MetricsListResponse:
        """Return metrics for all queues."""
        await _ready()
        rows = await call_queue(client.metrics_all)
        return MetricsListResponse(
            queues=[queue_metrics_to_response(item) for item in rows]
        )

    @router.get("/queues/{queue}/metrics", response_model=QueueMetricsResponse)
    @route_guard
    async def queue_metrics(queue: str) -> QueueMetricsResponse:
        """Return metrics for one queue. Missing queue is 404."""
        await _ready()
        stats = await call_queue(client.metrics, queue)
        return queue_metrics_to_response(stats)

    @router.post(
        "/queues/{queue}/messages",
        response_model=SendResponse,
        status_code=201,
    )
    @route_guard
    async def send_message(queue: str, body: SendRequest) -> SendResponse:
        """Send one message. Do not expose the legacy tz alias."""
        await _ready()
        log_with_context(logger, logging.DEBUG, "HTTP send", queue=queue)
        msg_id = await call_queue(
            client.send,
            queue,
            body.message,
            headers=body.headers,
            delay=body.delay,
        )
        if msg_id == -1:
            raise APIError(500, "send_failed", "send returned no message id")
        return SendResponse(msg_id=int(msg_id))

    @router.post(
        "/queues/{queue}/messages/batch",
        response_model=SendBatchResponse,
        status_code=201,
    )
    @route_guard
    async def send_batch(queue: str, body: SendBatchRequest) -> SendBatchResponse:
        """Send a batch. Reject over-cap length before the mixin runs."""
        await _ready()
        if len(body.messages) > api_config.max_batch:
            raise APIError(
                400,
                "invalid_request",
                f"batch length exceeds {api_config.max_batch}",
            )
        log_with_context(
            logger,
            logging.DEBUG,
            "HTTP send batch",
            queue=queue,
            count=len(body.messages),
        )
        msg_ids = await call_queue(
            client.send_batch,
            queue,
            body.messages,
            headers=body.headers,
            delay=body.delay,
        )
        return SendBatchResponse(msg_ids=[int(item) for item in msg_ids])

    def _reject_qty(qty: int) -> None:
        if qty > api_config.max_qty:
            raise APIError(
                400,
                "invalid_request",
                f"qty exceeds {api_config.max_qty}",
            )

    def _wrap_messages(result: Any) -> MessagesResponse:
        if result is None:
            items = []
        elif isinstance(result, list):
            items = result
        else:
            items = [result]
        return MessagesResponse(messages=[message_to_response(item) for item in items])

    @router.post("/queues/{queue}/read", response_model=MessagesResponse)
    @route_guard
    async def read_messages(queue: str, body: ReadRequest) -> MessagesResponse:
        """Read messages. Always return a list, including empty."""
        await _ready()
        _reject_qty(body.qty)
        log_with_context(logger, logging.DEBUG, "HTTP read", queue=queue, qty=body.qty)
        result = await call_queue(
            client.read,
            queue,
            vt=body.vt,
            qty=body.qty,
            conditional=body.conditional,
        )
        return _wrap_messages(result)

    @router.post("/queues/{queue}/read-poll", response_model=MessagesResponse)
    @route_guard
    async def read_poll(queue: str, body: ReadPollRequest) -> MessagesResponse:
        """Long-poll read. Reject over-cap poll seconds. Always return a list."""
        await _ready()
        _reject_qty(body.qty)
        if body.max_poll_seconds > api_config.max_poll_seconds:
            raise APIError(
                400,
                "poll_limit_exceeded",
                f"max_poll_seconds exceeds {api_config.max_poll_seconds}",
            )
        log_with_context(
            logger,
            logging.DEBUG,
            "HTTP read-poll",
            queue=queue,
            qty=body.qty,
            max_poll_seconds=body.max_poll_seconds,
        )
        result = await call_queue(
            client.read_with_poll,
            queue,
            vt=body.vt,
            qty=body.qty,
            max_poll_seconds=body.max_poll_seconds,
            poll_interval_ms=body.poll_interval_ms,
            conditional=body.conditional,
        )
        return _wrap_messages(result)

    @router.post("/queues/{queue}/pop", response_model=MessagesResponse)
    @route_guard
    async def pop_messages(queue: str, body: PopRequest) -> MessagesResponse:
        """Pop messages. Always return a list, including empty."""
        await _ready()
        _reject_qty(body.qty)
        result = await call_queue(client.pop, queue, qty=body.qty)
        return _wrap_messages(result)

    @router.delete("/queues/{queue}/messages/{msg_id}", status_code=204)
    @route_guard
    async def delete_message(queue: str, msg_id: int) -> Response:
        """Delete one message. Missing id is 404."""
        await _ready()
        deleted = await call_queue(client.delete, queue, msg_id)
        if not deleted:
            raise APIError(404, "message_not_found", f"Message {msg_id} does not exist")
        return Response(status_code=204)

    @router.post("/queues/{queue}/messages/delete", response_model=SendBatchResponse)
    @route_guard
    async def delete_messages(queue: str, body: BatchIdsRequest) -> SendBatchResponse:
        """Delete a batch. Return ids that were deleted."""
        await _ready()
        ids = await call_queue(client.delete_batch, queue, body.msg_ids)
        return SendBatchResponse(msg_ids=[int(item) for item in ids])

    @router.post("/queues/{queue}/messages/{msg_id}/archive", status_code=204)
    @route_guard
    async def archive_message(queue: str, msg_id: int) -> Response:
        """Archive one message. Missing id is 404."""
        await _ready()
        archived = await call_queue(client.archive, queue, msg_id)
        if not archived:
            raise APIError(404, "message_not_found", f"Message {msg_id} does not exist")
        return Response(status_code=204)

    @router.post("/queues/{queue}/messages/archive", response_model=SendBatchResponse)
    @route_guard
    async def archive_messages(queue: str, body: BatchIdsRequest) -> SendBatchResponse:
        """Archive a batch. Return ids that were archived."""
        await _ready()
        ids = await call_queue(client.archive_batch, queue, body.msg_ids)
        return SendBatchResponse(msg_ids=[int(item) for item in ids])

    @router.post(
        "/queues/{queue}/messages/{msg_id}/vt",
        response_model=MessageResponse,
    )
    @route_guard
    async def set_visibility(
        queue: str, msg_id: int, body: SetVtRequest
    ) -> MessageResponse:
        """Set visibility timeout for one message."""
        await _ready()
        result = await call_queue(client.set_vt, queue, msg_id, body.vt)
        if result is None:
            raise APIError(404, "message_not_found", f"Message {msg_id} does not exist")
        if isinstance(result, list):
            if not result:
                raise APIError(
                    404, "message_not_found", f"Message {msg_id} does not exist"
                )
            result = result[0]
        return message_to_response(result)
