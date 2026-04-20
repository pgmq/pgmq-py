# src/pgmq/__init__.py
"""
PGMQ Python Client - A Python client for the PGMQ PostgreSQL extension.

This package provides both synchronous and asynchronous clients for interacting
with PGMQ (Postgres Message Queue) functionality.
"""

# Core message types
from pgmq.messages import (
    Message,
    QueueMetrics,
    QueueRecord,
    TopicBinding,
    RoutingResult,
    NotificationThrottle,
)

# Client classes
from pgmq.queue import PGMQueue as SyncPGMQueue
from pgmq.async_queue import PGMQueue as AsyncPGMQueue

# Decorators
from pgmq.decorators import transaction, async_transaction

# Logging utilities
from pgmq.logger import PGMQLogger, create_logger, log_performance

# Backward compatibility: PGMQueue points to sync version
PGMQueue = SyncPGMQueue

__version__ = "2.0.0"
__all__ = [
    # Clients
    "PGMQueue",  # Sync (backward compatible alias)
    "SyncPGMQueue",  # Explicit sync
    "AsyncPGMQueue",  # Async (clear naming)
    # Data classes
    "Message",
    "QueueMetrics",
    "QueueRecord",
    "TopicBinding",
    "RoutingResult",
    "NotificationThrottle",
    # Decorators
    "transaction",
    "async_transaction",
    # Logging
    "PGMQLogger",
    "create_logger",
    "log_performance",
    # Version
    "__version__",
]
