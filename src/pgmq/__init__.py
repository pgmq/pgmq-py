# src/pgmq/__init__.py
"""
PGMQ Python Client - A Python client for the PGMQ PostgreSQL extension.

This package provides both synchronous and asynchronous clients for interacting
with PGMQ (Postgres Message Queue) functionality. It also provides SQLAlchemy-based
clients for users who prefer to use SQLAlchemy for database connections.
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

# Client classes (default psycopg-based)
from pgmq.queue import PGMQueue as SyncPGMQueue
from pgmq.async_queue import PGMQueue as AsyncPGMQueue

# SQLAlchemy-based clients (available when sqlalchemy is installed)
try:
    from pgmq.sqlalchemy_queue import PGMQueue as SQLAlchemyPGMQueue
    from pgmq.sqlalchemy_async_queue import PGMQueue as SQLAlchemyAsyncPGMQueue
except ImportError:  # pragma: no cover
    SQLAlchemyPGMQueue = None  # type: ignore[misc, assignment]
    SQLAlchemyAsyncPGMQueue = None  # type: ignore[misc, assignment]

# Decorators
from pgmq.decorators import (
    transaction,
    async_transaction,
    sqlalchemy_transaction,
    sqlalchemy_async_transaction,
)

# Logging utilities
from pgmq.logger import PGMQLogger, create_logger, log_performance

# Backward compatibility: PGMQueue points to sync version
PGMQueue = SyncPGMQueue

__version__ = "2.0.0"
__all__ = [
    # Clients (psycopg-based)
    "PGMQueue",  # Sync (backward compatible alias)
    "SyncPGMQueue",  # Explicit sync
    "AsyncPGMQueue",  # Async (clear naming)
    # Clients (sqlalchemy-based)
    "SQLAlchemyPGMQueue",
    "SQLAlchemyAsyncPGMQueue",
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
    "sqlalchemy_transaction",
    "sqlalchemy_async_transaction",
    # Logging
    "PGMQLogger",
    "create_logger",
    "log_performance",
    # Version
    "__version__",
]
