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

try:
    from pgmq.async_queue import PGMQueue as AsyncPGMQueue
except ImportError:  # pragma: no cover
    AsyncPGMQueue = None  # type: ignore[misc, assignment]

# SQLAlchemy-based clients (available when sqlalchemy is installed)
try:
    from pgmq.sqlalchemy_queue import PGMQueue as SQLAlchemyPGMQueue
except ImportError:  # pragma: no cover
    SQLAlchemyPGMQueue = None  # type: ignore[misc, assignment]

try:
    from pgmq.sqlalchemy_async_queue import PGMQueue as SQLAlchemyAsyncPGMQueue
except ImportError:  # pragma: no cover
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

# SQL installation utilities
from pgmq.install import (
    PGMQInstallError,
    build_install_sql_url,
    get_install_sql,
    get_latest_release_tag,
    install_pgmq_from_sql,
    install_pgmq_from_sql_async,
    install_pgmq_sql,
)

# Backward compatibility: PGMQueue points to sync version
PGMQueue = SyncPGMQueue

try:
    from importlib.metadata import version

    __version__ = version("pgmq")
except Exception:  # pragma: no cover
    __version__ = "unknown"
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
    # SQL installation
    "PGMQInstallError",
    "build_install_sql_url",
    "get_install_sql",
    "get_latest_release_tag",
    "install_pgmq_from_sql",
    "install_pgmq_from_sql_async",
    "install_pgmq_sql",
    # Version
    "__version__",
]
