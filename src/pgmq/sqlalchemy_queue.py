# src/pgmq/sqlalchemy_queue.py
"""
Synchronous PGMQ client implementation using SQLAlchemy.

This module provides the SQLAlchemyPGMQueue class for synchronous database operations
using SQLAlchemy Core, with full support for all PGMQ extension features including
topics, FIFO, and notifications.
"""

from dataclasses import dataclass, field, fields
from typing import Optional, List, Dict, Any
import logging

from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from pgmq.base import BaseQueue
from pgmq._client_fields import PGMQueueClientFields
from pgmq import _sql
from pgmq.decorators import sqlalchemy_transaction
from pgmq.logger import log_with_context
from pgmq.sync_operations import SyncPGMQueueOperationsMixin


def _parse_jsonb(val) -> Any:
    """Parse JSONB value from SQLAlchemy/psycopg result."""
    if val is None:
        return None
    return val


class SQLAlchemySyncBackend:
    """SQLAlchemy engine and SQL execution for the sync client."""

    _transaction_decorator = sqlalchemy_transaction

    def _encode_jsonb(self, value: Dict[str, Any]) -> Dict[str, Any]:
        return value

    def _encode_jsonb_list(self, values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return values

    @property
    def _json_parser(self):
        return _parse_jsonb

    def _init_engine(self) -> None:
        """Initialize the SQLAlchemy engine."""
        log_with_context(self.logger, logging.DEBUG, "Creating SQLAlchemy engine")
        connection_url = self.config.async_dsn.replace(
            "postgresql://", "postgresql+psycopg://", 1
        )
        self.engine = create_engine(
            connection_url,
            poolclass=QueuePool,
            pool_size=self.config.pool_size,
            max_overflow=20,
            pool_pre_ping=True,
        )

    def _init_extensions(self) -> None:
        """Ensure PGMQ extension is installed."""
        with self.engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;"))
            conn.commit()

    def _execute(self, sql: str, params: Optional[tuple] = None, conn=None) -> None:
        """Execute SQL without returning results."""
        converted_sql, param_dict = _sql.convert_sql_params(sql, params)

        def run_query(connection):
            if param_dict:
                connection.execute(text(converted_sql), param_dict)
            else:
                connection.execute(text(converted_sql))

        if conn is not None:
            run_query(conn)
        else:
            with self.engine.begin() as new_conn:
                run_query(new_conn)

    def _execute_with_result(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> List[tuple]:
        """Execute SQL and return all results."""
        converted_sql, param_dict = _sql.convert_sql_params(sql, params)

        def run_query(connection):
            if param_dict:
                return connection.execute(text(converted_sql), param_dict)
            else:
                return connection.execute(text(converted_sql))

        if conn is not None:
            return run_query(conn).fetchall()
        else:
            with self.engine.begin() as new_conn:
                return run_query(new_conn).fetchall()

    def _execute_one(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> Optional[tuple]:
        """Execute SQL and return first result or None."""
        results = self._execute_with_result(sql, params, conn)
        return results[0] if results else None


@dataclass
class PGMQueue(
    PGMQueueClientFields,
    SQLAlchemySyncBackend,
    SyncPGMQueueOperationsMixin,
    BaseQueue,
):
    """
    Synchronous PGMQueue client using SQLAlchemy for PostgreSQL Message Queue operations.
    """

    engine: Optional[Engine] = field(default=None)  # type: ignore

    def __post_init__(self):
        """Initialize configuration and engine after dataclass construction."""
        BaseQueue.__init__(
            self,
            **{f.name: getattr(self, f.name) for f in fields(PGMQueueClientFields)},
        )
        if self.engine is None:
            self._init_engine()
            if self.config.init_extension:
                self._init_extensions()
        else:
            if not isinstance(self.engine, Engine):
                raise TypeError(
                    f"Expected sqlalchemy.Engine, got {type(self.engine).__name__}"
                )
            if self.config.init_extension:
                self._init_extensions()
        self._session_factory = sessionmaker(bind=self.engine)

    def session(self) -> Session:
        """Return a new SQLAlchemy ORM Session bound to this queue's engine."""
        if self.engine is None:
            raise RuntimeError("Engine has not been initialized.")
        return self._session_factory()

    def dispose(self) -> None:
        """Dispose of the engine and close all connections."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
