# tests/test_sqlalchemy_unit.py
"""
Unit tests for SQLAlchemy-based PGMQ clients.

These tests use mocks and do NOT require a running PostgreSQL instance.
They verify engine injection, session factories, and Alembic integration patterns.
"""

import unittest
from unittest.mock import MagicMock, patch, AsyncMock

from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine


# ============================================================================
# Sync SQLAlchemy Unit Tests
# ============================================================================


class TestSyncSQLAlchemyQueueUnit(unittest.TestCase):
    """Unit tests for synchronous SQLAlchemy PGMQueue (no DB required)."""

    def _make_queue(self, **kwargs):
        from pgmq.sqlalchemy_queue import PGMQueue

        return PGMQueue(
            host="localhost",
            port="5432",
            username="postgres",
            password="postgres",
            database="postgres",
            init_extension=False,
            **kwargs,
        )

    def test_default_engine_creation(self):
        """When no engine is passed, one should be created internally."""
        with patch(
            "pgmq.sqlalchemy_queue.create_engine", return_value=MagicMock()
        ) as mock_create:
            queue = self._make_queue()
            self.assertIsNotNone(queue.engine)
            mock_create.assert_called_once()

    def test_external_engine_injection(self):
        """When an engine is passed, it should be used directly."""
        mock_engine = MagicMock(spec=Engine)
        queue = self._make_queue(engine=mock_engine)
        self.assertIs(queue.engine, mock_engine)

    def test_invalid_engine_type_raises(self):
        """Passing a non-Engine object should raise TypeError."""
        with self.assertRaises(TypeError) as ctx:
            self._make_queue(engine="not_an_engine")
        self.assertIn("Expected sqlalchemy.Engine", str(ctx.exception))

    def test_session_factory_returns_session(self):
        """session() should return a SQLAlchemy Session bound to the engine."""
        mock_engine = MagicMock(spec=Engine)
        queue = self._make_queue(engine=mock_engine)
        session = queue.session()
        self.assertIsNotNone(session)
        # sessionmaker produces a callable that returns a Session
        self.assertTrue(callable(session) or hasattr(session, "bind"))

    def test_session_raises_without_engine(self):
        """session() should raise RuntimeError if engine is None."""
        queue = self._make_queue()
        queue.engine = None
        with self.assertRaises(RuntimeError) as ctx:
            queue.session()
        self.assertIn("Engine has not been initialized", str(ctx.exception))

    def test_dispose_closes_engine(self):
        """dispose() should call dispose() on the engine and clear it."""
        mock_engine = MagicMock(spec=Engine)
        queue = self._make_queue(engine=mock_engine)
        queue.dispose()
        mock_engine.dispose.assert_called_once()
        self.assertIsNone(queue.engine)

    def test_alembic_op_pattern(self):
        """Simulate the Alembic op.get_bind() integration pattern."""
        mock_bind = MagicMock()
        mock_bind.engine = MagicMock(spec=Engine)

        with patch.object(
            mock_bind.engine, "connect", return_value=MagicMock()
        ) as mock_connect:
            from pgmq.sqlalchemy_queue import PGMQueue

            queue = PGMQueue(engine=mock_bind.engine, init_extension=False)
            # Ensure the queue uses the injected engine
            self.assertIs(queue.engine, mock_bind.engine)
            # A simple operation that touches the engine
            with queue.engine.connect() as conn:
                self.assertIsNotNone(conn)
            mock_connect.assert_called_once()


# ============================================================================
# Async SQLAlchemy Unit Tests
# ============================================================================


class TestAsyncSQLAlchemyQueueUnit(unittest.IsolatedAsyncioTestCase):
    """Unit tests for asynchronous SQLAlchemy PGMQueue (no DB required)."""

    def _make_queue(self, **kwargs):
        from pgmq.sqlalchemy_async_queue import PGMQueue

        return PGMQueue(
            host="localhost",
            port="5432",
            username="postgres",
            password="postgres",
            database="postgres",
            init_extension=False,
            **kwargs,
        )

    def test_default_engine_creation_on_init(self):
        """When no engine is passed, init() should create an AsyncEngine."""
        queue = self._make_queue()
        with patch(
            "pgmq.sqlalchemy_async_queue.create_async_engine",
            return_value=AsyncMock(),
        ) as mock_create:
            # run init synchronously in the test (it's async, but we can inspect)
            import asyncio

            asyncio.run(queue.init())
            mock_create.assert_called_once()
            self.assertIsNotNone(queue.engine)

    def test_external_async_engine_injection(self):
        """When an AsyncEngine is passed, init() should use it."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        queue = self._make_queue(engine=mock_engine)
        import asyncio

        asyncio.run(queue.init())
        self.assertIs(queue.engine, mock_engine)

    def test_invalid_async_engine_type_raises(self):
        """Passing a non-AsyncEngine object should raise TypeError."""
        queue = self._make_queue(engine="not_an_engine")
        import asyncio

        with self.assertRaises(TypeError) as ctx:
            asyncio.run(queue.init())
        self.assertIn("Expected sqlalchemy.ext.asyncio.AsyncEngine", str(ctx.exception))

    def test_async_session_factory(self):
        """session() should return an async_sessionmaker bound to the engine."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_engine.connect = AsyncMock()
        queue = self._make_queue(engine=mock_engine)
        import asyncio

        asyncio.run(queue.init())
        session_maker = queue.session()
        self.assertTrue(callable(session_maker))

    def test_async_session_raises_without_engine(self):
        """session() should raise RuntimeError if engine is None."""
        queue = self._make_queue()
        queue.engine = None
        with self.assertRaises(RuntimeError) as ctx:
            queue.session()
        self.assertIn("Engine has not been initialized", str(ctx.exception))

    async def test_close_disposes_engine(self):
        """close() should dispose the async engine and clear it."""
        mock_engine = AsyncMock(spec=AsyncEngine)
        mock_engine.connect = AsyncMock()
        queue = self._make_queue(engine=mock_engine)
        await queue.init()
        await queue.close()
        mock_engine.dispose.assert_awaited_once()
        self.assertIsNone(queue.engine)


# ============================================================================
# Import / Export Unit Tests
# ============================================================================


class TestSQLAlchemyImports(unittest.TestCase):
    """Verify that SQLAlchemy queue classes are exported correctly."""

    def test_sync_exported_from_package(self):
        from pgmq import SQLAlchemyPGMQueue

        self.assertIsNotNone(SQLAlchemyPGMQueue)

    def test_async_exported_from_package(self):
        from pgmq import SQLAlchemyAsyncPGMQueue

        self.assertIsNotNone(SQLAlchemyAsyncPGMQueue)


if __name__ == "__main__":
    unittest.main()
