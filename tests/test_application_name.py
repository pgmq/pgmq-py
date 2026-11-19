"""Tests for optional PostgreSQL application_name on client connections."""

import inspect
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg

from pgmq.async_queue import PGMQueue as AsyncPGMQueue
from pgmq.base import PGMQConfig
from pgmq.install import install_pgmq_sql
from pgmq.notify_listener import AsyncNotificationListener, SyncNotificationListener
from pgmq.queue import PGMQueue
from pgmq.sqlalchemy_async_queue import PGMQueue as SQLAlchemyAsyncPGMQueue
from pgmq.sqlalchemy_queue import PGMQueue as SQLAlchemyPGMQueue
from tests.utils import (
    PG_DATABASE,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USERNAME,
)


def _offline_kwargs(**overrides):
    values = {
        "conn_string": None,
        "host": "localhost",
        "port": "5432",
        "database": "postgres",
        "username": "postgres",
        "password": "postgres",
        "init_extension": False,
    }
    values.update(overrides)
    return values


def _pg_kwargs(**overrides):
    values = _offline_kwargs(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        username=PG_USERNAME,
        password=PG_PASSWORD,
        application_name="worker/pgmq",
    )
    values.update(overrides)
    return values


def _positional_names(cls):
    return [
        name
        for name, param in inspect.signature(cls.__init__).parameters.items()
        if name != "self" and param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    ]


def _dsn() -> str:
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DATABASE} "
        f"user={PG_USERNAME} password={PG_PASSWORD}"
    )


def _postgres_available() -> bool:
    try:
        conn = psycopg.connect(_dsn(), connect_timeout=2)
    except Exception:
        return False
    conn.close()
    return True


class TestApplicationNameConfig(unittest.TestCase):
    def test_default_is_none(self):
        config = PGMQConfig()
        self.assertIsNone(config.application_name)
        self.assertNotIn("application_name", config.dsn)
        self.assertNotIn("application_name", config.async_dsn)

    def test_stores_name(self):
        config = PGMQConfig(application_name="worker/pgmq")
        self.assertEqual(config.application_name, "worker/pgmq")
        config = PGMQConfig(application_name="worker pgmq")
        self.assertEqual(config.application_name, "worker pgmq")
        self.assertNotIn("application_name", config.dsn)
        self.assertNotIn("application_name", config.async_dsn)

    def test_blank_or_non_string_raises(self):
        for bad in ("", "   ", 7):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    PGMQConfig(application_name=bad)


class TestSyncApplicationName(unittest.TestCase):
    def test_pool_stays_positional(self):
        pool = MagicMock()
        queue = PGMQueue(
            None,
            "localhost",
            "5432",
            "postgres",
            "postgres",
            "postgres",
            0,
            30,
            10,
            False,
            None,
            False,
            False,
            False,
            "10 MB",
            "1 week",
            pool,
        )
        self.assertIs(queue.pool, pool)
        self.assertFalse(queue._own_pool)
        self.assertIsNone(queue.application_name)
        self.assertIsNone(queue.config.application_name)
        self.assertEqual(_positional_names(PGMQueue)[-1], "pool")
        self.assertEqual(
            inspect.signature(PGMQueue.__init__).parameters["application_name"].kind,
            inspect.Parameter.KEYWORD_ONLY,
        )

    def test_connection_pool_kwargs_only_when_set(self):
        with patch("pgmq.queue.ConnectionPool") as mock_pool:
            mock_pool.return_value = MagicMock()
            PGMQueue(**_offline_kwargs(application_name="worker/pgmq"))
            self.assertEqual(
                mock_pool.call_args.kwargs["kwargs"],
                {"application_name": "worker/pgmq"},
            )
            self.assertNotIn("application_name", mock_pool.call_args.args[0])

        with patch("pgmq.queue.ConnectionPool") as mock_pool:
            mock_pool.return_value = MagicMock()
            PGMQueue(**_offline_kwargs())
            self.assertNotIn("kwargs", mock_pool.call_args.kwargs)

    def test_supplied_pool_is_kept(self):
        pool = MagicMock()
        with patch("pgmq.queue.ConnectionPool") as mock_pool:
            queue = PGMQueue(
                **_offline_kwargs(pool=pool, application_name="worker/pgmq")
            )
            mock_pool.assert_not_called()
            self.assertIs(queue.pool, pool)


class TestAsyncApplicationName(unittest.IsolatedAsyncioTestCase):
    async def test_create_pool_server_settings_only_when_set(self):
        with patch(
            "pgmq.async_queue.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create:
            mock_create.return_value = MagicMock()
            queue = AsyncPGMQueue(**_offline_kwargs(application_name="worker/pgmq"))
            await queue.init()
            self.assertEqual(
                mock_create.call_args.kwargs["server_settings"],
                {"application_name": "worker/pgmq"},
            )

        with patch(
            "pgmq.async_queue.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create:
            mock_create.return_value = MagicMock()
            queue = AsyncPGMQueue(**_offline_kwargs())
            await queue.init()
            self.assertNotIn("server_settings", mock_create.call_args.kwargs)

    async def test_supplied_pool_is_kept(self):
        pool = MagicMock()
        with patch(
            "pgmq.async_queue.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create:
            queue = AsyncPGMQueue(
                **_offline_kwargs(pool=pool, application_name="worker/pgmq")
            )
            await queue.init()
            mock_create.assert_not_called()
            self.assertIs(queue.pool, pool)


class TestSQLAlchemyApplicationName(unittest.TestCase):
    def test_create_engine_connect_args_only_when_set(self):
        with patch("pgmq.sqlalchemy_queue.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            SQLAlchemyPGMQueue(**_offline_kwargs(application_name="worker/pgmq"))
            self.assertEqual(
                mock_create.call_args.kwargs["connect_args"],
                {"application_name": "worker/pgmq"},
            )

        with patch("pgmq.sqlalchemy_queue.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            SQLAlchemyPGMQueue(**_offline_kwargs())
            self.assertNotIn("connect_args", mock_create.call_args.kwargs)

    def test_supplied_engine_is_kept(self):
        from sqlalchemy import create_engine

        engine = create_engine(
            "postgresql+psycopg://postgres:postgres@127.0.0.1/postgres"
        )
        try:
            with patch("pgmq.sqlalchemy_queue.create_engine") as mock_create:
                queue = SQLAlchemyPGMQueue(
                    **_offline_kwargs(engine=engine, application_name="worker/pgmq")
                )
                mock_create.assert_not_called()
                self.assertIs(queue.engine, engine)
        finally:
            engine.dispose()


class TestSQLAlchemyAsyncApplicationName(unittest.IsolatedAsyncioTestCase):
    async def test_create_async_engine_server_settings_only_when_set(self):
        with patch("pgmq.sqlalchemy_async_queue.create_async_engine") as mock_create:
            mock_create.return_value = MagicMock()
            queue = SQLAlchemyAsyncPGMQueue(
                **_offline_kwargs(application_name="worker/pgmq")
            )
            await queue.init()
            self.assertEqual(
                mock_create.call_args.kwargs["connect_args"],
                {"server_settings": {"application_name": "worker/pgmq"}},
            )

        with patch("pgmq.sqlalchemy_async_queue.create_async_engine") as mock_create:
            mock_create.return_value = MagicMock()
            queue = SQLAlchemyAsyncPGMQueue(**_offline_kwargs())
            await queue.init()
            self.assertNotIn("connect_args", mock_create.call_args.kwargs)

    async def test_supplied_engine_is_kept(self):
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(
            "postgresql+asyncpg://postgres:postgres@127.0.0.1/postgres"
        )
        try:
            with patch(
                "pgmq.sqlalchemy_async_queue.create_async_engine"
            ) as mock_create:
                queue = SQLAlchemyAsyncPGMQueue(
                    **_offline_kwargs(engine=engine, application_name="worker/pgmq")
                )
                await queue.init()
                mock_create.assert_not_called()
                self.assertIs(queue.engine, engine)
        finally:
            await engine.dispose()


class TestListenerApplicationName(unittest.IsolatedAsyncioTestCase):
    def test_sync_connect_receives_name_only_when_set(self):
        queue = PGMQueue(
            **_offline_kwargs(pool=MagicMock(), application_name="worker/pgmq")
        )
        listener = SyncNotificationListener(queue)
        mock_conn = MagicMock()
        mock_conn.notifies.return_value = []
        with patch(
            "pgmq.notify_listener.psycopg.connect", return_value=mock_conn
        ) as mock_connect:
            listener.listen("demo", lambda _payload: None, timeout=0.01)
        self.assertEqual(
            mock_connect.call_args.kwargs["application_name"], "worker/pgmq"
        )
        self.assertEqual(mock_connect.call_args.kwargs["autocommit"], True)
        self.assertNotIn("application_name", mock_connect.call_args.args[0])

        queue = PGMQueue(**_offline_kwargs(pool=MagicMock()))
        listener = SyncNotificationListener(queue)
        mock_conn = MagicMock()
        mock_conn.notifies.return_value = []
        with patch(
            "pgmq.notify_listener.psycopg.connect", return_value=mock_conn
        ) as mock_connect:
            listener.listen("demo", lambda _payload: None, timeout=0.01)
        self.assertNotIn("application_name", mock_connect.call_args.kwargs)

    async def test_async_connect_receives_name_only_when_set(self):
        queue = AsyncPGMQueue(
            **_offline_kwargs(pool=MagicMock(), application_name="worker/pgmq")
        )
        listener = AsyncNotificationListener(queue)
        listener._stop_event.set()
        mock_conn = AsyncMock()
        with patch(
            "pgmq.notify_listener.asyncpg.connect",
            new_callable=AsyncMock,
            return_value=mock_conn,
        ) as mock_connect:
            await listener.listen("demo", AsyncMock())
        self.assertEqual(
            mock_connect.call_args.kwargs["server_settings"],
            {"application_name": "worker/pgmq"},
        )
        self.assertNotIn("application_name", mock_connect.call_args.args[0])

        queue = AsyncPGMQueue(**_offline_kwargs(pool=MagicMock()))
        listener = AsyncNotificationListener(queue)
        listener._stop_event.set()
        mock_conn = AsyncMock()
        with patch(
            "pgmq.notify_listener.asyncpg.connect",
            new_callable=AsyncMock,
            return_value=mock_conn,
        ) as mock_connect:
            await listener.listen("demo", AsyncMock())
        self.assertNotIn("server_settings", mock_connect.call_args.kwargs)


class TestInstallApplicationName(unittest.TestCase):
    @patch("pgmq.install.psycopg.connect")
    def test_connect_omits_name_when_unset(self, mock_connect):
        mock_connect.return_value = MagicMock()
        dsn = _dsn()
        install_pgmq_sql("SELECT 1;", dsn=dsn)
        mock_connect.assert_called_once_with(dsn, autocommit=False)

    @patch("pgmq.install.psycopg.connect")
    def test_connect_includes_name_when_set(self, mock_connect):
        mock_connect.return_value = MagicMock()
        dsn = _dsn()
        install_pgmq_sql("SELECT 1;", dsn=dsn, application_name="worker/pgmq")
        mock_connect.assert_called_once_with(
            dsn, autocommit=False, application_name="worker/pgmq"
        )
        self.assertNotIn("application_name", mock_connect.call_args.args[0])

    @patch("pgmq.install.psycopg.connect")
    def test_supplied_connection_is_not_opened(self, mock_connect):
        conn = MagicMock()
        install_pgmq_sql(
            "SELECT 1;",
            conn=conn,
            application_name="worker/pgmq",
        )
        mock_connect.assert_not_called()


class TestFactoryApplicationName(unittest.TestCase):
    def test_queue_kwargs_from_config_copies_name(self):
        from pgmq.api.factory import queue_kwargs_from_config

        config = PGMQConfig(**_offline_kwargs(application_name="worker/pgmq"))
        kwargs = queue_kwargs_from_config(config)
        self.assertEqual(kwargs["application_name"], "worker/pgmq")
        with (
            patch.object(PGMQueue, "_init_pool"),
            patch.object(PGMQueue, "_init_extensions"),
        ):
            client = PGMQueue(**kwargs)
        self.assertEqual(client.application_name, "worker/pgmq")
        self.assertEqual(client.config.application_name, "worker/pgmq")


@unittest.skipUnless(_postgres_available(), "Postgres is not available")
class TestApplicationNameIntegration(unittest.IsolatedAsyncioTestCase):
    def _assert_setting(self, row):
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "worker/pgmq")

    def test_sync_client_sets_name(self):
        queue = PGMQueue(**_pg_kwargs())
        try:
            self._assert_setting(
                queue._execute_one("SELECT current_setting('application_name')")
            )
        finally:
            queue.close()

    def test_sqlalchemy_client_sets_name(self):
        queue = SQLAlchemyPGMQueue(**_pg_kwargs())
        try:
            self._assert_setting(
                queue._execute_one("SELECT current_setting('application_name')")
            )
        finally:
            queue.dispose()

    async def test_async_client_sets_name(self):
        queue = AsyncPGMQueue(**_pg_kwargs())
        await queue.init()
        try:
            self._assert_setting(
                await queue._execute_one("SELECT current_setting('application_name')")
            )
        finally:
            await queue.close()

    async def test_sqlalchemy_async_client_sets_name(self):
        queue = SQLAlchemyAsyncPGMQueue(**_pg_kwargs())
        await queue.init()
        try:
            self._assert_setting(
                await queue._execute_one("SELECT current_setting('application_name')")
            )
        finally:
            await queue.close()

    def test_supplied_pool_does_not_use_client_name(self):
        from psycopg_pool import ConnectionPool

        pool = ConnectionPool(_dsn(), open=True)
        try:
            queue = PGMQueue(**_pg_kwargs(pool=pool))
            self.assertIs(queue.pool, pool)
            with pool.connection() as conn:
                row = conn.execute(
                    "SELECT current_setting('application_name')"
                ).fetchone()
            self.assertNotEqual(row[0], "worker/pgmq")
        finally:
            pool.close()

    async def test_supplied_async_pool_does_not_use_client_name(self):
        import asyncpg

        config = PGMQConfig(**_pg_kwargs(application_name=None))
        pool = await asyncpg.create_pool(config.async_dsn, min_size=1, max_size=1)
        try:
            queue = AsyncPGMQueue(**_pg_kwargs(pool=pool))
            await queue.init()
            self.assertIs(queue.pool, pool)
            row = await queue._execute_one("SELECT current_setting('application_name')")
            self.assertNotEqual(row[0], "worker/pgmq")
        finally:
            await pool.close()
