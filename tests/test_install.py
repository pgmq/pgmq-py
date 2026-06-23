"""Tests for PGMQ SQL installation from GitHub."""

import importlib.util
import json
import os
import unittest
import uuid
from unittest.mock import patch

import psycopg

from pgmq import PGMQueue
from pgmq.base import PGMQConfig
from pgmq.install import (
    PGMQInstallError,
    build_install_sql_url,
    get_install_sql,
    get_latest_release_tag,
    install_pgmq_from_sql,
    install_pgmq_sql,
)
from tests.utils import PG_HOST, PG_PASSWORD, PG_USERNAME, PG_DATABASE

ASYNCPG_AVAILABLE = importlib.util.find_spec("asyncpg") is not None

# Plain Postgres (no PGMQ extension) for SQL-only install integration tests.
PLAIN_PG_HOST = os.getenv("PG_SQL_INSTALL_HOST", PG_HOST)
PLAIN_PG_PORT = os.getenv("PG_SQL_INSTALL_PORT", "5433")
PLAIN_PG_DATABASE = os.getenv("PG_SQL_INSTALL_DATABASE", PG_DATABASE)
PLAIN_PG_USERNAME = os.getenv("PG_SQL_INSTALL_USERNAME", PG_USERNAME)
PLAIN_PG_PASSWORD = os.getenv("PG_SQL_INSTALL_PASSWORD", PG_PASSWORD)
PLAIN_PG_SYNC_DATABASE = "pgmq_sql_install_sync"
PLAIN_PG_ASYNC_DATABASE = "pgmq_sql_install_async"
INSTALL_TEST_VERSION = "1.11.1"


def _plain_postgres_config(database: str = PLAIN_PG_DATABASE) -> PGMQConfig:
    return PGMQConfig(
        host=PLAIN_PG_HOST,
        port=PLAIN_PG_PORT,
        database=database,
        username=PLAIN_PG_USERNAME,
        password=PLAIN_PG_PASSWORD,
    )


def _plain_postgres_connection_kwargs(
    database: str = PLAIN_PG_DATABASE,
) -> dict[str, str]:
    return {
        "host": PLAIN_PG_HOST,
        "port": PLAIN_PG_PORT,
        "database": database,
        "username": PLAIN_PG_USERNAME,
        "password": PLAIN_PG_PASSWORD,
    }


def _plain_postgres_available() -> bool:
    try:
        conn = psycopg.connect(_plain_postgres_config().dsn, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


def _pgmq_extension_installed(database: str = PLAIN_PG_DATABASE) -> bool:
    conn = psycopg.connect(
        _plain_postgres_config(database=database).dsn, autocommit=True
    )
    try:
        return bool(
            conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgmq')"
            ).fetchone()[0]
        )
    finally:
        conn.close()


def _pgmq_schema_exists(database: str = PLAIN_PG_DATABASE) -> bool:
    conn = psycopg.connect(
        _plain_postgres_config(database=database).dsn, autocommit=True
    )
    try:
        return bool(
            conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'pgmq')"
            ).fetchone()[0]
        )
    finally:
        conn.close()


def _plain_postgres_ready_for_sql_install() -> bool:
    if not _plain_postgres_available():
        return False
    return not _pgmq_extension_installed()


def _create_database(database: str) -> None:
    conn = psycopg.connect(
        _plain_postgres_config(database=PLAIN_PG_DATABASE).dsn,
        autocommit=True,
    )
    try:
        exists = conn.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
            (database,),
        ).fetchone()[0]
        if not exists:
            conn.execute(f'CREATE DATABASE "{database}"')
    finally:
        conn.close()


def _drop_database(database: str) -> None:
    conn = psycopg.connect(
        _plain_postgres_config(database=PLAIN_PG_DATABASE).dsn,
        autocommit=True,
    )
    try:
        conn.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (database,),
        )
        conn.execute(f'DROP DATABASE IF EXISTS "{database}"')
    finally:
        conn.close()


PLAIN_POSTGRES_SKIP_REASON = (
    "Plain Postgres without PGMQ extension required for SQL install tests "
    f"(expected at {PLAIN_PG_HOST}:{PLAIN_PG_PORT}). "
    "Run `make run-plain-postgres` or `make test`."
)


class TestBuildInstallSqlUrl(unittest.TestCase):
    def test_release_tag_with_v_prefix(self):
        url = build_install_sql_url("v1.9.0")
        self.assertEqual(
            url,
            "https://raw.githubusercontent.com/pgmq/pgmq/refs/tags/v1.9.0"
            "/pgmq-extension/sql/pgmq.sql",
        )

    def test_release_tag_without_v_prefix(self):
        url = build_install_sql_url("1.9.0")
        self.assertEqual(
            url,
            "https://raw.githubusercontent.com/pgmq/pgmq/refs/tags/v1.9.0"
            "/pgmq-extension/sql/pgmq.sql",
        )

    def test_git_hash(self):
        url = build_install_sql_url("eb182e23d71543ba9f0a304fb2865f8b8cc18ae7")
        self.assertEqual(
            url,
            "https://raw.githubusercontent.com/pgmq/pgmq/"
            "eb182e23d71543ba9f0a304fb2865f8b8cc18ae7/pgmq-extension/sql/pgmq.sql",
        )


class TestInstallFetch(unittest.TestCase):
    @patch("pgmq.install._github_request")
    def test_get_latest_release_tag(self, mock_request):
        mock_request.return_value = json.dumps({"tag_name": "v1.11.1"}).encode("utf-8")
        self.assertEqual(get_latest_release_tag(), "v1.11.1")

    @patch("pgmq.install._github_request")
    def test_get_install_sql_with_explicit_version(self, mock_request):
        mock_request.return_value = b"CREATE SCHEMA pgmq;"
        sql = get_install_sql("1.11.1")
        self.assertEqual(sql, "CREATE SCHEMA pgmq;")
        mock_request.assert_called_once_with(
            "https://raw.githubusercontent.com/pgmq/pgmq/refs/tags/v1.11.1"
            "/pgmq-extension/sql/pgmq.sql"
        )

    @patch("pgmq.install.get_latest_release_tag", return_value="v1.11.1")
    @patch("pgmq.install._github_request")
    def test_get_install_sql_latest(self, mock_request, _mock_latest):
        mock_request.return_value = b"CREATE SCHEMA pgmq;"
        sql = get_install_sql()
        self.assertEqual(sql, "CREATE SCHEMA pgmq;")
        mock_request.assert_called_once_with(
            "https://raw.githubusercontent.com/pgmq/pgmq/refs/tags/v1.11.1"
            "/pgmq-extension/sql/pgmq.sql"
        )

    @patch("pgmq.install._github_request")
    def test_get_install_sql_empty_raises(self, mock_request):
        mock_request.return_value = b"   "
        with self.assertRaises(PGMQInstallError):
            get_install_sql("1.11.1")


@unittest.skipUnless(_plain_postgres_available(), PLAIN_POSTGRES_SKIP_REASON)
class TestInstallSqlExecution(unittest.TestCase):
    def test_install_pgmq_sql_with_existing_connection(self):
        conn = psycopg.connect(_plain_postgres_config().dsn, autocommit=False)
        try:
            install_pgmq_sql(
                "CREATE SCHEMA IF NOT EXISTS pgmq_install_test;"
                "CREATE TABLE IF NOT EXISTS pgmq_install_test.t (id int);",
                conn=conn,
            )
            result = conn.execute(
                "SELECT to_regclass('pgmq_install_test.t')"
            ).fetchone()[0]
            self.assertEqual(result, "pgmq_install_test.t")
            conn.execute("DROP SCHEMA pgmq_install_test CASCADE")
            conn.commit()
        finally:
            conn.close()

    def test_install_pgmq_sql_rejects_conflicting_config(self):
        with self.assertRaises(ValueError):
            install_pgmq_sql(
                "SELECT 1;",
                config=PGMQConfig(),
                host="localhost",
            )


@unittest.skipUnless(
    _plain_postgres_ready_for_sql_install(),
    PLAIN_POSTGRES_SKIP_REASON,
)
class TestInstallFromSqlIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _drop_database(PLAIN_PG_SYNC_DATABASE)
        _create_database(PLAIN_PG_SYNC_DATABASE)
        cls._connection_kwargs = _plain_postgres_connection_kwargs(
            database=PLAIN_PG_SYNC_DATABASE
        )
        cls.installed_version = install_pgmq_from_sql(
            version=INSTALL_TEST_VERSION,
            **cls._connection_kwargs,
        )
        cls.queue = PGMQueue(init_extension=False, **cls._connection_kwargs)

    @classmethod
    def tearDownClass(cls):
        cls.queue.close()
        _drop_database(PLAIN_PG_SYNC_DATABASE)

    def test_pgmq_extension_not_installed(self):
        self.assertFalse(_pgmq_extension_installed(database=PLAIN_PG_SYNC_DATABASE))

    def test_pgmq_schema_exists_after_sql_install(self):
        self.assertTrue(_pgmq_schema_exists(database=PLAIN_PG_SYNC_DATABASE))

    def test_queue_operations_after_sql_install(self):
        self.assertEqual(self.installed_version, INSTALL_TEST_VERSION)
        queue_name = f"install_test_{uuid.uuid4().hex[:8]}"
        self.queue.create_queue(queue_name)
        try:
            msg_id = self.queue.send(queue_name, {"installed": True})
            self.assertGreater(msg_id, 0)
        finally:
            self.queue.drop_queue(queue_name)


@unittest.skipUnless(ASYNCPG_AVAILABLE, "asyncpg not installed")
@unittest.skipUnless(
    _plain_postgres_ready_for_sql_install(),
    PLAIN_POSTGRES_SKIP_REASON,
)
class TestInstallFromSqlAsyncIntegration(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        _drop_database(PLAIN_PG_ASYNC_DATABASE)
        _create_database(PLAIN_PG_ASYNC_DATABASE)

    @classmethod
    def tearDownClass(cls):
        _drop_database(PLAIN_PG_ASYNC_DATABASE)

    async def test_install_pgmq_from_sql_async(self):
        from pgmq.install import install_pgmq_from_sql_async

        connection_kwargs = _plain_postgres_connection_kwargs(
            database=PLAIN_PG_ASYNC_DATABASE
        )
        self.assertFalse(_pgmq_extension_installed(database=PLAIN_PG_ASYNC_DATABASE))
        self.assertFalse(_pgmq_schema_exists(database=PLAIN_PG_ASYNC_DATABASE))

        version = await install_pgmq_from_sql_async(
            version=INSTALL_TEST_VERSION,
            **connection_kwargs,
        )
        self.assertEqual(version, INSTALL_TEST_VERSION)
        self.assertTrue(_pgmq_schema_exists(database=PLAIN_PG_ASYNC_DATABASE))
        self.assertFalse(_pgmq_extension_installed(database=PLAIN_PG_ASYNC_DATABASE))

        queue = PGMQueue(init_extension=False, **connection_kwargs)
        try:
            queue_name = f"install_async_{uuid.uuid4().hex[:8]}"
            queue.create_queue(queue_name)
            msg_id = queue.send(queue_name, {"installed": True})
            self.assertGreater(msg_id, 0)
            queue.drop_queue(queue_name)
        finally:
            queue.close()
