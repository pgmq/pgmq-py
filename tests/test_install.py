"""Tests for PGMQ SQL installation."""

import os
import unittest
import uuid
from unittest.mock import MagicMock, patch

import psycopg
from psycopg import sql

from pgmq import PGMQueue
from pgmq.base import PGMQConfig, resolve_pgmq_config
from pgmq.install import (
    PGMQInstallError,
    get_embedded_install_sql,
    get_embedded_sql_version,
    install_pgmq_from_sql,
    install_pgmq_sql,
)
from tests.utils import PG_HOST, PG_PASSWORD, PG_USERNAME, PG_DATABASE

# Plain Postgres (no PGMQ extension) for SQL-only install integration tests.
PLAIN_PG_HOST = os.getenv("PG_SQL_INSTALL_HOST", PG_HOST)
PLAIN_PG_PORT = os.getenv("PG_SQL_INSTALL_PORT", "5433")
PLAIN_PG_DATABASE = os.getenv("PG_SQL_INSTALL_DATABASE", PG_DATABASE)
PLAIN_PG_USERNAME = os.getenv("PG_SQL_INSTALL_USERNAME", PG_USERNAME)
PLAIN_PG_PASSWORD = os.getenv("PG_SQL_INSTALL_PASSWORD", PG_PASSWORD)
PLAIN_PG_SYNC_DATABASE = "pgmq_sql_install_sync"
PLAIN_PG_SECOND_DATABASE = "pgmq_sql_install_second"


def _plain_postgres_config(database: str = PLAIN_PG_DATABASE) -> PGMQConfig:
    return PGMQConfig(
        host=PLAIN_PG_HOST,
        port=PLAIN_PG_PORT,
        database=database,
        username=PLAIN_PG_USERNAME,
        password=PLAIN_PG_PASSWORD,
        conn_string=None,
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
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
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
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database))
        )
    finally:
        conn.close()


PLAIN_POSTGRES_SKIP_REASON = (
    "Plain Postgres without PGMQ extension required for SQL install tests "
    f"(expected at {PLAIN_PG_HOST}:{PLAIN_PG_PORT}). "
    "Run `make run-plain-postgres` or `make test`."
)


class TestEmbeddedInstallSql(unittest.TestCase):
    def test_get_embedded_sql_version(self):
        version = get_embedded_sql_version()
        self.assertRegex(version, r"^\d+\.\d+\.\d+$")

    def test_get_embedded_install_sql(self):
        sql_script = get_embedded_install_sql()
        self.assertIn("CREATE SCHEMA IF NOT EXISTS pgmq", sql_script)
        self.assertGreater(len(sql_script), 1000)
        self.assertIn(
            f"-- pgmq-py bundled SQL version: {get_embedded_sql_version()}",
            sql_script,
        )

    @patch("pgmq.install.files")
    def test_get_embedded_install_sql_read_failure(self, mock_files):
        mock_files.return_value.joinpath.return_value.read_text.side_effect = (
            FileNotFoundError("pgmq.sql not found")
        )
        with self.assertRaises(PGMQInstallError) as ctx:
            get_embedded_install_sql()
        self.assertIn("Failed to read embedded PGMQ SQL script", str(ctx.exception))
        self.assertIn("pgmq.sql not found", str(ctx.exception))


class TestResolveConfig(unittest.TestCase):
    def test_dsn_merges_config_kwargs(self):
        config = resolve_pgmq_config(
            dsn=(
                "host=localhost port=5432 dbname=postgres "
                "user=postgres password=postgres"
            ),
            config_kwargs={"verbose": True, "pool_size": 5},
        )
        self.assertTrue(config.verbose)
        self.assertEqual(config.pool_size, 5)
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.database, "postgres")

    def test_dsn_takes_precedence_over_conn_string_in_config_kwargs(self):
        config = resolve_pgmq_config(
            dsn="host=sqlhost port=5432 dbname=postgres user=postgres password=secret",
            config_kwargs={"conn_string": "host=ignored port=5432 dbname=ignored"},
        )
        self.assertEqual(config.host, "sqlhost")

    def test_rejects_unsupported_config_kwargs(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_pgmq_config(
                config_kwargs={
                    "host": "localhost",
                    "port": "5432",
                    "database": "postgres",
                    "username": "postgres",
                    "password": "postgres",
                    "sslmode": "require",
                    "connect_timeout": 5,
                },
            )
        self.assertIn("sslmode", str(ctx.exception))
        self.assertIn("connect_timeout", str(ctx.exception))
        self.assertIn("dsn", str(ctx.exception))

    @patch.dict(os.environ, {"DATABASE_URL": "postgresql://envhost:5432/envdb"})
    def test_non_connection_kwargs_preserve_database_url(self):
        config = resolve_pgmq_config(config_kwargs={"verbose": True, "pool_size": 5})
        self.assertTrue(config.verbose)
        self.assertEqual(config.pool_size, 5)
        self.assertEqual(config.host, "envhost")
        self.assertEqual(config.database, "envdb")

    @patch.dict(os.environ, {"DATABASE_URL": "postgresql://envhost:5432/envdb"})
    def test_explicit_connection_kwargs_not_overridden_by_database_url(self):
        config = resolve_pgmq_config(
            config_kwargs={
                "host": "localhost",
                "port": "5432",
                "database": "postgres",
                "username": "postgres",
                "password": "postgres",
            },
        )
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.database, "postgres")


class TestInstallPgmqSqlErrors(unittest.TestCase):
    def test_empty_sql_raises_install_error(self):
        with self.assertRaises(PGMQInstallError) as ctx:
            install_pgmq_sql("   ", host="localhost")
        self.assertIn("empty", str(ctx.exception).lower())

    @patch("pgmq.install.psycopg.connect")
    def test_connection_failure_raises_install_error(self, mock_connect):
        mock_connect.side_effect = psycopg.OperationalError("connection refused")
        with self.assertRaises(PGMQInstallError) as ctx:
            install_pgmq_sql(
                "SELECT 1;",
                host="localhost",
                port="5432",
                database="postgres",
                username="postgres",
                password="postgres",
            )
        self.assertIn("connection refused", str(ctx.exception))

    @patch("pgmq.install.psycopg.connect")
    def test_connect_uses_conn_string_to_preserve_options(self, mock_connect):
        mock_connect.return_value = MagicMock()
        dsn = (
            "host=localhost port=5432 dbname=postgres user=postgres "
            "password=postgres sslmode=require connect_timeout=5"
        )
        install_pgmq_sql("SELECT 1;", dsn=dsn)
        mock_connect.assert_called_once_with(dsn, autocommit=False)

    @patch("pgmq.install._execute_sql_script")
    @patch("pgmq.install.psycopg.connect")
    def test_sql_execution_failure_raises_install_error(
        self, mock_connect, mock_execute
    ):
        mock_connect.return_value = MagicMock()
        mock_execute.side_effect = psycopg.Error("syntax error at line 1")
        with self.assertRaises(PGMQInstallError) as ctx:
            install_pgmq_sql(
                "SELECT 1;",
                host="localhost",
                port="5432",
                database="postgres",
                username="postgres",
                password="postgres",
            )
        self.assertIn("syntax error", str(ctx.exception))


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

    def test_install_pgmq_sql_rejects_unsupported_connect_kwargs(self):
        with self.assertRaises(ValueError) as ctx:
            install_pgmq_sql(
                "SELECT 1;",
                host="localhost",
                sslmode="require",
            )
        self.assertIn("sslmode", str(ctx.exception))


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
        cls.installed_version = install_pgmq_from_sql(**cls._connection_kwargs)
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
        self.assertEqual(self.installed_version, get_embedded_sql_version())
        queue_name = f"install_test_{uuid.uuid4().hex[:8]}"
        self.queue.create_queue(queue_name)
        try:
            msg_id = self.queue.send(queue_name, {"installed": True})
            self.assertGreater(msg_id, 0)
        finally:
            self.queue.drop_queue(queue_name)

    def test_reinstall_raises_when_schema_already_exists(self):
        with self.assertRaises(PGMQInstallError):
            install_pgmq_from_sql(**self._connection_kwargs)

    def test_install_on_second_database(self):
        _drop_database(PLAIN_PG_SECOND_DATABASE)
        _create_database(PLAIN_PG_SECOND_DATABASE)
        connection_kwargs = _plain_postgres_connection_kwargs(
            database=PLAIN_PG_SECOND_DATABASE
        )
        try:
            self.assertFalse(
                _pgmq_extension_installed(database=PLAIN_PG_SECOND_DATABASE)
            )
            self.assertFalse(_pgmq_schema_exists(database=PLAIN_PG_SECOND_DATABASE))

            version = install_pgmq_from_sql(**connection_kwargs)
            self.assertEqual(version, get_embedded_sql_version())
            self.assertTrue(_pgmq_schema_exists(database=PLAIN_PG_SECOND_DATABASE))

            queue = PGMQueue(init_extension=False, **connection_kwargs)
            try:
                queue_name = f"install_second_db_{uuid.uuid4().hex[:8]}"
                queue.create_queue(queue_name)
                msg_id = queue.send(queue_name, {"installed": True})
                self.assertGreater(msg_id, 0)
                queue.drop_queue(queue_name)
            finally:
                queue.close()
        finally:
            _drop_database(PLAIN_PG_SECOND_DATABASE)
