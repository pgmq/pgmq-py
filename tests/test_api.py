# tests/test_api.py
import os
import stat
import tempfile
import unittest
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional
from unittest.mock import patch

try:
    import fastapi  # noqa: F401
    from pgmq.api import create_app, create_router, PGMQAPIConfig

    HAS_API = True
except ImportError:
    HAS_API = False
    create_app = None
    create_router = None
    PGMQAPIConfig = None


@contextmanager
def isolated_api_env(**overrides: str) -> Iterator[None]:
    """Clear PGMQ_API_*, PGMQ_NAMED_KEY_*, and PGMQ_KEY_* env vars, then restore."""
    prefixes = ("PGMQ_API_", "PGMQ_NAMED_KEY_", "PGMQ_KEY_", "PGMQ_REVOKED_IDS")
    saved = {
        key: value for key, value in os.environ.items() if key.startswith(prefixes)
    }
    for key in list(saved):
        del os.environ[key]
    merged = {"PGMQ_API_ENV": "/no/such/pgmq.env"}
    merged.update(overrides)
    os.environ.update(merged)
    try:
        yield
    finally:
        for key in list(os.environ):
            if key.startswith(prefixes):
                del os.environ[key]
        os.environ.update(saved)


class FakeAcquire:
    """Async context manager that mimics asyncpg pool.acquire()."""

    def __init__(self, fail: bool) -> None:
        self.fail = fail

    async def __aenter__(self) -> "FakeAcquire":
        if self.fail:
            raise ConnectionError("db down")
        return self

    async def __aexit__(self, *args: Any) -> bool:
        return False

    async def execute(self, sql: str) -> None:
        return None


class FakePool:
    """Pool with async acquire() for ping_queue."""

    def __init__(self) -> None:
        self.fail = False

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.fail)


class FakeQueue:
    """Injected client. init/ping can be toggled without a real database."""

    def __init__(self) -> None:
        self.pool = FakePool()
        self.fail_init = False
        self.init_calls = 0
        self.close_calls = 0

    async def init(self) -> None:
        self.init_calls += 1
        if self.fail_init:
            raise ConnectionError("db down")

    async def close(self) -> None:
        self.close_calls += 1


class LazyFakeQueue:
    """Pool is None until init(), like the standalone _DefaultQueue."""

    def __init__(self) -> None:
        self.pool = None
        self.init_calls = 0

    async def init(self) -> None:
        self.init_calls += 1
        self.pool = FakePool()


def _attach_probe(app: Any) -> None:
    """Add a protected probe so auth tests have a route to hit."""
    from fastapi import Depends

    verify = app.state.verify_key

    @app.get("/v1/_probe")
    async def _probe(_auth: Any = Depends(verify)) -> Dict[str, bool]:
        return {"ok": True}


@unittest.skipUnless(HAS_API, "pgmq[fastapi] extra is not installed")
class TestAPISchemas(unittest.TestCase):
    """Pydantic response models for PR1."""

    def test_health_and_ready_schema_keys(self) -> None:
        from pgmq.api.schemas import ErrorResponse, HealthResponse, ReadyResponse

        health = HealthResponse(status="ok", version="1.0.0", auth="disabled")
        self.assertEqual(
            health.model_dump(),
            {
                "status": "ok",
                "version": "1.0.0",
                "auth": "disabled",
            },
        )
        ready = ReadyResponse(status="ok")
        self.assertEqual(ready.model_dump(), {"status": "ok"})
        err = ErrorResponse(error="unauthorized", detail="Missing API key")
        self.assertEqual(
            err.model_dump(),
            {
                "error": "unauthorized",
                "detail": "Missing API key",
            },
        )


@unittest.skipUnless(HAS_API, "pgmq[fastapi] extra is not installed")
class TestAPIConfig(unittest.TestCase):
    """YAML, env, CLI, and init-config loading."""

    def test_load_yaml_and_env_override(self) -> None:
        from pgmq.api.config import load_api_config

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "pgmq-api.yaml")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "listen:\n"
                    '  host: "10.0.0.1"\n'
                    "  port: 9999\n"
                    "auth:\n"
                    "  mode: static\n"
                    "limits:\n"
                    "  max_qty: 50\n"
                    "  max_batch: 10\n"
                    "  max_poll_seconds: 7\n"
                )
            os.chmod(path, 0o600)
            with isolated_api_env(PGMQ_API_PORT="7777", PGMQ_API_MAX_QTY="80"):
                cfg = load_api_config(path)
            self.assertEqual(cfg.host, "10.0.0.1")
            self.assertEqual(cfg.port, 7777)
            self.assertEqual(cfg.auth_mode, "keys")
            self.assertEqual(cfg.max_qty, 80)
            self.assertEqual(cfg.max_batch, 10)
            self.assertEqual(cfg.max_poll_seconds, 7)

    def test_refuse_secrets_in_yaml(self) -> None:
        from pgmq.api.config import load_api_config

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "pgmq-api.yaml")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("auth:\n  mode: keys\n  static_key: super-secret\n")
            with isolated_api_env():
                with self.assertRaises(ValueError) as ctx:
                    load_api_config(path)
            self.assertIn(".env", str(ctx.exception))

    def test_init_config_writes_0600_yaml(self) -> None:
        from pgmq.api.__main__ import main

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "pgmq-api.yaml")
            rc = main(["init-config", "--path", path])
            self.assertIn(rc, (0, None))
            mode = stat.S_IMODE(os.stat(path).st_mode)
            self.assertEqual(mode, 0o600)
            with open(path, encoding="utf-8") as handle:
                text = handle.read()
            self.assertIn("mode: none", text)
            self.assertIn("use_db: false", text)
            self.assertIn("127.0.0.1", text)
            self.assertNotIn("static_key:", text)
            env_example = os.path.join(tmp, ".env.example")
            self.assertTrue(os.path.isfile(env_example))

    def test_init_config_refuses_existing(self) -> None:
        from pgmq.api.__main__ import main

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "pgmq-api.yaml")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("listen:\n  host: leftover\n")
            rc = main(["init-config", "--path", path])
            self.assertNotEqual(rc, 0)
            with open(path, encoding="utf-8") as handle:
                self.assertIn("leftover", handle.read())
            rc = main(["init-config", "--path", path, "--force"])
            self.assertIn(rc, (0, None))
            with open(path, encoding="utf-8") as handle:
                self.assertNotIn("leftover", handle.read())

    def test_start_without_yaml(self) -> None:
        from pgmq.api.config import load_api_config

        with isolated_api_env():
            cfg = load_api_config()
        self.assertEqual(cfg.host, "0.0.0.0")
        self.assertEqual(cfg.port, 8080)
        self.assertEqual(cfg.auth_mode, "none")
        queue = FakeQueue()
        app = create_app(queue=queue, api_config=cfg)
        self.assertIsNotNone(app)

    def test_queue_kwargs_from_config_constructs_client(self) -> None:
        from pgmq.api.factory import queue_kwargs_from_config
        from pgmq.base import PGMQConfig
        from pgmq.queue import PGMQueue as SyncPGMQueue

        cfg = PGMQConfig(
            host="localhost",
            port="5432",
            database="postgres",
            username="postgres",
            password="postgres",
            init_extension=False,
        )
        kwargs = queue_kwargs_from_config(cfg)
        self.assertNotIn("config", kwargs)
        self.assertEqual(kwargs["host"], "localhost")
        with (
            patch.object(SyncPGMQueue, "_init_pool"),
            patch.object(SyncPGMQueue, "_init_extensions"),
        ):
            client = SyncPGMQueue(**kwargs)
        self.assertEqual(client.host, "localhost")
        self.assertEqual(client.database, "postgres")


@unittest.skipUnless(HAS_API, "pgmq[fastapi] extra is not installed")
class TestAPI(unittest.IsolatedAsyncioTestCase):
    """ASGI health, ready, and static auth."""

    async def _client(
        self,
        queue: Optional[FakeQueue] = None,
        api_config: Optional[Any] = None,
        probe: bool = False,
    ) -> Any:
        from httpx import ASGITransport, AsyncClient

        if queue is None:
            queue = FakeQueue()
        if api_config is None:
            api_config = PGMQAPIConfig(auth_mode="none")
        app = create_app(queue=queue, api_config=api_config)
        if probe:
            _attach_probe(app)
        client = AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        )
        return client, queue, app

    async def test_health_ok(self) -> None:
        client, _, _ = await self._client()
        try:
            resp = await client.get("/v1/health")
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["status"], "ok")
            self.assertIn("version", body)
            self.assertIn("auth", body)
            alias = await client.get("/health")
            self.assertEqual(alias.status_code, 200)
            self.assertEqual(alias.json()["status"], "ok")
        finally:
            await client.aclose()

    async def test_ready_ok(self) -> None:
        cfg = PGMQAPIConfig(auth_mode="static", api_key="secret")
        client, _, _ = await self._client(api_config=cfg)
        try:
            resp = await client.get("/v1/ready")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"status": "ok"})
        finally:
            await client.aclose()

    async def test_auth_disabled_warns(self) -> None:
        with patch("pgmq.api.factory.log_with_context") as mocked:
            create_app(queue=FakeQueue(), api_config=PGMQAPIConfig(auth_mode="none"))
        warning_calls = [
            call
            for call in mocked.call_args_list
            if call.args
            and call.args[1] in ("WARNING", 30)
            or (
                len(call.args) > 1
                and (
                    call.args[1] == 30
                    or (
                        isinstance(call.args[1], str)
                        and call.args[1].upper() == "WARNING"
                    )
                )
            )
        ]
        if not warning_calls:
            import logging

            warning_calls = [
                call
                for call in mocked.call_args_list
                if logging.WARNING in call.args or "disabled" in str(call).lower()
            ]
        self.assertTrue(warning_calls, "expected WARNING when auth.mode is none")

    async def test_static_key_required(self) -> None:
        cfg = PGMQAPIConfig(auth_mode="static", api_key="secret")
        client, _, _ = await self._client(api_config=cfg, probe=True)
        try:
            missing = await client.get("/v1/_probe")
            self.assertEqual(missing.status_code, 401)
            self.assertEqual(missing.json()["error"], "unauthorized")
            wrong = await client.get(
                "/v1/_probe", headers={"Authorization": "Bearer wrong"}
            )
            self.assertEqual(wrong.status_code, 401)
            self.assertEqual(wrong.json()["error"], "unauthorized")
        finally:
            await client.aclose()

    async def test_static_key_ok(self) -> None:
        cfg = PGMQAPIConfig(auth_mode="static", api_key="secret")
        client, _, _ = await self._client(api_config=cfg, probe=True)
        try:
            bearer = await client.get(
                "/v1/_probe", headers={"Authorization": "Bearer secret"}
            )
            self.assertEqual(bearer.status_code, 200)
            header = await client.get("/v1/_probe", headers={"X-API-Key": "secret"})
            self.assertEqual(header.status_code, 200)
        finally:
            await client.aclose()

    async def test_health_ok_when_db_down(self) -> None:
        queue = FakeQueue()
        queue.fail_init = True
        queue.pool.fail = True
        client, queue, _ = await self._client(queue=queue)
        try:
            resp = await client.get("/v1/health")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["status"], "ok")
            self.assertEqual(queue.init_calls, 0)
        finally:
            await client.aclose()

    async def test_ready_503_then_ok_after_init(self) -> None:
        queue = FakeQueue()
        queue.fail_init = True
        client, queue, _ = await self._client(queue=queue)
        try:
            first = await client.get("/v1/ready")
            self.assertEqual(first.status_code, 503)
            self.assertEqual(first.json()["error"], "dependency_unavailable")
            queue.fail_init = False
            second = await client.get("/v1/ready")
            self.assertEqual(second.status_code, 200)
            self.assertEqual(second.json(), {"status": "ok"})
        finally:
            await client.aclose()

    async def test_ready_503_when_ping_fails_after_init(self) -> None:
        queue = FakeQueue()
        queue.fail_init = False
        queue.pool.fail = True
        client, _, _ = await self._client(queue=queue)
        try:
            resp = await client.get("/v1/ready")
            self.assertEqual(resp.status_code, 503)
            self.assertEqual(resp.json()["error"], "dependency_unavailable")
        finally:
            await client.aclose()

    async def test_create_router_missing_key_is_401(self) -> None:
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        app = FastAPI()
        app.include_router(
            create_router(
                queue=FakeQueue(),
                api_config=PGMQAPIConfig(auth_mode="keys", api_key="secret"),
            )
        )
        client = AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test",
        )
        try:
            resp = await client.get("/v1/queues")
            self.assertEqual(resp.status_code, 401)
            body = resp.json()
            self.assertEqual(body["error"], "unauthorized")
            self.assertIn("detail", body)
        finally:
            await client.aclose()

    async def test_health_auth_enabled_with_issued_keys(self) -> None:
        cfg = PGMQAPIConfig(
            auth_mode="keys",
            issued_keys=[
                {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "name": "billing",
                    "secret": "issued-secret",
                }
            ],
        )
        client, _, _ = await self._client(api_config=cfg)
        try:
            resp = await client.get("/v1/health")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["auth"], "enabled")
        finally:
            await client.aclose()

    async def test_use_db_inits_before_lookup(self) -> None:
        from httpx import ASGITransport, AsyncClient
        from unittest.mock import AsyncMock, patch

        queue = LazyFakeQueue()
        cfg = PGMQAPIConfig(auth_mode="keys", use_db=True, pepper="pepper")
        seen: Dict[str, Any] = {}

        async def _fake_verify(q: Any, _cfg: Any, presented: str) -> str:
            seen["init_calls"] = queue.init_calls
            seen["has_pool"] = q.pool is not None
            from pgmq.api.factory import APIError

            raise APIError(401, "unauthorized", "Invalid API key")

        with patch(
            "pgmq.api.auth.verify_worker_key", new=AsyncMock(side_effect=_fake_verify)
        ):
            app = create_app(queue=queue, api_config=cfg)
            client = AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            )
            try:
                resp = await client.get(
                    "/v1/queues", headers={"Authorization": "Bearer x"}
                )
                self.assertEqual(resp.status_code, 401)
                self.assertEqual(seen.get("init_calls"), 1)
                self.assertTrue(seen.get("has_pool"))
            finally:
                await client.aclose()


def _try_sync_queue() -> Any:
    """Build a SyncPGMQueue or return None when Postgres is down."""
    from pgmq.queue import PGMQueue as SyncPGMQueue
    from tests.utils import (
        PG_DATABASE,
        PG_HOST,
        PG_PASSWORD,
        PG_PORT,
        PG_USERNAME,
    )

    try:
        queue = SyncPGMQueue(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            username=PG_USERNAME,
            password=PG_PASSWORD,
            verbose=False,
            init_extension=False,
        )
        with queue.pool.connection() as conn:
            conn.execute("SELECT 1")
        return queue
    except Exception:
        return None


@unittest.skipUnless(HAS_API, "pgmq[fastapi] extra is not installed")
class TestAPINamedKeys(unittest.IsolatedAsyncioTestCase):
    """Named API keys from env. No Postgres required."""

    async def _http(self, api_config: Any) -> Any:
        from httpx import ASGITransport, AsyncClient

        app = create_app(queue=FakeQueue(), api_config=api_config)
        _attach_probe(app)
        return AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        )

    async def test_named_key_ok(self) -> None:
        cfg = PGMQAPIConfig(
            auth_mode="keys",
            named_keys={"billing": "secret-billing", "ingest": "secret-ingest"},
        )
        client = await self._http(cfg)
        try:
            ok = await client.get(
                "/v1/_probe", headers={"Authorization": "Bearer secret-billing"}
            )
            self.assertEqual(ok.status_code, 200)
            ok2 = await client.get("/v1/_probe", headers={"X-API-Key": "secret-ingest"})
            self.assertEqual(ok2.status_code, 200)
            bad = await client.get(
                "/v1/_probe", headers={"Authorization": "Bearer no-match"}
            )
            self.assertEqual(bad.status_code, 401)
        finally:
            await client.aclose()

    async def test_keys_mode_accepts_static_or_named(self) -> None:
        cfg = PGMQAPIConfig(
            auth_mode="keys",
            api_key="shared-secret",
            named_keys={"billing": "secret-billing"},
        )
        client = await self._http(cfg)
        try:
            static = await client.get(
                "/v1/_probe", headers={"Authorization": "Bearer shared-secret"}
            )
            self.assertEqual(static.status_code, 200)
            named = await client.get(
                "/v1/_probe", headers={"X-API-Key": "secret-billing"}
            )
            self.assertEqual(named.status_code, 200)
        finally:
            await client.aclose()

    async def test_issue_key_dry_run_does_not_write(self) -> None:
        from pgmq.api.__main__ import main

        with tempfile.TemporaryDirectory() as tmp:
            env_path = os.path.join(tmp, ".env")
            rc = main(
                [
                    "issue-key",
                    "--name",
                    "billing",
                    "--env-file",
                    env_path,
                ]
            )
            self.assertIn(rc, (0, None))
            self.assertFalse(os.path.exists(env_path))

    async def test_issue_key_apply_writes_env(self) -> None:
        from pgmq.api.__main__ import main

        with tempfile.TemporaryDirectory() as tmp:
            env_path = os.path.join(tmp, ".env")
            rc = main(
                [
                    "issue-key",
                    "--name",
                    "billing",
                    "--env-file",
                    env_path,
                    "--apply",
                ]
            )
            self.assertIn(rc, (0, None))
            with open(env_path, encoding="utf-8") as handle:
                text = handle.read()
            self.assertIn("PGMQ_KEY_", text)
            self.assertIn("PGMQ_KEY_NAME_", text)
            self.assertIn("billing", text)
            mode = stat.S_IMODE(os.stat(env_path).st_mode)
            self.assertEqual(mode, 0o600)

    async def test_revoke_key_rejects_presented_secret(self) -> None:
        from pgmq.api.__main__ import main
        from pgmq.api.config import (
            load_env_file,
            normalize_key_id,
            parse_issued_keys,
            parse_revoked_ids,
        )

        with tempfile.TemporaryDirectory() as tmp:
            env_path = os.path.join(tmp, ".env")
            rc = main(
                [
                    "issue-key",
                    "--name",
                    "billing",
                    "--env-file",
                    env_path,
                    "--apply",
                ]
            )
            self.assertIn(rc, (0, None))
            with open(env_path, encoding="utf-8") as handle:
                text = handle.read()
            key_id = None
            secret = None
            for line in text.splitlines():
                if line.startswith("PGMQ_KEY_") and not line.startswith(
                    "PGMQ_KEY_NAME_"
                ):
                    var, _, secret = line.partition("=")
                    suffix = var[len("PGMQ_KEY_") :]
                    key_id = normalize_key_id(suffix)
                    break
            self.assertIsNotNone(key_id)
            self.assertIsNotNone(secret)
            rc = main(
                [
                    "revoke-key",
                    "--id",
                    key_id,
                    "--env-file",
                    env_path,
                    "--apply",
                ]
            )
            self.assertIn(rc, (0, None))
            with isolated_api_env():
                load_env_file(env_path)
                cfg = PGMQAPIConfig(
                    auth_mode="keys",
                    issued_keys=parse_issued_keys(),
                    revoked_ids=parse_revoked_ids(),
                )
            client = await self._http(cfg)
            try:
                resp = await client.get(
                    "/v1/_probe", headers={"Authorization": f"Bearer {secret}"}
                )
                self.assertEqual(resp.status_code, 401)
            finally:
                await client.aclose()

    def test_load_named_keys_from_env(self) -> None:
        from pgmq.api.config import load_api_config

        with isolated_api_env(
            PGMQ_API_AUTH_MODE="keys",
            PGMQ_API_KEYS="billing:aaa,ingest:bbb",
        ):
            cfg = load_api_config()
        self.assertEqual(cfg.auth_mode, "keys")
        self.assertEqual(cfg.named_keys["billing"], "aaa")
        self.assertEqual(cfg.named_keys["ingest"], "bbb")

    def test_use_db_requires_pepper(self) -> None:
        queue = FakeQueue()
        with self.assertRaises(RuntimeError) as ctx:
            create_app(
                queue=queue,
                api_config=PGMQAPIConfig(auth_mode="keys", use_db=True),
            )
        self.assertIn("pepper", str(ctx.exception).lower())


@unittest.skipUnless(HAS_API, "pgmq[fastapi] extra is not installed")
class TestAPIQueues(unittest.IsolatedAsyncioTestCase):
    """Queue CRUD and metrics. Skip when Postgres is down."""

    async def asyncSetUp(self) -> None:
        self.queue = _try_sync_queue()
        if self.queue is None:
            self.skipTest("Postgres is not available")
        self.qname = f"apiq_{uuid.uuid4().hex[:8]}"
        self.created: list[str] = []

    async def asyncTearDown(self) -> None:
        client = getattr(self, "queue", None)
        if client is None:
            return
        for name in list(self.created) + [self.qname]:
            try:
                client.drop_queue(name)
            except Exception:
                pass
        client.close()

    async def _http(self) -> Any:
        from httpx import ASGITransport, AsyncClient

        app = create_app(queue=self.queue, api_config=PGMQAPIConfig(auth_mode="none"))
        return AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        )

    async def test_create_list_drop_queue(self) -> None:
        http = await self._http()
        try:
            created = await http.put(f"/v1/queues/{self.qname}")
            self.assertEqual(created.status_code, 201)
            self.assertEqual(
                created.json(),
                {"queue_name": self.qname, "unlogged": False},
            )
            self.created.append(self.qname)

            listed = await http.get("/v1/queues")
            self.assertEqual(listed.status_code, 200)
            body = listed.json()
            self.assertIn("queues", body)
            names = [item["queue_name"] for item in body["queues"]]
            self.assertIn(self.qname, names)
            record = next(
                item for item in body["queues"] if item["queue_name"] == self.qname
            )
            self.assertIn("is_partitioned", record)
            self.assertIn("is_unlogged", record)
            self.assertIn("created_at", record)

            dropped = await http.delete(f"/v1/queues/{self.qname}")
            self.assertEqual(dropped.status_code, 204)
            self.created.remove(self.qname)

            missing = await http.delete(f"/v1/queues/{self.qname}")
            self.assertEqual(missing.status_code, 404)
            self.assertEqual(missing.json()["error"], "queue_not_found")
        finally:
            await http.aclose()

    async def test_metrics_unknown_queue_404(self) -> None:
        http = await self._http()
        try:
            missing = f"nomq_{uuid.uuid4().hex[:8]}"
            resp = await http.get(f"/v1/queues/{missing}/metrics")
            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"], "queue_not_found")
        finally:
            await http.aclose()

    async def test_purge(self) -> None:
        http = await self._http()
        try:
            created = await http.put(f"/v1/queues/{self.qname}")
            self.assertEqual(created.status_code, 201)
            self.created.append(self.qname)
            self.queue.send(self.qname, {"n": 1})
            self.queue.send(self.qname, {"n": 2})
            resp = await http.post(f"/v1/queues/{self.qname}/purge")
            self.assertEqual(resp.status_code, 200)
            self.assertGreaterEqual(resp.json()["purged"], 2)
        finally:
            await http.aclose()

    async def test_put_queue_idempotent_201(self) -> None:
        http = await self._http()
        try:
            first = await http.put(f"/v1/queues/{self.qname}")
            second = await http.put(f"/v1/queues/{self.qname}")
            self.assertEqual(first.status_code, 201)
            self.assertEqual(second.status_code, 201)
            self.assertEqual(second.json()["unlogged"], False)
            self.created.append(self.qname)

            other = f"apiu_{uuid.uuid4().hex[:8]}"
            flagged = await http.put(f"/v1/queues/{other}?unlogged=true")
            self.assertEqual(flagged.status_code, 201)
            self.assertEqual(
                flagged.json(),
                {"queue_name": other, "unlogged": True},
            )
            self.created.append(other)
        finally:
            await http.aclose()


@unittest.skipUnless(HAS_API, "pgmq[fastapi] extra is not installed")
class TestAPISend(unittest.IsolatedAsyncioTestCase):
    """HTTP send and send_batch. Skip when Postgres is down."""

    async def asyncSetUp(self) -> None:
        self.queue = _try_sync_queue()
        if self.queue is None:
            self.skipTest("Postgres is not available")
        self.qname = f"apis_{uuid.uuid4().hex[:8]}"
        self.queue.create_queue(self.qname)

    async def asyncTearDown(self) -> None:
        client = getattr(self, "queue", None)
        if client is None:
            return
        try:
            client.drop_queue(self.qname)
        except Exception:
            pass
        client.close()

    async def _http(self, **cfg: Any) -> Any:
        from httpx import ASGITransport, AsyncClient

        fields = {"auth_mode": "none"}
        fields.update(cfg)
        app = create_app(queue=self.queue, api_config=PGMQAPIConfig(**fields))
        return AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        )

    async def test_send_returns_msg_id(self) -> None:
        http = await self._http()
        try:
            payload = {"task": "process_data"}
            resp = await http.post(
                f"/v1/queues/{self.qname}/messages",
                json={"message": payload, "headers": {"source": "api"}},
            )
            self.assertEqual(resp.status_code, 201)
            body = resp.json()
            self.assertIn("msg_id", body)
            self.assertIsInstance(body["msg_id"], int)
            self.assertGreater(body["msg_id"], 0)
            read = self.queue.read(self.qname)
            self.assertIsNotNone(read)
            self.assertEqual(read.msg_id, body["msg_id"])
            self.assertEqual(read.message, payload)
        finally:
            await http.aclose()

    async def test_send_batch(self) -> None:
        http = await self._http()
        try:
            resp = await http.post(
                f"/v1/queues/{self.qname}/messages/batch",
                json={"messages": [{"task": "a"}, {"task": "b"}]},
            )
            self.assertEqual(resp.status_code, 201)
            ids = resp.json()["msg_ids"]
            self.assertEqual(len(ids), 2)
            self.assertTrue(all(isinstance(item, int) for item in ids))
        finally:
            await http.aclose()

    async def test_send_batch_empty(self) -> None:
        http = await self._http()
        try:
            resp = await http.post(
                f"/v1/queues/{self.qname}/messages/batch",
                json={"messages": []},
            )
            self.assertEqual(resp.status_code, 201)
            self.assertEqual(resp.json(), {"msg_ids": []})
        finally:
            await http.aclose()

    async def test_send_batch_headers_length(self) -> None:
        http = await self._http()
        try:
            resp = await http.post(
                f"/v1/queues/{self.qname}/messages/batch",
                json={
                    "messages": [{"task": "a"}, {"task": "b"}],
                    "headers": [{"k": 1}],
                },
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"], "invalid_request")
        finally:
            await http.aclose()

    async def test_batch_cap_rejected(self) -> None:
        http = await self._http(max_batch=2)
        try:
            resp = await http.post(
                f"/v1/queues/{self.qname}/messages/batch",
                json={"messages": [{"n": 1}, {"n": 2}, {"n": 3}]},
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"], "invalid_request")
        finally:
            await http.aclose()

    async def test_send_message_must_be_object(self) -> None:
        http = await self._http()
        try:
            as_list = await http.post(
                f"/v1/queues/{self.qname}/messages",
                json={"message": ["not", "an", "object"]},
            )
            self.assertEqual(as_list.status_code, 422)
            as_scalar = await http.post(
                f"/v1/queues/{self.qname}/messages",
                json={"message": "scalar"},
            )
            self.assertEqual(as_scalar.status_code, 422)
        finally:
            await http.aclose()


@unittest.skipUnless(HAS_API, "pgmq[fastapi] extra is not installed")
class TestAPIConsume(unittest.IsolatedAsyncioTestCase):
    """HTTP read, pop, delete, archive, and set_vt. Skip when Postgres is down."""

    async def asyncSetUp(self) -> None:
        self.queue = _try_sync_queue()
        if self.queue is None:
            self.skipTest("Postgres is not available")
        self.qname = f"apir_{uuid.uuid4().hex[:8]}"
        self.queue.create_queue(self.qname)

    async def asyncTearDown(self) -> None:
        client = getattr(self, "queue", None)
        if client is None:
            return
        try:
            client.drop_queue(self.qname)
        except Exception:
            pass
        client.close()

    async def _http(self, **cfg: Any) -> Any:
        from httpx import ASGITransport, AsyncClient

        fields = {"auth_mode": "none"}
        fields.update(cfg)
        app = create_app(queue=self.queue, api_config=PGMQAPIConfig(**fields))
        return AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        )

    async def test_send_and_read_roundtrip(self) -> None:
        http = await self._http()
        try:
            payload = {"task": "roundtrip"}
            sent = await http.post(
                f"/v1/queues/{self.qname}/messages",
                json={"message": payload},
            )
            self.assertEqual(sent.status_code, 201)
            msg_id = sent.json()["msg_id"]
            read = await http.post(
                f"/v1/queues/{self.qname}/read",
                json={"qty": 1},
            )
            self.assertEqual(read.status_code, 200)
            messages = read.json()["messages"]
            self.assertEqual(len(messages), 1)
            item = messages[0]
            self.assertEqual(item["msg_id"], msg_id)
            self.assertEqual(item["message"], payload)
            self.assertIn("last_read_at", item)
            self.assertIsNotNone(item["last_read_at"])
        finally:
            await http.aclose()

    async def test_empty_read_is_200_empty_list(self) -> None:
        http = await self._http()
        try:
            resp = await http.post(f"/v1/queues/{self.qname}/read", json={})
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"messages": []})
        finally:
            await http.aclose()

    async def test_delete_unknown_message_404(self) -> None:
        http = await self._http()
        try:
            resp = await http.delete(f"/v1/queues/{self.qname}/messages/999999")
            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"], "message_not_found")
        finally:
            await http.aclose()

    async def test_pop_empty_list(self) -> None:
        http = await self._http()
        try:
            resp = await http.post(
                f"/v1/queues/{self.qname}/pop",
                json={"qty": 1},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"messages": []})
        finally:
            await http.aclose()

    async def test_archive(self) -> None:
        http = await self._http()
        try:
            sent = await http.post(
                f"/v1/queues/{self.qname}/messages",
                json={"message": {"n": 1}},
            )
            msg_id = sent.json()["msg_id"]
            archived = await http.post(
                f"/v1/queues/{self.qname}/messages/{msg_id}/archive"
            )
            self.assertEqual(archived.status_code, 204)
            empty = await http.post(f"/v1/queues/{self.qname}/read", json={})
            self.assertEqual(empty.json(), {"messages": []})
        finally:
            await http.aclose()

    async def test_set_vt(self) -> None:
        http = await self._http()
        try:
            sent = await http.post(
                f"/v1/queues/{self.qname}/messages",
                json={"message": {"n": 1}},
            )
            msg_id = sent.json()["msg_id"]
            resp = await http.post(
                f"/v1/queues/{self.qname}/messages/{msg_id}/vt",
                json={"vt": 60},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["msg_id"], msg_id)
            self.assertIn("vt", resp.json())
        finally:
            await http.aclose()

    async def test_qty_cap_rejected(self) -> None:
        http = await self._http(max_qty=2)
        try:
            resp = await http.post(
                f"/v1/queues/{self.qname}/read",
                json={"qty": 3},
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"], "invalid_request")
        finally:
            await http.aclose()

    async def test_read_poll_cap_rejected(self) -> None:
        http = await self._http()
        try:
            resp = await http.post(
                f"/v1/queues/{self.qname}/read-poll",
                json={"max_poll_seconds": 99},
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"], "poll_limit_exceeded")
        finally:
            await http.aclose()

    async def test_read_poll_empty_returns_list(self) -> None:
        http = await self._http()
        try:
            resp = await http.post(
                f"/v1/queues/{self.qname}/read-poll",
                json={"max_poll_seconds": 1, "poll_interval_ms": 100},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"messages": []})
        finally:
            await http.aclose()

    async def test_delete_batch(self) -> None:
        http = await self._http()
        try:
            sent = await http.post(
                f"/v1/queues/{self.qname}/messages/batch",
                json={"messages": [{"a": 1}, {"a": 2}]},
            )
            ids = sent.json()["msg_ids"]
            resp = await http.post(
                f"/v1/queues/{self.qname}/messages/delete",
                json={"msg_ids": ids},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(sorted(resp.json()["msg_ids"]), sorted(ids))
        finally:
            await http.aclose()
