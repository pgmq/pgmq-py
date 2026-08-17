# src/pgmq/api/factory.py
"""FastAPI factory, router, client adapter, and lazy init for the HTTP API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Dict, Optional, Set
import asyncio
import functools
import inspect
import logging

from fastapi import Depends, FastAPI, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.routing import APIRoute
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from starlette.requests import Request
from starlette.responses import JSONResponse

from pgmq.base import PGMQConfig
from pgmq.logger import LoggingManager, log_with_context

from pgmq.api.config import PGMQAPIConfig, load_api_config

logger = LoggingManager.get_logger("pgmq.api")

_bearer_scheme = HTTPBearer(auto_error=False)
_api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)


class APIError(Exception):
    """Application error mapped to a top-level ``{error, detail}`` body."""

    def __init__(self, status_code: int, error: str, detail: str) -> None:
        self.status_code = status_code
        self.error = error
        self.detail = detail
        super().__init__(detail)


def api_error(status_code: int, error: str, detail: str) -> JSONResponse:
    """Build the frozen application error response."""
    return JSONResponse(
        status_code=status_code,
        content={"error": error, "detail": detail},
    )


def queue_kwargs_from_config(config: PGMQConfig) -> Dict[str, Any]:
    """Copy PGMQConfig fields onto PGMQueue dataclass kwargs."""
    return {
        "conn_string": config.conn_string,
        "host": config.host,
        "port": config.port,
        "database": config.database,
        "username": config.username,
        "password": config.password,
        "delay": config.delay,
        "vt": config.vt,
        "pool_size": config.pool_size,
        "verbose": config.verbose,
        "log_filename": config.log_filename,
        "init_extension": config.init_extension,
        "structured_logging": config.structured_logging,
        "log_rotation": config.log_rotation,
        "log_rotation_size": config.log_rotation_size,
        "log_retention": config.log_retention,
    }


class _DefaultQueue:
    """Build SyncPGMQueue on first init so create_app binds if Postgres is down."""

    def __init__(self, kwargs: Dict[str, Any]) -> None:
        self._kwargs = kwargs
        self._inner: Any = None

    def init(self) -> None:
        """Construct the sync client. SyncPGMQueue opens the pool in post_init."""
        if self._inner is None:
            from pgmq.queue import PGMQueue as SyncPGMQueue

            self._inner = SyncPGMQueue(**self._kwargs)

    def close(self) -> None:
        """Close the owned sync client when it exists."""
        if self._inner is not None:
            closer = getattr(self._inner, "close", None)
            if closer is not None:
                closer()
            self._inner = None

    @property
    def pool(self) -> Any:
        if self._inner is None:
            return None
        return getattr(self._inner, "pool", None)

    @property
    def engine(self) -> Any:
        if self._inner is None:
            return None
        return getattr(self._inner, "engine", None)

    def __getattr__(self, name: str) -> Any:
        if self._inner is None:
            raise AttributeError(name)
        return getattr(self._inner, name)


def map_db_exception(exc: BaseException) -> Optional[APIError]:
    """Map a driver or connection failure to APIError. Return None if unknown."""
    seen: Set[int] = set()
    current: Optional[BaseException] = exc
    asyncpg_exc: Any = None
    try:
        import asyncpg.exceptions as asyncpg_exc
    except ImportError:
        asyncpg_exc = None

    while current is not None and id(current) not in seen:
        seen.add(id(current))
        name = type(current).__name__
        msg = str(current).lower()

        if asyncpg_exc is not None:
            if isinstance(current, asyncpg_exc.UndefinedTableError):
                return APIError(404, "queue_not_found", str(current))
            if isinstance(current, asyncpg_exc.UndefinedFunctionError):
                return APIError(501, "not_supported", str(current))
            if isinstance(
                current,
                (
                    asyncpg_exc.PostgresConnectionError,
                    asyncpg_exc.CannotConnectNowError,
                ),
            ):
                return APIError(503, "dependency_unavailable", str(current))

        if isinstance(current, (ConnectionError, TimeoutError)):
            return APIError(503, "dependency_unavailable", str(current))

        if name in ("UndefinedTableError", "UndefinedTable") or (
            "does not exist" in msg and "relation" in msg
        ):
            return APIError(404, "queue_not_found", str(current))
        if "does not exist" in msg and "function" not in msg:
            return APIError(404, "queue_not_found", str(current))
        if "too long" in msg:
            return APIError(400, "invalid_queue_name", str(current))
        if name == "UndefinedFunctionError" or "undefined function" in msg:
            return APIError(501, "not_supported", str(current))
        if name in ("OperationalError", "InterfaceError") or "could not connect" in msg:
            return APIError(503, "dependency_unavailable", str(current))

        nxt = current.__cause__
        if nxt is None:
            nxt = getattr(current, "orig", None)
        current = nxt
    return None


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _close_queue(queue: Any) -> None:
    closer = getattr(queue, "close", None)
    if closer is None:
        return
    if inspect.iscoroutinefunction(closer):
        await closer()
    else:
        await asyncio.to_thread(closer)


async def ensure_initialized(
    queue: Any,
    lock: asyncio.Lock,
    init_ok: Dict[str, bool],
    own_queue: bool,
) -> None:
    """Single-flight client init. Set init_ok only after init returns."""
    if init_ok["value"]:
        return
    async with lock:
        if init_ok["value"]:
            return
        init_fn = getattr(queue, "init", None)
        try:
            if init_fn is not None and callable(init_fn):
                if inspect.iscoroutinefunction(init_fn):
                    await init_fn()
                else:
                    await asyncio.to_thread(init_fn)
        except Exception as exc:
            if own_queue:
                await _close_queue(queue)
            mapped = map_db_exception(exc)
            if mapped is not None:
                raise mapped from exc
            raise APIError(503, "dependency_unavailable", str(exc)) from exc
        init_ok["value"] = True


async def ping_queue(queue: Any) -> None:
    """Run SELECT 1 on the client pool or engine. Used only by /ready."""
    try:
        await _ping_queue(queue)
    except APIError:
        raise
    except Exception as exc:
        mapped = map_db_exception(exc)
        if mapped is not None:
            raise mapped from exc
        raise APIError(503, "dependency_unavailable", str(exc)) from exc


async def _ping_queue(queue: Any) -> None:
    pool = getattr(queue, "pool", None)
    if pool is not None:
        acquire = getattr(pool, "acquire", None)
        if callable(acquire):
            ctx = acquire()
            if hasattr(ctx, "__aenter__"):
                async with ctx as conn:
                    await _maybe_await(conn.execute("SELECT 1"))
                return
        connection = getattr(pool, "connection", None)
        if callable(connection):

            def _sync_pool_ping() -> None:
                with pool.connection() as conn:
                    conn.execute("SELECT 1")

            await asyncio.to_thread(_sync_pool_ping)
            return
    engine = getattr(queue, "engine", None)
    if engine is not None:
        from sqlalchemy import text

        connector = engine.connect()
        if hasattr(connector, "__aenter__"):
            async with connector as conn:
                await _maybe_await(conn.execute(text("SELECT 1")))
            return

        def _sync_engine_ping(cm: Any = connector) -> None:
            with cm as conn:
                conn.execute(text("SELECT 1"))

        await asyncio.to_thread(_sync_engine_ping)
        return
    raise APIError(503, "dependency_unavailable", "queue has no pool or engine")


async def call_queue(method: Any, *args: Any, **kwargs: Any) -> Any:
    """Run one mixin call. Await async methods; thread sync methods."""
    try:
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        return await asyncio.to_thread(method, *args, **kwargs)
    except APIError:
        raise
    except ValueError as exc:
        msg = str(exc)
        if "not found" in msg.lower():
            raise APIError(404, "queue_not_found", msg) from exc
        raise APIError(400, "invalid_request", msg) from exc
    except Exception as exc:
        mapped = map_db_exception(exc)
        if mapped is not None:
            raise mapped from exc
        raise


def route_guard(func: Callable) -> Callable:
    """Wrap a route so APIError becomes a top-level {error, detail} body."""

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return await func(*args, **kwargs)
        except APIError as exc:
            return api_error(exc.status_code, exc.error, exc.detail)

    return wrapper


class _APIErrorRoute(APIRoute):
    """Map APIError from handlers and security deps so embeds get 401 JSON."""

    def get_route_handler(self) -> Callable:
        original = super().get_route_handler()

        async def _handler(request: Request) -> Any:
            try:
                return await original(request)
            except APIError as exc:
                return api_error(exc.status_code, exc.error, exc.detail)

        return _handler


def make_verify_key(
    api_config: PGMQAPIConfig,
    queue: Any,
    init_lock: asyncio.Lock,
    init_ok: Dict[str, bool],
    own_queue: bool,
) -> Callable:
    """Build the FastAPI security dependency for none/keys/use_db."""

    async def verify_key(
        credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme),
        api_key_header: Optional[str] = Security(_api_key_scheme),
    ) -> Optional[str]:
        from pgmq.api.auth import match_env_key, uses_db, verify_worker_key

        mode = (api_config.auth_mode or "none").lower()
        if mode == "none":
            return None

        presented = None
        if credentials is not None and credentials.credentials:
            presented = credentials.credentials
        elif api_key_header:
            presented = api_key_header

        if not presented:
            raise APIError(401, "unauthorized", "Missing API key")

        matched = match_env_key(api_config, presented)
        if matched is not None:
            log_with_context(
                logger,
                logging.INFO,
                "API key accepted",
                name=matched.get("name"),
                key_id=matched.get("id") or None,
            )
            return presented

        if uses_db(api_config):
            await ensure_initialized(queue, init_lock, init_ok, own_queue)
            await verify_worker_key(queue, api_config, presented)
            return presented

        raise APIError(401, "unauthorized", "Invalid API key")

    return verify_key


def register_exception_handlers(app: FastAPI) -> None:
    """Map APIError to the frozen JSON body. Optional for embedders."""

    @app.exception_handler(APIError)
    async def _api_error_handler(_request: Any, exc: APIError) -> JSONResponse:
        return api_error(exc.status_code, exc.error, exc.detail)


def _auth_enabled(api_config: PGMQAPIConfig) -> bool:
    """Return True when a request can be authenticated with configured keys."""
    mode = (api_config.auth_mode or "none").lower()
    if mode == "none":
        return False
    if api_config.api_key or api_config.named_keys or api_config.issued_keys:
        return True
    if api_config.use_db or mode in ("workers", "both"):
        return True
    return False


def _auth_field(api_config: PGMQAPIConfig) -> str:
    return "enabled" if _auth_enabled(api_config) else "disabled"


def _warn_if_auth_disabled(api_config: PGMQAPIConfig) -> None:
    if not _auth_enabled(api_config):
        log_with_context(
            logger,
            logging.WARNING,
            "API authentication is disabled",
            auth="disabled",
        )


def _install_openapi(app: FastAPI) -> None:
    def _openapi() -> Dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title,
            version=app.version,
            routes=app.routes,
        )
        components = schema.setdefault("components", {})
        schemes = components.setdefault("securitySchemes", {})
        schemes["HTTPBearer"] = {"type": "http", "scheme": "bearer"}
        schemes["APIKeyHeader"] = {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
        }
        app.openapi_schema = schema
        return app.openapi_schema

    app.openapi = _openapi  # type: ignore[method-assign]


def create_router(
    queue: Any,
    api_config: Optional[PGMQAPIConfig] = None,
    own_queue: bool = False,
) -> Any:
    """
    Build the /v1 APIRouter. ``queue`` is required.

    The router owns the init lock and the init_ok flag.
    """
    from fastapi import APIRouter

    from pgmq.api.routes import register_protected_routes, register_public_routes

    if queue is None:
        raise TypeError("create_router() missing required argument: 'queue'")

    resolved = api_config or load_api_config()
    from pgmq.api.auth import require_pepper

    require_pepper(resolved)
    init_lock = asyncio.Lock()
    init_ok = {"value": False}
    verify = make_verify_key(resolved, queue, init_lock, init_ok, own_queue)

    parent = APIRouter(prefix="/v1", route_class=_APIErrorRoute)
    public = APIRouter(route_class=_APIErrorRoute)
    register_public_routes(
        public,
        queue=queue,
        api_config=resolved,
        init_lock=init_lock,
        init_ok=init_ok,
        own_queue=own_queue,
        auth_status=_auth_field(resolved),
    )
    parent.include_router(public)

    protected = APIRouter(
        dependencies=[Depends(verify)],
        route_class=_APIErrorRoute,
    )
    register_protected_routes(
        protected,
        queue=queue,
        api_config=resolved,
        init_lock=init_lock,
        init_ok=init_ok,
        own_queue=own_queue,
    )
    parent.include_router(protected)

    parent.verify_key = verify  # type: ignore[attr-defined]
    return parent


def create_app(
    queue: Any = None,
    config: Optional[PGMQConfig] = None,
    api_config: Optional[PGMQAPIConfig] = None,
) -> FastAPI:
    """Build a standalone FastAPI application."""
    from pgmq import __version__
    from pgmq.api.schemas import HealthResponse

    resolved = api_config or load_api_config()
    from pgmq.api.auth import require_pepper

    require_pepper(resolved)
    own_queue = queue is None
    if own_queue:
        queue = _DefaultQueue(queue_kwargs_from_config(config or PGMQConfig()))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log_with_context(
            logger,
            logging.INFO,
            "PGMQ API starting",
            host=getattr(getattr(queue, "config", None), "host", None),
            database=getattr(getattr(queue, "config", None), "database", None),
        )
        try:
            yield
        finally:
            if own_queue:
                await _close_queue(queue)

    app = FastAPI(title="PGMQ HTTP API", version=__version__, lifespan=lifespan)
    app.state.queue = queue
    app.state.own_queue = own_queue
    app.state.api_config = resolved

    register_exception_handlers(app)
    router = create_router(queue=queue, api_config=resolved, own_queue=own_queue)
    app.state.verify_key = router.verify_key
    app.include_router(router)

    @app.get("/health", response_model=HealthResponse, include_in_schema=False)
    async def health_alias() -> HealthResponse:
        return HealthResponse(
            status="ok",
            version=__version__,
            auth=_auth_field(resolved),
        )

    if resolved.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=resolved.cors_origins,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "X-API-Key", "Content-Type"],
        )

    _install_openapi(app)
    _warn_if_auth_disabled(resolved)
    return app
