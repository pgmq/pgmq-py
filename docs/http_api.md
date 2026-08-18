# HTTP API

!!! warning "Protect this process"

    Auth is optional. Mode `none` starts with every data-plane route open and
    logs a WARNING.

    - Put keys in `.env` (`PGMQ_API_KEY` or named keys). Do not put secrets in YAML.
    - Bind `127.0.0.1` or a private network. Do not publish purge or drop on
      the public internet.
    - Terminate TLS at the proxy. The process does not serve HTTPS.

The HTTP layer is an optional FastAPI adapter. It calls the existing Python
clients. It does not add PGMQ SQL and it is **not** wire-compatible with
[pgmq-rest](https://github.com/eichenroth/pgmq-rest) RPC tuples.

v1 does **not** ship a first-party Docker image.

## Install

```bash
pip install 'pgmq[fastapi]'
```

The `fastapi` extra installs FastAPI, uvicorn, and PyYAML. It does **not** install
`asyncpg` or SQLAlchemy. Combine extras when you inject those clients:

```bash
pip install 'pgmq[fastapi]'
pip install 'pgmq[fastapi,async]'
pip install 'pgmq[fastapi,sqlalchemy]'
pip install 'pgmq[fastapi,sqlalchemy-async]'
```

The standalone default client is `SyncPGMQueue` (always installed).

## Run

The process uses `PG_*` / `DATABASE_URL` for Postgres, same as the library.

```bash
# 1. Module CLI. Sets timeout_keep_alive to max_poll_seconds + 10.
python -m pgmq.api --host 127.0.0.1 --port 8080

# Same entry as the console script
pgmq-api --host 127.0.0.1 --port 8080

# 2. ASGI target. Pass keep-alive yourself.
uvicorn pgmq.api.asgi:app --host 127.0.0.1 --port 8080 --timeout-keep-alive 30

# 3. Factory
uvicorn pgmq.api:create_app --factory --host 127.0.0.1 --port 8080 --timeout-keep-alive 30
```

Flags: `--host`, `--port`, `--auth-mode`, `--config`, `--reload`, `--log-level`.
There is no `--password`. Database credentials stay in the environment.

When `auth.mode` is `none`, the CLI also writes a WARNING to stderr.

OpenAPI stays at `/docs`, `/redoc`, and `/openapi.json`. Those paths are
public. Use the Swagger **Authorize** button for Bearer or `X-API-Key`.

## Config file

A YAML file is **not** required. Defaults plus env plus CLI flags are enough.

```bash
python -m pgmq.api init-config
python -m pgmq.api init-config --path ./pgmq-api.yaml
python -m pgmq.api init-config --path ./pgmq-api.yaml --force
```

`init-config` writes mode `0600` and does not write secrets. It also writes a
sibling `.env.example`. Defaults in the starter file: `auth.mode: none`,
`use_db: false`, `listen.host: 127.0.0.1`. The command refuses to overwrite a
YAML file unless you pass `--force`.

Sample without secrets: `example/pgmq-api.yaml`.

Load order: **`.env` → code defaults → YAML if present → environment → serve CLI
flags**. YAML strings support `${VAR}` interpolation. Path: `--config` or
`PGMQ_API_CONFIG`. Env file path: `PGMQ_API_ENV` or `.env`.

YAML must not hold `static_key`, `pepper`, or key maps. Those belong in `.env`.

## Environment

HTTP settings use the `PGMQ_API_*` prefix. They are **not** fields on
`PGMQConfig`. Database settings stay on `PG_*` / `DATABASE_URL`.

| Variable | Purpose | Default |
|----------|---------|---------|
| `PGMQ_API_CONFIG` | Path to YAML | unset |
| `PGMQ_API_HOST` | Bind address | `0.0.0.0` |
| `PGMQ_API_PORT` | Listen port | `8080` |
| `PGMQ_API_AUTH_MODE` | `none` \| `keys` (`static` is an alias) | `none` |
| `PGMQ_API_KEY` | Shared API key | unset |
| `PGMQ_API_KEYS` | Named keys `name:secret,name2:secret2` | unset |
| `PGMQ_NAMED_KEY_<name>` | One named key | unset |
| `PGMQ_API_ENV` | Path to `.env` | `.env` if present |
| `PGMQ_API_USE_DB` | Optional hashed-key table (`true`/`false`) | `false` |
| `PGMQ_API_KEY_PEPPER` | HMAC pepper. Required only when `use_db` is true | unset |
| `PGMQ_API_MAX_POLL_SECONDS` | Long-poll cap | `20` |
| `PGMQ_API_MAX_QTY` | Max `qty` on read / read-poll / pop | `100` |
| `PGMQ_API_MAX_BATCH` | Max `messages` length on send-batch | `1000` |
| `PGMQ_API_CORS_ORIGINS` | Comma-separated origins; unset = no CORS | unset |

`PGMQ_API_CORS_ORIGINS` allows `GET`, `POST`, `PUT`, `DELETE`, `OPTIONS` and
headers `Authorization`, `X-API-Key`, `Content-Type`. Whitespace around origins
is stripped.

## Auth

Send the key in `Authorization: Bearer <key>` or `X-API-Key: <key>`. Do not put
the key in the query string. The server compares with `hmac.compare_digest`.
It never logs the raw key. Accepted named keys log `name` only.

`GET /health`, `GET /v1/health`, and `GET /v1/ready` stay public so probes do
not need the secret.

| `auth.mode` | Behavior |
|-------------|----------|
| `none` | All data-plane routes open. WARNING at start. Health `auth` is `disabled`. |
| `keys` | Accept `PGMQ_API_KEY` and any named key from `.env`. |

`static` is an alias of `keys`. Postgres is **not** required for auth.

Put secrets in `.env`:

```bash
PGMQ_API_KEY=shared-optional
PGMQ_API_KEYS=billing:secret-one,ingest:secret-two
# or
PGMQ_NAMED_KEY_billing=secret-one
```

Issue a named key from the CLI. Each key gets a UUID `id`. Default is dry-run:

```bash
python -m pgmq.api issue-key --name billing
python -m pgmq.api issue-key --name billing --apply
python -m pgmq.api revoke-key --id <uuid>
python -m pgmq.api revoke-key --id <uuid> --apply
```

`--apply` on issue writes `PGMQ_KEY_<uuidhex>` and `PGMQ_KEY_NAME_<uuidhex>`
to the env file (mode `0600`) and prints the raw key **once**.

`--apply` on revoke comments out that key and adds the UUID to
`PGMQ_REVOKED_IDS`. A revoked key is rejected even if the secret is still
present. There is no HTTP route to mint, list, or revoke keys.

### Optional database store

Set `auth.use_db: true` in YAML (or `PGMQ_API_USE_DB=true`) only if you want
hashed keys in Postgres. Then set `PGMQ_API_KEY_PEPPER` in `.env`. The table
is `pgmq_api.api_keys` (not the `pgmq` extension schema). Default is off.

## Endpoints

All application routes live under `/v1`. Auth (when configured) applies to
every `/v1` route except health and ready. The app-root `GET /health` alias is
also public.

### Service

| Method | Path | Success |
|--------|------|---------|
| `GET` | `/health` and `/v1/health` | `200` `{"status","version","auth"}`. No database. No `init()`. |
| `GET` | `/v1/ready` | `200` `{"status":"ok"}` after lazy init + `SELECT 1`. Failure is `503`. |
| `GET` | `/docs`, `/redoc`, `/openapi.json` | FastAPI built-in. Public. |

`/v1/health` is liveness. `/v1/ready` is readiness. The process binds even if
Postgres is down.

### Queues

| Method | Path | Client call | Success |
|--------|------|-------------|---------|
| `PUT` | `/v1/queues/{queue}` | `create_queue(queue, unlogged=...)` | `201` `{"queue_name","unlogged"}` |
| `GET` | `/v1/queues` | `list_queues()` | `200` `{"queues":[...]}` |
| `DELETE` | `/v1/queues/{queue}` | `drop_queue(queue)` | `204` if dropped; `404` if missing |
| `POST` | `/v1/queues/{queue}/purge` | `purge(queue)` | `200` `{"purged": int}` |
| `GET` | `/v1/metrics` | `metrics_all()` | `200` `{"queues":[...]}` |
| `GET` | `/v1/queues/{queue}/metrics` | `metrics(queue)` | `200` metrics object; `404` if missing |

`PUT` **ensures the queue exists**. It always returns `201`. Query flag
`?unlogged=true` applies only on first create. The response `unlogged` field
**echoes the request**. It is not a read-back. A second PUT with a different
flag is a no-op and is not `409`.

Queue names cannot contain `/`. PGMQ validates the name (max 47 characters).

### Send

| Method | Path | Success |
|--------|------|---------|
| `POST` | `/v1/queues/{queue}/messages` | `201` `{"msg_id": int}` |
| `POST` | `/v1/queues/{queue}/messages/batch` | `201` `{"msg_ids":[int, ...]}` |

Send body:

```json
{"message": {"task": "process_data"}, "headers": {"source": "api"}, "delay": 0}
```

- `message` must be a JSON **object**. An array or scalar is `422`.
- `headers` is an optional object.
- `delay` is a non-negative integer (seconds) or an ISO-8601 datetime. The
  legacy Python `tz` alias is not exposed.

Batch body:

```json
{"messages": [{"task": "a"}, {"task": "b"}], "headers": [{"k": 1}, {"k": 2}], "delay": 0}
```

Empty `messages: []` returns `201 {"msg_ids": []}`. If `headers` is present and
the lengths differ, the response is `400 invalid_request`. A batch longer than
`max_batch` is `400` **before** the mixin runs. If `send` returns `-1`, the
response is `500 send_failed`.

### Read, pop, delete, archive, set_vt

These change visibility or delete rows, so they are `POST` (except single
delete).

| Method | Path | Success |
|--------|------|---------|
| `POST` | `/v1/queues/{queue}/read` | `200` `{"messages":[...]}` |
| `POST` | `/v1/queues/{queue}/read-poll` | `200` `{"messages":[...]}` |
| `POST` | `/v1/queues/{queue}/pop` | `200` `{"messages":[...]}` |
| `DELETE` | `/v1/queues/{queue}/messages/{msg_id}` | `204` / `404` |
| `POST` | `/v1/queues/{queue}/messages/delete` | `200` `{"msg_ids":[...]}` |
| `POST` | `/v1/queues/{queue}/messages/{msg_id}/archive` | `204` / `404` |
| `POST` | `/v1/queues/{queue}/messages/archive` | `200` `{"msg_ids":[...]}` |
| `POST` | `/v1/queues/{queue}/messages/{msg_id}/vt` | `200` message object; `404` if missing |

Read body (defaults shown):

```json
{"vt": 30, "qty": 1, "conditional": null}
```

Omit `vt` to use the client default (`self.vt`). `"vt": 0` also uses that
default because the mixin treats `0` as falsy.

Read-poll adds `max_poll_seconds` (default `5`) and `poll_interval_ms`
(default `100`). Pop body is `{"qty": 1}`.

**Empty read / pop / poll is `200 {"messages": []}`.** It is never `204` and
never `404`. The HTTP API always wraps results in a list. It does not leak the
Python `qty==1` single-object return type.

Message objects use named fields, including `last_read_at`:

```json
{
  "msg_id": 123,
  "read_ct": 1,
  "enqueued_at": "2026-08-17T12:00:00+00:00",
  "last_read_at": "2026-08-17T12:00:05+00:00",
  "vt": "2026-08-17T12:00:35+00:00",
  "message": {"task": "process_data"},
  "headers": {"source": "api"}
}
```

`message` on the **response** is arbitrary JSON (`Any`). Send-time `message`
must still be an object. `headers` and `last_read_at` may be `null`.

`set_vt` is single-id only. Body: `{"vt": 60}` or an ISO-8601 datetime. There
is no batch `set_vt` in v1.

`conditional` is an optional JSON object on read and read-poll only. If the
extension lacks the function, the API returns `501 not_supported`.

v1 does **not** expose topic routing, FIFO grouped reads, notify admin, or
SSE.

## Caps and long poll

| Cap | Default | Over limit |
|-----|---------|------------|
| `qty` on read / read-poll / pop | `100` | `400 invalid_request` |
| batch `messages` length | `1000` | `400 invalid_request` |
| `max_poll_seconds` | `20` | `400 poll_limit_exceeded` (reject, do not clamp) |

Long poll occupies **one pool slot and an open mixin transaction** for the
whole wait. Size `pool_size` as `concurrent_pollers + write_concurrency`.

Set uvicorn `--timeout-keep-alive` to at least the poll cap plus 10 seconds
(30 when the cap is 20). `python -m pgmq.api` sets this for you. Reverse-proxy
`proxy_read_timeout` must be greater than the cap.

## Errors

Application errors use a frozen object:

```json
{"error": "queue_not_found", "detail": "Queue 'jobs' does not exist"}
```

FastAPI request validation stays the framework shape (`422`):

```json
{"detail": [{"type": "int_parsing", "loc": ["body", "qty"], "msg": "...", "input": "x"}]}
```

| Situation | HTTP | `error` |
|-----------|------|---------|
| Missing or wrong API key | `401` | `unauthorized` |
| Invalid request / qty or batch over cap | `400` | `invalid_request` |
| Poll seconds over cap | `400` | `poll_limit_exceeded` |
| Queue missing | `404` | `queue_not_found` |
| Message missing (delete / archive / set_vt) | `404` | `message_not_found` |
| Conditional/read function missing | `501` | `not_supported` |
| Database down or auth table missing | `503` | `dependency_unavailable` |
| `send` returned `-1` | `500` | `send_failed` |
| Pydantic / path / query validation | `422` | FastAPI `detail` list |

## Embed the router

`create_router(queue=...)` requires `queue`. It does not build a client. Auth,
caps, and `{error, detail}` live on the router. You own `init()` / `close()`.

```python
from fastapi import FastAPI
from pgmq import AsyncPGMQueue
from pgmq.api import create_router, PGMQAPIConfig

queue = AsyncPGMQueue()  # or SyncPGMQueue, SQLAlchemyPGMQueue, SQLAlchemyAsyncPGMQueue
await queue.init()

app = FastAPI()
app.include_router(
    create_router(
        queue=queue,
        api_config=PGMQAPIConfig(auth_mode="keys", api_key="..."),
    )
)
```

The adapter awaits coroutine methods and runs sync methods with
`asyncio.to_thread`. Unpack `PGMQConfig` with
`queue_kwargs_from_config`. Do not call `SyncPGMQueue(config=...)` or
`AsyncPGMQueue(config=...)`.

`create_app(queue=...)` builds a standalone app. If you omit `queue`, it
constructs a lazy `SyncPGMQueue` on first `/ready` or data-plane request so
the process can bind when Postgres is down.

## Not compatible with pgmq-rest

pgmq-rest uses `POST /api/v1/<function>` and positional tuples that drop
`last_read_at`. This API uses resource paths under `/v1` and named JSON
objects. Pointing an existing pgmq-rest client at this server will not work.
