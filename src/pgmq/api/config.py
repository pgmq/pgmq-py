# src/pgmq/api/config.py
"""Load HTTP API settings from defaults, YAML, environment, and CLI flags."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Union
import os
import re
import uuid

import yaml


_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")
_NAMED_KEY_PREFIX = "PGMQ_NAMED_KEY_"
_ISSUED_KEY_PREFIX = "PGMQ_KEY_"
_ISSUED_NAME_PREFIX = "PGMQ_KEY_NAME_"
_REVOKED_IDS_VAR = "PGMQ_REVOKED_IDS"

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8080
DEFAULT_AUTH_MODE = "none"
DEFAULT_MAX_POLL_SECONDS = 20
DEFAULT_MAX_QTY = 100
DEFAULT_MAX_BATCH = 1000
DEFAULT_WORKERS_SCHEMA = "pgmq_api"
DEFAULT_WORKERS_TABLE = "api_keys"


@dataclass
class PGMQAPIConfig:
    """
    HTTP listen, auth, and limit settings for the PGMQ API.

    Load with ``load_api_config``. Direct construction is valid for tests
    and embedders. Secrets use ``repr=False``.
    """

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    auth_mode: str = DEFAULT_AUTH_MODE
    api_key: Optional[str] = field(default=None, repr=False)
    named_keys: Dict[str, str] = field(default_factory=dict, repr=False)
    issued_keys: List[Dict[str, str]] = field(default_factory=list, repr=False)
    revoked_ids: Set[str] = field(default_factory=set)
    use_db: bool = False
    pepper: Optional[str] = field(default=None, repr=False)
    workers_create_table: bool = False
    workers_schema: str = DEFAULT_WORKERS_SCHEMA
    workers_table: str = DEFAULT_WORKERS_TABLE
    max_poll_seconds: int = DEFAULT_MAX_POLL_SECONDS
    max_qty: int = DEFAULT_MAX_QTY
    max_batch: int = DEFAULT_MAX_BATCH
    cors_origins: Optional[List[str]] = None
    allow_insecure_config: bool = False


def parse_cors_origins(value: Union[str, List[Any], None]) -> Optional[List[str]]:
    """Split a comma-separated origin list and strip whitespace."""
    if value is None:
        return None
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = [str(item) for item in value]
    origins = [part.strip() for part in parts if str(part).strip()]
    return origins or None


def _interpolate(value: Any) -> Any:
    """Replace ``${VAR}`` in YAML strings with environment values."""
    if isinstance(value, str):

        def _replace(match: re.Match) -> str:
            return os.environ.get(match.group(1), match.group(0))

        return _VAR_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {key: _interpolate(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_interpolate(item) for item in value]
    return value


def load_env_file(path: Optional[str] = None) -> Optional[str]:
    """
    Load KEY=VALUE pairs from a .env file into os.environ.

    Existing environment values win. Missing default ``.env`` is ignored.
    """
    resolved = path if path is not None else os.getenv("PGMQ_API_ENV")
    if resolved is None:
        resolved = ".env" if os.path.isfile(".env") else None
    if not resolved:
        return None
    if not os.path.isfile(resolved):
        if path is not None:
            raise FileNotFoundError(f"Env file not found: {resolved}")
        return None
    with open(resolved, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value
    return resolved


def parse_named_keys(blob: Optional[str] = None) -> Dict[str, str]:
    """Parse ``name:secret`` pairs and ``PGMQ_NAMED_KEY_*`` env vars."""
    keys: Dict[str, str] = {}
    text = blob if blob is not None else os.getenv("PGMQ_API_KEYS")
    if text:
        for part in text.split(","):
            item = part.strip()
            if not item:
                continue
            if ":" in item:
                name, _, secret = item.partition(":")
            elif "=" in item:
                name, _, secret = item.partition("=")
            else:
                continue
            name = name.strip()
            secret = secret.strip()
            if name and secret:
                keys[name] = secret
    for env_key, env_val in os.environ.items():
        if env_key.startswith(_NAMED_KEY_PREFIX) and env_val:
            keys[env_key[len(_NAMED_KEY_PREFIX) :]] = env_val
    return keys


def named_key_env_name(name: str) -> str:
    """Build a ``PGMQ_NAMED_KEY_*`` variable for a key name."""
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name).strip("_")
    if not cleaned:
        raise ValueError(f"Invalid key name: {name!r}")
    return f"{_NAMED_KEY_PREFIX}{cleaned}"


def normalize_key_id(value: str) -> str:
    """Return a canonical UUID string. Accept hyphenated or hex form."""
    try:
        return str(uuid.UUID(str(value).strip()))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError(f"Invalid key id: {value!r}") from exc


def issued_key_env_name(key_id: str) -> str:
    """Build ``PGMQ_KEY_<hex>`` for a UUID id."""
    return f"{_ISSUED_KEY_PREFIX}{uuid.UUID(normalize_key_id(key_id)).hex}"


def issued_key_name_env(key_id: str) -> str:
    """Build ``PGMQ_KEY_NAME_<hex>`` for a UUID id."""
    return f"{_ISSUED_NAME_PREFIX}{uuid.UUID(normalize_key_id(key_id)).hex}"


def parse_revoked_ids(blob: Optional[str] = None) -> Set[str]:
    """Parse ``PGMQ_REVOKED_IDS`` as a set of canonical UUIDs."""
    text = blob if blob is not None else os.getenv(_REVOKED_IDS_VAR)
    revoked: Set[str] = set()
    if not text:
        return revoked
    for part in text.split(","):
        item = part.strip()
        if not item:
            continue
        try:
            revoked.add(normalize_key_id(item))
        except ValueError:
            continue
    return revoked


def revoke_key_in_env_file(path: str, key_id: str) -> bool:
    """
    Revoke a UUID key in a .env file.

    Comment out ``PGMQ_KEY_<hex>`` and add the id to ``PGMQ_REVOKED_IDS``.
    Return True when the key line was found.
    """
    canonical = normalize_key_id(key_id)
    env_name = issued_key_env_name(canonical)
    name_var = issued_key_name_env(canonical)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Env file not found: {path}")
    with open(path, encoding="utf-8") as handle:
        lines = handle.readlines()
    found = False
    revoked_written = False
    new_lines: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(f"{env_name}=") or stripped.startswith(f"{name_var}="):
            new_lines.append(f"# revoked {canonical}\n")
            new_lines.append(f"# {stripped}\n")
            found = True
            continue
        if stripped.startswith(f"{_REVOKED_IDS_VAR}="):
            existing = stripped.split("=", 1)[1]
            ids: List[str] = []
            for part in existing.split(","):
                item = part.strip()
                if not item:
                    continue
                try:
                    ids.append(normalize_key_id(item))
                except ValueError:
                    ids.append(item)
            if canonical not in ids:
                ids.append(canonical)
            new_lines.append(f"{_REVOKED_IDS_VAR}={','.join(ids)}\n")
            revoked_written = True
            continue
        new_lines.append(line)
    if not revoked_written:
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines.append("\n")
        new_lines.append(f"{_REVOKED_IDS_VAR}={canonical}\n")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.writelines(new_lines)
    os.chmod(path, 0o600)
    return found


def parse_issued_keys() -> List[Dict[str, str]]:
    """Load issued keys from ``PGMQ_KEY_<hex>`` and optional name vars."""
    keys: List[Dict[str, str]] = []
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(_ISSUED_KEY_PREFIX):
            continue
        if env_key.startswith(_ISSUED_NAME_PREFIX):
            continue
        suffix = env_key[len(_ISSUED_KEY_PREFIX) :]
        if len(suffix) != 32:
            continue
        try:
            key_id = normalize_key_id(suffix)
        except ValueError:
            continue
        if not env_val:
            continue
        name = os.environ.get(issued_key_name_env(key_id), "")
        keys.append(
            {
                "id": key_id,
                "name": name or key_id,
                "secret": env_val,
            }
        )
    return keys


def _refuse_yaml_secrets(data: Dict[str, Any]) -> None:
    """Secrets belong in .env, not in YAML."""
    auth = data.get("auth") or {}
    if not isinstance(auth, dict):
        return
    if auth.get("static_key") or auth.get("pepper") or auth.get("keys"):
        raise ValueError(
            "Do not put API keys or pepper in YAML. "
            "Put secrets in .env (PGMQ_API_KEY, PGMQ_API_KEYS, "
            "PGMQ_NAMED_KEY_<name>)."
        )


def _apply_yaml(cfg: PGMQAPIConfig, data: Dict[str, Any]) -> None:
    listen = data.get("listen") or {}
    if isinstance(listen, dict):
        if listen.get("host") is not None:
            cfg.host = str(listen["host"])
        if listen.get("port") is not None:
            cfg.port = int(listen["port"])
    auth = data.get("auth") or {}
    if isinstance(auth, dict):
        if auth.get("mode") is not None:
            cfg.auth_mode = str(auth["mode"])
        if "use_db" in auth:
            cfg.use_db = bool(auth["use_db"])
        store = auth.get("store") or {}
        if isinstance(store, dict):
            if "create_table" in store:
                cfg.workers_create_table = bool(store["create_table"])
            if store.get("schema") is not None:
                cfg.workers_schema = str(store["schema"])
            if store.get("table") is not None:
                cfg.workers_table = str(store["table"])
        workers = auth.get("workers") or {}
        if isinstance(workers, dict):
            if "create_table" in workers:
                cfg.workers_create_table = bool(workers["create_table"])
            if workers.get("schema") is not None:
                cfg.workers_schema = str(workers["schema"])
            if workers.get("table") is not None:
                cfg.workers_table = str(workers["table"])
    limits = data.get("limits") or {}
    if isinstance(limits, dict):
        if limits.get("max_poll_seconds") is not None:
            cfg.max_poll_seconds = int(limits["max_poll_seconds"])
        if limits.get("max_qty") is not None:
            cfg.max_qty = int(limits["max_qty"])
        if limits.get("max_batch") is not None:
            cfg.max_batch = int(limits["max_batch"])
    if "allow_insecure_config" in data:
        cfg.allow_insecure_config = bool(data["allow_insecure_config"])
    if data.get("cors_origins") is not None:
        cfg.cors_origins = parse_cors_origins(data["cors_origins"])


def _apply_env(cfg: PGMQAPIConfig) -> None:
    host = os.getenv("PGMQ_API_HOST")
    if host:
        cfg.host = host
    port = os.getenv("PGMQ_API_PORT")
    if port:
        cfg.port = int(port)
    mode = os.getenv("PGMQ_API_AUTH_MODE")
    if mode:
        cfg.auth_mode = mode
    use_db = os.getenv("PGMQ_API_USE_DB")
    if use_db is not None:
        cfg.use_db = use_db.strip().lower() in ("1", "true", "yes")
    key = os.getenv("PGMQ_API_KEY")
    if key:
        cfg.api_key = key
    named = parse_named_keys()
    if named:
        merged = dict(cfg.named_keys)
        merged.update(named)
        cfg.named_keys = merged
    issued = parse_issued_keys()
    if issued:
        cfg.issued_keys = list(cfg.issued_keys) + issued
    revoked = parse_revoked_ids()
    if revoked:
        cfg.revoked_ids = set(cfg.revoked_ids) | revoked
    pepper = os.getenv("PGMQ_API_KEY_PEPPER")
    if pepper:
        cfg.pepper = pepper
    poll = os.getenv("PGMQ_API_MAX_POLL_SECONDS")
    if poll:
        cfg.max_poll_seconds = int(poll)
    qty = os.getenv("PGMQ_API_MAX_QTY")
    if qty:
        cfg.max_qty = int(qty)
    batch = os.getenv("PGMQ_API_MAX_BATCH")
    if batch:
        cfg.max_batch = int(batch)
    origins = os.getenv("PGMQ_API_CORS_ORIGINS")
    if origins:
        cfg.cors_origins = parse_cors_origins(origins)


def load_api_config(
    path: Optional[str] = None,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
    auth_mode: Optional[str] = None,
) -> PGMQAPIConfig:
    """
    Load API config. Order: .env, defaults, YAML if present, env, CLI flags.

    ``path`` or ``PGMQ_API_CONFIG`` selects the YAML file. A missing file is
    ignored when the path comes from the environment. A missing explicit
    ``path`` argument raises FileNotFoundError. Secrets come from ``.env``.
    """
    load_env_file()
    cfg = PGMQAPIConfig()
    resolved = path if path is not None else os.getenv("PGMQ_API_CONFIG")
    if resolved:
        if os.path.isfile(resolved):
            with open(resolved, encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle)
            if loaded is None:
                loaded = {}
            if not isinstance(loaded, dict):
                raise ValueError(f"Config file {resolved} must be a mapping")
            _refuse_yaml_secrets(loaded)
            _apply_yaml(cfg, _interpolate(loaded))
        elif path is not None:
            raise FileNotFoundError(f"Config file not found: {resolved}")
    _apply_env(cfg)
    if host is not None:
        cfg.host = host
    if port is not None:
        cfg.port = port
    if auth_mode is not None:
        cfg.auth_mode = auth_mode
    cfg.auth_mode = _normalize_mode(cfg.auth_mode)
    if cfg.auth_mode in ("workers", "both"):
        cfg.use_db = True
        cfg.auth_mode = "keys"
    return cfg


def _normalize_mode(mode: str) -> str:
    """Map aliases. ``static`` is env keys. ``workers``/``both`` enable the DB store."""
    value = (mode or "none").strip().lower()
    if value == "static":
        return "keys"
    return value
