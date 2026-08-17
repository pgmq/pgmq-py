# src/pgmq/api/__main__.py
"""CLI for the PGMQ HTTP API: serve, init-config, and issue-key."""

from typing import List, Optional
import argparse
import os
import sys

from pgmq.api.config import load_api_config

STARTER_YAML = """\
listen:
  host: "127.0.0.1"
  port: 8080
auth:
  mode: none          # none | keys
  use_db: false       # optional hashed keys in Postgres
limits:
  max_poll_seconds: 20
  max_qty: 100
  max_batch: 1000
"""

STARTER_ENV = """\
# Secrets for pgmq-api. Do not commit this file.
# PGMQ_API_KEY=change-me
# Issued keys (prefer issue-key --apply):
# PGMQ_KEY_<uuidhex>=change-me
# PGMQ_KEY_NAME_<uuidhex>=billing
# PGMQ_REVOKED_IDS=
"""

AUTH_DISABLED_WARNING = (
    "WARNING: API authentication is disabled (auth.mode=none). "
    "Do not bind this process to a public interface."
)


def _write_0600(path: str, content: str, force: bool) -> int:
    """Write a file with mode 0600. Refuse overwrite unless force."""
    if os.path.exists(path) and not force:
        print(f"Refusing to overwrite existing file: {path}", file=sys.stderr)
        return 1
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(content)
    os.chmod(path, 0o600)
    print(f"Wrote {path}")
    return 0


def write_starter_config(path: str, force: bool = False) -> int:
    """Write starter YAML and a sibling .env.example. No secrets."""
    rc = _write_0600(path, STARTER_YAML, force)
    if rc != 0:
        return rc
    env_example = os.path.join(os.path.dirname(os.path.abspath(path)), ".env.example")
    _write_0600(env_example, STARTER_ENV, force=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the serve / init-config argument parser."""
    parser = argparse.ArgumentParser(
        prog="pgmq-api",
        description="Run the PGMQ HTTP API or write a starter config file.",
    )
    parser.add_argument("--host", help="Bind address (overrides YAML and env)")
    parser.add_argument("--port", type=int, help="Listen port")
    parser.add_argument(
        "--auth-mode",
        choices=["none", "keys", "static"],
        help="Auth mode (static is an alias of keys)",
    )
    parser.add_argument("--config", help="Path to YAML config")
    parser.add_argument("--reload", action="store_true", help="Auto-reload (dev)")
    parser.add_argument("--log-level", default="info", help="Uvicorn log level")
    sub = parser.add_subparsers(dest="command")
    init_cmd = sub.add_parser("init-config", help="Write a starter YAML file")
    init_cmd.add_argument(
        "--path",
        default="pgmq-api.yaml",
        help="Output path (default: pgmq-api.yaml)",
    )
    init_cmd.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing file",
    )
    issue_cmd = sub.add_parser(
        "issue-key",
        help="Create a named API key (dry-run by default)",
    )
    issue_cmd.add_argument("--name", required=True, help="Any key name")
    issue_cmd.add_argument(
        "--env-file",
        default=".env",
        help="Env file to append (default: .env)",
    )
    issue_cmd.add_argument(
        "--apply",
        action="store_true",
        help="Append the key to the env file and print it once",
    )
    revoke_cmd = sub.add_parser(
        "revoke-key",
        help="Revoke a key by UUID (dry-run by default)",
    )
    revoke_cmd.add_argument("--id", required=True, dest="key_id", help="Key UUID")
    revoke_cmd.add_argument(
        "--env-file",
        default=".env",
        help="Env file to update (default: .env)",
    )
    revoke_cmd.add_argument(
        "--apply",
        action="store_true",
        help="Comment out the key and add it to PGMQ_REVOKED_IDS",
    )
    return parser


def issue_key_command(args: argparse.Namespace) -> int:
    """Dry-run or append a named key with a UUID id to a .env file."""
    from pgmq.api.auth import build_key_record
    from pgmq.api.config import issued_key_env_name, issued_key_name_env

    try:
        record = build_key_record(args.name)
        secret_var = issued_key_env_name(record["id"])
        name_var = issued_key_name_env(record["id"])
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    env_file = args.env_file
    if not args.apply:
        print("Dry-run: would add these lines to", env_file)
        print(f"  id: {record['id']}")
        print(f"  name: {record['name']}")
        print(f"  {secret_var}=<secret>")
        print(f"  {name_var}={record['name']}")
        print("Pass --apply to write the file and print the raw key once.")
        return 0

    directory = os.path.dirname(os.path.abspath(env_file))
    if directory:
        os.makedirs(directory, exist_ok=True)
    exists = os.path.exists(env_file)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    fd = os.open(env_file, flags, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as handle:
        if exists:
            handle.write("\n")
        handle.write(f"# pgmq-api id={record['id']} name={record['name']}\n")
        handle.write(f"{secret_var}={record['raw_key']}\n")
        handle.write(f"{name_var}={record['name']}\n")
    os.chmod(env_file, 0o600)
    print("API key created (shown once):")
    print(f"  id: {record['id']}")
    print(f"  name: {record['name']}")
    print(f"  file: {env_file}")
    print(f"  key: {record['raw_key']}")
    print("Revoke later with: pgmq-api revoke-key --id", record["id"])
    return 0


def revoke_key_command(args: argparse.Namespace) -> int:
    """Dry-run or revoke a key UUID in the env file."""
    from pgmq.api.config import normalize_key_id, revoke_key_in_env_file

    try:
        key_id = normalize_key_id(args.key_id)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    env_file = args.env_file
    if not args.apply:
        print("Dry-run: would revoke key", key_id, "in", env_file)
        print("Pass --apply to comment out the key and add PGMQ_REVOKED_IDS.")
        return 0
    try:
        found = revoke_key_in_env_file(env_file, key_id)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"Revoked key {key_id} in {env_file}")
    if not found:
        print("No PGMQ_KEY_ line matched. The id was still added to PGMQ_REVOKED_IDS.")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point for ``python -m pgmq.api`` and the ``pgmq-api`` script."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "init-config":
        return write_starter_config(args.path, args.force)

    if args.config:
        os.environ["PGMQ_API_CONFIG"] = args.config
    if args.host:
        os.environ["PGMQ_API_HOST"] = args.host
    if args.port is not None:
        os.environ["PGMQ_API_PORT"] = str(args.port)
    if args.auth_mode:
        os.environ["PGMQ_API_AUTH_MODE"] = args.auth_mode

    if args.command == "issue-key":
        return issue_key_command(args)
    if args.command == "revoke-key":
        return revoke_key_command(args)

    api_config = load_api_config()
    if api_config.auth_mode == "none":
        print(AUTH_DISABLED_WARNING, file=sys.stderr)
    from pgmq.api.auth import require_pepper

    try:
        require_pepper(api_config)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    import uvicorn

    uvicorn.run(
        "pgmq.api.asgi:app",
        host=api_config.host,
        port=api_config.port,
        timeout_keep_alive=api_config.max_poll_seconds + 10,
        reload=args.reload,
        log_level=args.log_level,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
