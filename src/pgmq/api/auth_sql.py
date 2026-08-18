# src/pgmq/api/auth_sql.py
"""Auth DDL and lookup SQL. Identifiers are interpolated only after validation."""

import re

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXTENSION_SCHEMA = "pgmq"


def validate_identifier(name: str) -> str:
    """Accept only simple SQL identifiers."""
    if not name or not IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def validate_schema(schema: str) -> str:
    """Accept a schema name. Reject the PGMQ extension schema."""
    validate_identifier(schema)
    if schema.lower() == EXTENSION_SCHEMA:
        raise ValueError("auth table must not use the pgmq extension schema")
    return schema


def quote_ident(name: str) -> str:
    """Quote a validated identifier."""
    return f'"{validate_identifier(name)}"'


def qualified_table(schema: str, table: str) -> str:
    """Return a validated quoted schema.table name."""
    validate_schema(schema)
    validate_identifier(table)
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def create_schema_sql(schema: str) -> str:
    """Build CREATE SCHEMA IF NOT EXISTS for a validated schema."""
    validate_schema(schema)
    return f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}"


def create_table_sql(schema: str, table: str) -> str:
    """Build CREATE TABLE IF NOT EXISTS for the worker API-key table."""
    target = qualified_table(schema, table)
    return (
        f"CREATE TABLE IF NOT EXISTS {target} ("
        "id uuid PRIMARY KEY, "
        "name text NOT NULL, "
        "key_prefix text NOT NULL, "
        "key_hash text NOT NULL UNIQUE, "
        "expires_at timestamptz, "
        "revoked_at timestamptz, "
        "created_at timestamptz NOT NULL DEFAULT now(), "
        "last_used_at timestamptz"
        ")"
    )


def table_exists_sql() -> str:
    """Lookup information_schema. Parameters: schema, table."""
    return (
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s"
    )


def lookup_by_hash_sql(schema: str, table: str) -> str:
    """Select a key row by hash. Parameter: key_hash."""
    target = qualified_table(schema, table)
    return (
        f"SELECT name, key_prefix, expires_at, revoked_at "
        f"FROM {target} WHERE key_hash = %s"
    )


def insert_key_sql(schema: str, table: str) -> str:
    """Insert a hashed key. Parameters: name, type, prefix, hash, expires, revoked."""
    target = qualified_table(schema, table)
    return (
        f"INSERT INTO {target} "
        "(id, name, key_prefix, key_hash, expires_at, revoked_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )


def touch_last_used_sql(schema: str, table: str) -> str:
    """Set last_used_at. Parameter: key_hash."""
    target = qualified_table(schema, table)
    return f"UPDATE {target} SET last_used_at = now() WHERE key_hash = %s"
