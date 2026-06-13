# src/pgmq/_client_fields.py
"""
Shared dataclass fields for all PGMQueue client variants.

Backward-compatible connection and logging fields are defined once here
so backend implementations do not duplicate them.
"""

from dataclasses import dataclass, field
from typing import Optional
import os


@dataclass
class PGMQueueClientFields:
    """Connection and configuration fields shared by all PGMQueue clients."""

    conn_string: Optional[str] = field(
        default_factory=lambda: os.getenv("DATABASE_URL")
    )
    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("PG_PORT", "5432"))
    database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "postgres"))
    username: str = field(default_factory=lambda: os.getenv("PG_USERNAME", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres"))
    delay: int = 0
    vt: int = 30
    pool_size: int = 10
    verbose: bool = False
    log_filename: Optional[str] = None
    init_extension: bool = True
    structured_logging: bool = False
    log_rotation: bool = False
    log_rotation_size: str = "10 MB"
    log_retention: str = "1 week"
