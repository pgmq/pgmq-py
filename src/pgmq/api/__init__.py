# src/pgmq/api/__init__.py
"""
HTTP API extra for PGMQ.

Import factory symbols only. ``app`` is loaded on attribute access so
``from pgmq.api import create_app`` does not construct a default client.
"""

from typing import Any

try:
    from pgmq.api.config import PGMQAPIConfig
    from pgmq.api.factory import create_app, create_router
except ImportError as exc:
    raise ImportError(
        "pgmq.api requires the 'fastapi' extra. Install with: pip install 'pgmq[fastapi]'"
    ) from exc

__all__ = ["create_app", "create_router", "PGMQAPIConfig"]


def __getattr__(name: str) -> Any:
    """Load the ASGI app only when a caller asks for ``app``."""
    if name == "app":
        from pgmq.api.asgi import app

        return app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
