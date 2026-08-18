# src/pgmq/api/asgi.py
"""ASGI target. Importing this module constructs the default app."""

from pgmq.api.factory import create_app

app = create_app()
