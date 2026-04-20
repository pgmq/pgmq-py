# src/pgmq/decorators.py
"""
Transaction decorators for sync and async operations.

Provides automatic transaction management with connection injection.
"""

import functools
from typing import Callable, Any


def transaction(func: Callable) -> Callable:
    """
    Synchronous transaction decorator.

    Automatically manages database transactions by:
    1. Checking if 'conn' is already provided (nested transactions)
    2. If not, acquiring connection from pool and starting transaction
    3. Injecting connection as 'conn' keyword argument
    4. Handling commit/rollback automatically

    Usage:
        @transaction
        def my_method(self, queue: str, conn=None):
            # conn is provided, either injected or passed explicitly
            conn.execute("SELECT ...")
    """

    @functools.wraps(func)
    def wrapper(self, *args: Any, **kwargs: Any) -> Any:
        # Check if connection already provided
        if "conn" in kwargs and kwargs["conn"] is not None:
            return func(self, *args, **kwargs)

        # Acquire connection and manage transaction
        with self.pool.connection() as conn:
            with conn.transaction():
                kwargs["conn"] = conn
                return func(self, *args, **kwargs)

    return wrapper


def async_transaction(func: Callable) -> Callable:
    """
    Asynchronous transaction decorator.

    Same functionality as @transaction but for async methods using asyncpg.
    Manages asyncpg transactions properly with explicit start/commit/rollback.
    """

    @functools.wraps(func)
    async def wrapper(self, *args: Any, **kwargs: Any) -> Any:
        # Check if connection already provided
        if "conn" in kwargs and kwargs["conn"] is not None:
            return await func(self, *args, **kwargs)

        # Acquire connection and manage transaction
        async with self.pool.acquire() as conn:
            txn = conn.transaction()
            await txn.start()
            try:
                kwargs["conn"] = conn
                result = await func(self, *args, **kwargs)
                await txn.commit()
                return result
            except Exception:
                await txn.rollback()
                raise

    return wrapper
