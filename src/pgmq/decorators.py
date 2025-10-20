# src/pgmq/decorators.py

import functools
from pgmq.logger import PGMQLogger


def transaction(func):
    """Decorator to run a function within a database transaction."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if args and hasattr(args[0], "pool") and hasattr(args[0], "logger"):
            self = args[0]

            if "conn" not in kwargs:
                with self.pool.connection() as conn:
                    with conn.transaction():
                        PGMQLogger.log_transaction_start(
                            self.logger, func.__name__, conn_id=id(conn)
                        )
                        try:
                            kwargs["conn"] = conn  # Inject 'conn' into kwargs
                            result = func(*args, **kwargs)
                            PGMQLogger.log_transaction_success(
                                self.logger, func.__name__, conn_id=id(conn)
                            )
                            return result
                        except Exception as e:
                            PGMQLogger.log_transaction_error(
                                self.logger, func.__name__, e, conn_id=id(conn)
                            )
                            raise
            else:
                return func(*args, **kwargs)

        else:
            queue = kwargs.get("queue") or args[0]

            if "conn" not in kwargs:
                with queue.pool.connection() as conn:
                    with conn.transaction():
                        PGMQLogger.log_transaction_start(
                            queue.logger, func.__name__, conn_id=id(conn)
                        )
                        try:
                            kwargs["conn"] = conn  # Inject 'conn' into kwargs
                            result = func(*args, **kwargs)
                            PGMQLogger.log_transaction_success(
                                queue.logger, func.__name__, conn_id=id(conn)
                            )
                            return result
                        except Exception as e:
                            PGMQLogger.log_transaction_error(
                                queue.logger, func.__name__, e, conn_id=id(conn)
                            )
                            raise
            else:
                return func(*args, **kwargs)

    return wrapper


def async_transaction(func):
    """Asynchronous decorator to run a method within a database transaction."""

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if "conn" not in kwargs:
            async with self.pool.acquire() as conn:
                txn = conn.transaction()
                await txn.start()
                PGMQLogger.log_transaction_start(
                    self.logger, func.__name__, conn_id=id(conn)
                )
                try:
                    kwargs["conn"] = conn
                    result = await func(self, *args, **kwargs)
                    await txn.commit()
                    PGMQLogger.log_transaction_success(
                        self.logger, func.__name__, conn_id=id(conn)
                    )
                    return result
                except Exception as e:
                    await txn.rollback()
                    PGMQLogger.log_transaction_error(
                        self.logger, func.__name__, e, conn_id=id(conn)
                    )
                    raise
        else:
            return await func(self, *args, **kwargs)

    return wrapper
