# Logging

`pgmq` uses a dual-backend logging system that works out of the box with Python's standard `logging` module, while optionally supporting `loguru` when installed.

## Default Behavior

By default, the library does **not** attach any handlers or change global logging levels. It simply acquires a logger via `logging.getLogger(name)` and emits `DEBUG` level messages during normal operation. If you do not configure logging, these messages are silently ignored.

## Enabling Verbose Logging

Pass `verbose=True` to see `DEBUG` output on stderr and in a log file:

```python
queue = PGMQueue(verbose=True, log_filename="pgmq.log")
```

## Structured Logging

Output JSON-formatted logs for ingestion by log aggregators:

```python
queue = PGMQueue(
    verbose=True,
    structured_logging=True,
    log_filename="pgmq.jsonl",
)
```

## Log Rotation (loguru)

If `loguru` is installed, you can enable rotation and retention:

```python
queue = PGMQueue(
    verbose=True,
    log_rotation=True,
    log_rotation_size="10 MB",
    log_retention="1 week",
)
```

## Global Logging Configuration

You can apply settings globally via `LoggingManager`:

```python
from pgmq.logger import LoggingManager

LoggingManager.configure_global_logging(
    log_level="INFO",
    structured=True,
    use_loguru=True,  # requires loguru to be installed
)
```

## Performance Logging

The `log_performance` decorator records execution time and success/failure status:

```python
from pgmq.logger import log_performance

logger = queue.logger

@log_performance(logger)
def process_message(msg):
    ...
```

It works with both sync and async functions.

## Contextual Logging

Add structured context to any log entry:

```python
from pgmq.logger import log_with_context

log_with_context(
    queue.logger,
    "INFO",
    "Processing batch",
    batch_size=100,
    queue="my_queue",
)
```

Both stdlib `logging` and `loguru` backends are handled transparently.

## Logger Types

| Symbol | Description |
|--------|-------------|
| `LoggingManager` | Centralized logger factory and configuration. |
| `PGMQLogger` | Backward-compatible alias for `LoggingManager`. |
| `create_logger()` | Factory function for quick logger creation. |

## Test Mode

`LoggingManager._test_mode = True` disables `enqueue` in loguru handlers, ensuring synchronous logging for test assertions. This is used internally by the test suite.
