# Sending Messages

## Send a Single Message

```python
msg_id: int = queue.send("my_queue", {"task": "process_payment"})
```

## Send with Headers

Headers are optional metadata dictionaries attached to the message:

```python
msg_id = queue.send(
    "my_queue",
    {"task": "process_payment"},
    headers={"priority": "high", "source": "webhook"},
)
```

## Send with Delay

Delay message delivery by an integer number of seconds or a specific `datetime`:

```python
# Delay by 60 seconds
msg_id = queue.send("my_queue", {"task": "reminder"}, delay=60)

# Delay until a specific time
from datetime import datetime, timezone
future = datetime.now(timezone.utc).replace(hour=23, minute=0)
msg_id = queue.send("my_queue", {"task": "nightly_report"}, delay=future)
```

### Backward Compatible Alias

The `tz` parameter is an alias for `delay` and exists for backward compatibility:

```python
msg_id = queue.send("my_queue", {"task": "reminder"}, tz=60)
```

## Send a Batch

Send multiple messages in one transaction for higher throughput:

```python
messages = [
    {"task": "email_user_1"},
    {"task": "email_user_2"},
    {"task": "email_user_3"},
]

msg_ids: list[int] = queue.send_batch("my_queue", messages)
```

### Batch with Headers

```python
headers = [
    {"priority": "high"},
    {"priority": "low"},
    {"priority": "high"},
]
msg_ids = queue.send_batch("my_queue", messages, headers=headers)
```

> The `headers` list must be the same length as `messages`.

### Batch with Delay

```python
msg_ids = queue.send_batch("my_queue", messages, delay=300)
```

## Return Value

- `send` returns a single `int` (message ID).
- `send_batch` returns `list[int]`.

On failure or unsupported configurations, `-1` or an empty list may be returned.
