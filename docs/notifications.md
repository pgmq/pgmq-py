# Notifications

PGMQ can emit PostgreSQL `NOTIFY` events when new messages are inserted into a queue. This enables real-time, push-style consumers without constant polling.

## Enabling Notifications

```python
queue.enable_notify("my_queue", throttle_interval_ms=250)
```

| Parameter | Description |
|-----------|-------------|
| `throttle_interval_ms` | Minimum time between notifications for this queue. Prevents notification storms. |

## Disabling Notifications

```python
queue.disable_notify("my_queue")
```

## Updating Throttle Settings

```python
queue.update_notify("my_queue", throttle_interval_ms=500)
```

## Listing Notification Configurations

```python
throttles = queue.list_notify_throttles()
for t in throttles:
    print(t.queue_name, t.throttle_interval_ms, t.last_notified_at)
```

## Notification Listeners

The library provides two listener implementations that wrap PostgreSQL `LISTEN`:

### Sync Listener

```python
from pgmq.notify_listener import SyncNotificationListener

listener = SyncNotificationListener(queue)

def on_message(payload):
    print("Received:", payload)

# Blocks until timeout or stop signal
listener.listen("my_queue", callback=on_message, timeout=30.0)
```

### Async Listener

```python
from pgmq.notify_listener import AsyncNotificationListener

listener = AsyncNotificationListener(queue)

async def on_message(payload):
    print("Received:", payload)

# Run in an asyncio task
await listener.listen("my_queue", callback=on_message)
```

### Stopping a Listener

Both listeners support a `stop()` method:

```python
listener.stop()
```

This sets an internal event and closes the underlying connection, causing the listening loop to exit cleanly.

### Payload Format

The notification payload is a JSON object. If the payload is empty or cannot be parsed, the callback still fires with a minimal dict:

```python
{"event": "insert", "payload_empty": True}
# or
{"event": "insert", "raw_payload": "...", "parse_error": "..."}
```

## Channel Naming

The listener automatically listens on the channel:

```
pgmq.q_<queue_name>.INSERT
```

Channel names are safely quoted to prevent identifier injection.
