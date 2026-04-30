# Visibility Timeout

The visibility timeout (VT) determines how long a message remains invisible to other consumers after being read. If the consumer does not delete or archive the message before the VT expires, the message becomes visible again.

## Set VT for a Single Message

```python
updated = queue.set_vt("my_queue", msg_id=42, vt=120)
```

Returns the updated `Message` object.

## Set VT for Multiple Messages

```python
updated = queue.set_vt("my_queue", msg_id=[42, 43, 44], vt=120)
```

Returns a `list[Message]`.

## Set VT to a Specific Timestamp

Instead of an integer number of seconds, you can pass a `datetime` to set an absolute expiration:

```python
from datetime import datetime, timezone, timedelta

future = datetime.now(timezone.utc) + timedelta(minutes=10)
updated = queue.set_vt("my_queue", msg_id=42, vt=future)
```

## Behavior Notes

- `set_vt` is transactional. If the operation fails, no changes are applied.
- The returned `Message.vt` reflects the new expiration time.
- Use this to implement "heartbeat" or "lease extension" patterns in long-running workers.

## Example: Worker Heartbeat

```python
msg = queue.read("my_queue", vt=30)
if msg:
    try:
        process_task(msg.message)
        # Extend the lease while working
        queue.set_vt("my_queue", msg.msg_id, vt=60)
        finalize_task(msg.message)
        queue.archive("my_queue", msg.msg_id)
    except Exception:
        # Let the message expire and become visible again
        pass
```
