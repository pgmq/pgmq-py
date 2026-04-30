# Messages and Dataclasses

The `pgmq` library maps PGMQ PostgreSQL composite types to Python `@dataclass` objects for type safety and IDE autocomplete support.

## Message

Represents a single message record.

```python
from pgmq import Message

msg: Message = queue.read("my_queue", vt=30)
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `msg_id` | `int` | Unique message identifier. |
| `read_ct` | `int` | Number of times the message has been read. |
| `enqueued_at` | `datetime` | Timestamp when the message was sent. |
| `last_read_at` | `datetime \| None` | Timestamp of the most recent read. |
| `vt` | `datetime` | Visibility timeout expiration timestamp. |
| `message` | `dict` | The JSON payload. |
| `headers` | `dict \| None` | Optional metadata headers. |

## QueueRecord

Metadata about a queue.

```python
queues = queue.list_queues()
for q in queues:
    print(q.queue_name)
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `queue_name` | `str` | Queue name. |
| `is_partitioned` | `bool` | Partitioned flag. |
| `is_unlogged` | `bool` | Unlogged flag. |
| `created_at` | `datetime` | Creation timestamp. |

Casting to `str` returns the queue name for convenience:

```python
str(q) == q.queue_name
```

## QueueMetrics

Statistics returned by `metrics()` and `metrics_all()`.

```python
metrics = queue.metrics("my_queue")
print(metrics.queue_length)
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `queue_name` | `str` | Queue name. |
| `queue_length` | `int` | Total messages in the queue. |
| `newest_msg_age_sec` | `int \| None` | Age of the newest message in seconds. |
| `oldest_msg_age_sec` | `int \| None` | Age of the oldest message in seconds. |
| `total_messages` | `int` | Total messages ever sent. |
| `scrape_time` | `datetime` | When the metrics were collected. |
| `queue_visible_length` | `int` | Messages currently visible (not invisible due to VT). |

## TopicBinding

Represents a topic routing binding.

```python
bindings = queue.list_topic_bindings()
for b in bindings:
    print(b.pattern, b.queue_name, b.compiled_regex)
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `pattern` | `str` | The binding pattern (e.g., `orders.*`). |
| `queue_name` | `str` | Target queue. |
| `bound_at` | `datetime` | When the binding was created. |
| `compiled_regex` | `str` | The internal regex representation. |

## RoutingResult

Result from `test_routing()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `pattern` | `str` | Matched pattern. |
| `queue_name` | `str` | Target queue. |
| `compiled_regex` | `str` | Internal regex. |

## BatchTopicResult

Result from `send_batch_topic()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `queue_name` | `str` | Queue that received the message. |
| `msg_id` | `int` | Assigned message ID. |

## NotificationThrottle

Configuration for NOTIFY throttling.

```python
throttles = queue.list_notify_throttles()
for t in throttles:
    print(t.queue_name, t.throttle_interval_ms)
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `queue_name` | `str` | Queue name. |
| `throttle_interval_ms` | `int` | Minimum milliseconds between notifications. |
| `last_notified_at` | `datetime` | Last notification timestamp. |
