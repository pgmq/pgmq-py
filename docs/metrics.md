# Metrics

PGMQ exposes queue-level metrics that are useful for monitoring, alerting, and capacity planning.

## Queue Metrics

Retrieve statistics for a specific queue:

```python
metrics = queue.metrics("my_queue")
print(f"Length: {metrics.queue_length}")
print(f"Visible: {metrics.queue_visible_length}")
print(f"Oldest: {metrics.oldest_msg_age_sec}s")
```

## All Queue Metrics

Retrieve metrics for every queue in the database:

```python
all_metrics = queue.metrics_all()
for m in all_metrics:
    print(m.queue_name, m.queue_length, m.total_messages)
```

## Metrics Reference

The `QueueMetrics` dataclass contains the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `queue_name` | `str` | Queue name. |
| `queue_length` | `int` | Total messages in the queue (including invisible). |
| `newest_msg_age_sec` | `int \| None` | Age of the newest message in seconds. |
| `oldest_msg_age_sec` | `int \| None` | Age of the oldest message in seconds. |
| `total_messages` | `int` | Total messages ever sent to the queue. |
| `scrape_time` | `datetime` | Timestamp when metrics were collected. |
| `queue_visible_length` | `int` | Messages currently visible (not hidden by VT). |

> **Schema compatibility**: The client handles both old (6-column) and new (7-column) `pgmq.metrics` result schemas automatically.
