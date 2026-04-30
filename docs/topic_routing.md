# Topic-Based Routing

Topic routing allows you to send a message once and have it delivered to multiple queues based on pattern matching. This is useful for pub/sub and event-driven architectures.

## Concepts

- **Routing Key**: A dot-separated string describing the event (e.g., `orders.created`).
- **Pattern**: A binding rule using `*` (single word) or `#` (zero or more words) wildcards.
- **Binding**: Associating a pattern with a queue.

## Bind a Pattern

```python
queue.bind_topic("orders.*", "orders_queue")
queue.bind_topic("orders.#", "audit_queue")
```

| Pattern | Matches |
|---------|---------|
| `orders.*` | `orders.created`, `orders.updated` (one word) |
| `orders.#` | `orders.created`, `orders.created.us` (any depth) |
| `*.error` | `payment.error`, `shipping.error` |

## Unbind a Pattern

```python
queue.unbind_topic("orders.*", "orders_queue")
```

## Send to a Topic

```python
queue.send_topic("orders.created", {"order_id": 123, "total": 99.99})
```

This delivers the message to **all queues** whose bound patterns match the routing key.

### Send with Headers

```python
queue.send_topic(
    "orders.created",
    {"order_id": 123},
    headers={"source": "web"},
)
```

### Send with Delay

```python
queue.send_topic("orders.created", {"order_id": 123}, delay=60)
```

## Send Batch to a Topic

```python
messages = [
    {"order_id": 1},
    {"order_id": 2},
    {"order_id": 3},
]
results = queue.send_batch_topic("orders.created", messages)
for r in results:
    print(r.queue_name, r.msg_id)
```

## List Bindings

```python
# All bindings
bindings = queue.list_topic_bindings()

# Filtered by queue
bindings = queue.list_topic_bindings(queue_name="orders_queue")
```

Returns `list[TopicBinding]`.

## Test Routing

Preview which queues would receive a message without actually sending it:

```python
results = queue.test_routing("orders.created")
for r in results:
    print(r.queue_name, r.pattern)
```

## Validation

Validate routing keys and patterns before using them:

```python
queue.validate_routing_key("orders.created")  # True or False
queue.validate_topic_pattern("orders.*")      # True or False
```

These return booleans and will not raise exceptions on invalid input.
