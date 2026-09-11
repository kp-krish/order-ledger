# Architecture

## Processing boundary

Kafka provides ordered records only within one partition and may redeliver a record
until its offset is committed. Order Ledger therefore treats Kafka offsets as
transport progress, not as the idempotency source of truth.

For every record, one PostgreSQL transaction will:

1. acquire a transaction-scoped advisory lock derived from `orderId`;
2. insert `eventId` into `processed_events` using its primary-key constraint;
3. reject the record as stale if its sequence is not greater than the accepted
   sequence already stored for that order;
4. lock all affected inventory rows in sorted SKU order;
5. mutate `inventory` and `order_lines`; and
6. mark the event outcome and commit.

The Kafka listener returns only after that transaction completes. A duplicate
primary-key violation rolls back the transaction; the listener classifies it as an
already-completed delivery and returns normally so its offset can be acknowledged.

## Why three tables are sufficient

`processed_events` is both the idempotency ledger and the durable per-order sequence
history. The advisory lock closes the first-event race that row locking alone cannot
close when an order has no rows yet. `order_lines.last_sequence` makes the current
projection self-describing, while the event ledger remains the authoritative order
watermark.

## Inventory semantics

- `order.placed`: subtract quantity from available and add it to reserved.
- `order.cancelled`: add the previously reserved quantity back to available and
  subtract it from reserved.
- `order.shipped`: subtract the shipped quantity from reserved; available was
  already reduced when the order was placed.

Available quantity is allowed to become negative. The database constraint requires
`oversold` to exactly match that condition, so an oversell is observable rather than
silently clamped to zero.

Every lifecycle event carries its complete line set. This makes records independently
auditable and lets the processor verify that later events agree with the placed order.

