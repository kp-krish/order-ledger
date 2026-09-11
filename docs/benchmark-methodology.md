# Benchmark methodology and diagnosis

## Measurement phases

The benchmark separates work that the original 440-record result combined:

1. A backlog is published before a fresh JVM starts. Cold-start throughput measures
   JVM launch, consumer-group join, and backlog processing together.
2. A warmup workload runs on that JVM and its results are discarded.
3. Steady state uses 11,000 deliveries by default (10,500 unique events and 500
   duplicates), satisfying the 10,000-record minimum.
4. The consumer is stopped, a restart backlog is published, and total restart
   recovery plus JVM startup are recorded separately.

For each measured phase, end-to-end latency is the event's `occurredAt` timestamp
to its durable `processed_events.processed_at` timestamp. Percentiles include Kafka
queueing and database transaction time and cover unique events; deliberate duplicate
deliveries reuse an event ID and therefore do not create a second durable timestamp.
The database-window rate uses the first and last durable timestamps. Producer rate
is timed independently so a slow producer cannot be mistaken for a slow consumer.

## Measured result

The 2026-09-11 Windows 11 run recorded:

- cold start: 420 deliveries at 15.3 events/s, including 11.167 seconds to health;
- discarded warmup: 2,100 deliveries;
- steady state: 11,000 deliveries at 47.7 events/s end-to-end and 47.0 unique
  events/s over the database completion window;
- producer: 3,018.5 events/s;
- steady-state durable latency: 114,841.988 ms p50 and 223,761.032 ms p99;
- duplicates: 500 of 500 rejected; and
- restart: 1,050 deliveries recovered in 69.804 seconds, including 10.659 seconds
  to health and a 25.320-second database processing window.

The raw machine-readable output is in `docs/benchmark-results.json`.

## Why throughput remains below 100 events/s

The evidence rules out the producer, connection-pool absence, and a missing dedup
index:

- the producer was about 64 times faster than the database completion rate;
- live Hikari gauges showed a 10-connection pool with zero pending borrowers; and
- `processed_events_pkey` is a unique B-tree on `event_id` and recorded 12,213 index
  scans during the official run. The order/sequence index recorded 12,214 scans.

The topic had one partition and the listener had one consumer thread. Every record
calls an `@Transactional` processor and record acknowledgment occurs only after it
returns. Each event therefore performs its advisory lock, dedup insert, sequence
lookup, projection reads, row locks, mutations, and outcome update serially in one
transaction. A controlled 2,000-event run with event INFO logging suppressed still
completed at 45.4 database events/s. Repeating it with PostgreSQL
`synchronous_commit=off` completed at 45.1 database events/s, so log formatting and
the synchronous WAL flush are not the dominant limit. The bottleneck is the serial
per-record database round-trip/transaction shape, not a missing resource or the
producer. That shape is intentionally unchanged because it enforces the service's
idempotency and locking guarantees.

## Consumer-group timing

The effective Kafka 4.2.1 consumer values are `session.timeout.ms=45000` and
`max.poll.interval.ms=300000`; neither is overridden by the application.
`session.timeout.ms` permits roughly 45 seconds before a crashed member is declared
dead, so it can dominate ungraceful restart recovery. `max.poll.interval.ms` allows
five minutes between polls and matters only when record processing stalls that long;
it does not explain normal per-record throughput. In the measured restart, total
recovery was 69.804 seconds while JVM startup was 10.659 seconds and active database
processing spanned 25.320 seconds. The remaining interval is consistent with group
detection/join coordination and is why the old aggregate 48-second figure could not
be treated as pure backlog-drain time.
