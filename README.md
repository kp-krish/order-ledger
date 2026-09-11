# Order Ledger

Order Ledger is a Spring Boot inventory projection built from Kafka order events. It
is intentionally designed to demonstrate at-least-once delivery, database-backed
idempotency, out-of-order handling, and deterministic row locking with real Kafka
and PostgreSQL integration tests.

The implementation roadmap and acceptance criteria live in [docs/PLAN.md](docs/PLAN.md).

## Prerequisites

- Java 21
- Docker Desktop with Linux containers

## Local infrastructure

```shell
docker compose up -d
```

## Build

```shell
./mvnw verify
```

On Windows PowerShell, use `./mvnw.cmd verify`.

## Generate demo events

Start the service, then seed the five demo SKUs and publish a workload:

```shell
python scripts/producer.py --seed-inventory --orders 100 --duplicates 10 --out-of-order 5
```

The producer uses `orderId` as the Kafka key. To expand an existing topic to three
partitions and exercise the deliberately stale `1, 3, 2` sequences:

```shell
python scripts/producer.py --partitions 3 --orders 100 --duplicates 10 --out-of-order 10
```

Kafka topics cannot be reduced from three partitions back to one. Use
`python scripts/producer.py --dry-run` to inspect records without Docker or Kafka.
