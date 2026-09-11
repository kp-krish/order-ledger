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

## Measure it locally

Build the jar, then let the benchmark own the service process and reset the local
Compose demo tables:

```shell
./mvnw clean package -DskipTests
python -m scripts.benchmark --confirm-reset
```

Use `--java C:\\path\\to\\java.exe` on Windows when Java 21 is not on `PATH`.
The command writes `benchmark-results.json` containing sustained events/second,
exact duplicate rejection, and backlog recovery time after a consumer restart.

One measured Windows 11 run delivered 440 events at 29.2 events/second, rejected
20/20 duplicates, and recovered a 210-event restart backlog in 48.309 seconds while
rejecting another 10/10 duplicates. See the [raw benchmark evidence](docs/benchmark-results.json)
and the [failure lab](docs/failure-lab.md). Results vary with hardware and workload.

The verified suite currently contains 16 Java tests (5 real Kafka/PostgreSQL
integration tests) plus 3 producer tests. JaCoCo reports 85.1% line coverage
(183 of 215 executable lines) for the Java service.
