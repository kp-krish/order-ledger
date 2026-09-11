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

