#!/usr/bin/env python3
"""Generate synthetic order events, including duplicates and stale sequences."""

from __future__ import annotations

import argparse
import copy
import json
import os
import random
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKUS = ("SKU-001", "SKU-002", "SKU-003", "SKU-004", "SKU-005")
DEFAULT_TOPIC = os.getenv("ORDERS_TOPIC", "orders.events")
DEFAULT_DOCKER_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_DOCKER_BOOTSTRAP_SERVERS", "kafka:19092"
)
DEFAULT_DATABASE_NAME = os.getenv("POSTGRES_DB", "order_ledger")
DEFAULT_DATABASE_USERNAME = os.getenv("POSTGRES_USER", "order_ledger")


def event_id(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_event(
    rng: random.Random,
    event_type: str,
    order_id: str,
    sequence: int,
    lines: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "eventId": event_id(rng),
        "schemaVersion": 1,
        "eventType": event_type,
        "orderId": order_id,
        "sequence": sequence,
        "occurredAt": timestamp(),
        "lines": copy.deepcopy(lines),
    }


def build_workload(
    orders: int,
    duplicates: int,
    out_of_order: int,
    seed: int,
    order_prefix: str = "order",
) -> list[tuple[str, dict[str, Any]]]:
    if orders < 1:
        raise ValueError("orders must be at least 1")
    if duplicates < 0:
        raise ValueError("duplicates cannot be negative")
    if not 0 <= out_of_order <= orders:
        raise ValueError("out_of_order must be between 0 and orders")

    rng = random.Random(seed)
    records: list[tuple[str, dict[str, Any]]] = []
    for number in range(orders):
        order_id = f"{order_prefix}-{number + 1:06d}"
        line_count = rng.randint(1, 2)
        lines = [
            {"sku": sku, "qty": rng.randint(1, 4)}
            for sku in rng.sample(SKUS, line_count)
        ]
        placed = build_event(rng, "order.placed", order_id, 1, lines)
        records.append((order_id, placed))

        if number < out_of_order:
            records.append((order_id, build_event(rng, "order.shipped", order_id, 3, lines)))
            records.append((order_id, build_event(rng, "order.cancelled", order_id, 2, lines)))
        else:
            terminal_type = "order.cancelled" if number % 4 == 0 else "order.shipped"
            records.append((order_id, build_event(rng, terminal_type, order_id, 2, lines)))

    original_records = list(records)
    for index in range(duplicates):
        key, event = original_records[index % len(original_records)]
        records.append((key, copy.deepcopy(event)))
    return records


def seed_inventory(compose_file: str) -> None:
    sql_path = Path(__file__).with_name("seed-inventory.sql")
    subprocess.run(
        [
            "docker", "compose", "-f", compose_file, "exec", "-T",
            "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", DEFAULT_DATABASE_USERNAME,
            "-d", DEFAULT_DATABASE_NAME,
        ],
        input=sql_path.read_text(encoding="utf-8"),
        text=True,
        check=True,
    )


def ensure_partitions(
    compose_file: str,
    topic: str,
    partitions: int,
    bootstrap_servers: str = DEFAULT_DOCKER_BOOTSTRAP_SERVERS,
) -> None:
    subprocess.run(
        [
            "docker", "compose", "-f", compose_file, "exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bootstrap_servers,
            "--alter", "--topic", topic, "--partitions", str(partitions),
        ],
        check=True,
    )


def publish(
    compose_file: str,
    topic: str,
    records: list[tuple[str, dict[str, Any]]],
    bootstrap_servers: str = DEFAULT_DOCKER_BOOTSTRAP_SERVERS,
) -> None:
    command = [
        "docker", "compose", "-f", compose_file, "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server", bootstrap_servers,
        "--topic", topic,
        "--property", "parse.key=true",
        "--property", "key.separator=|",
    ]
    with subprocess.Popen(command, stdin=subprocess.PIPE, text=True) as process:
        assert process.stdin is not None
        for key, event in records:
            process.stdin.write(f"{key}|{json.dumps(event, separators=(',', ':'))}\n")
        process.stdin.close()
        return_code = process.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, command)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, default=100)
    parser.add_argument("--duplicates", type=int, default=10)
    parser.add_argument("--out-of-order", type=int, default=5)
    parser.add_argument("--random-seed", type=int, default=20260911)
    parser.add_argument("--order-prefix", default="order")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--bootstrap-servers", default=DEFAULT_DOCKER_BOOTSTRAP_SERVERS,
        help="Kafka address reachable from the Compose kafka container",
    )
    parser.add_argument("--compose-file", default="compose.yaml")
    parser.add_argument("--partitions", type=int, choices=(1, 3))
    parser.add_argument("--seed-inventory", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    records = build_workload(
        args.orders,
        args.duplicates,
        args.out_of_order,
        args.random_seed,
        args.order_prefix,
    )
    if args.dry_run:
        for key, event in records:
            print(f"{key}|{json.dumps(event, separators=(',', ':'))}")
    else:
        if args.seed_inventory:
            seed_inventory(args.compose_file)
        if args.partitions:
            ensure_partitions(
                args.compose_file, args.topic, args.partitions, args.bootstrap_servers
            )
        publish(args.compose_file, args.topic, records, args.bootstrap_servers)

    print(json.dumps({
        "orders": args.orders,
        "records": len(records),
        "duplicates": args.duplicates,
        "outOfOrderOrders": args.out_of_order,
        "dryRun": args.dry_run,
    }))


if __name__ == "__main__":
    main()
