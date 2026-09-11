#!/usr/bin/env python3
"""Run a local throughput, duplicate, and consumer-recovery benchmark."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from scripts.producer import build_workload, publish, seed_inventory


DATABASE_NAME = os.getenv("POSTGRES_DB", "order_ledger")
DATABASE_USERNAME = os.getenv(
    "DATABASE_USERNAME", os.getenv("POSTGRES_USER", "order_ledger")
)
DATABASE_PASSWORD = os.getenv(
    "DATABASE_PASSWORD", os.getenv("POSTGRES_PASSWORD", "order_ledger")
)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"jdbc:postgresql://localhost:{os.getenv('POSTGRES_HOST_PORT', '15432')}/{DATABASE_NAME}",
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders.events")


def command(*parts: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(parts, check=True, text=True, capture_output=capture)


def psql(compose_file: str, sql: str) -> str:
    result = command(
        "docker", "compose", "-f", compose_file, "exec", "-T", "postgres",
        "psql", "-At", "-v", "ON_ERROR_STOP=1", "-U", DATABASE_USERNAME,
        "-d", DATABASE_NAME, "-c", sql,
        capture=True,
    )
    return result.stdout.strip()


def reset_demo_database(compose_file: str) -> None:
    psql(compose_file, "TRUNCATE order_lines, processed_events; DELETE FROM inventory;")
    seed_inventory(compose_file)


def restore_demo_credentials(compose_file: str) -> None:
    database_password = DATABASE_PASSWORD.replace("'", "''")
    psql(
        compose_file,
        f"ALTER ROLE {DATABASE_USERNAME} WITH PASSWORD '{database_password}';",
    )


def processed_count(compose_file: str) -> int:
    return int(psql(compose_file, "SELECT COUNT(*) FROM processed_events;"))


def processed_timestamps(compose_file: str, order_prefix: str) -> dict[str, float]:
    escaped_prefix = order_prefix.replace("'", "''").replace("%", "\\%").replace("_", "\\_")
    output = psql(compose_file, f"""
        SELECT event_id::text || '|' || EXTRACT(EPOCH FROM processed_at)::text
        FROM processed_events
        WHERE order_id LIKE '{escaped_prefix}-%' ESCAPE '\\'
        ORDER BY processed_at
    """)
    if not output:
        return {}
    return {
        event_id: float(processed_at)
        for event_id, processed_at in (line.split("|", 1) for line in output.splitlines())
    }


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot calculate a percentile of an empty sample")
    ordered = sorted(values)
    rank = (len(ordered) - 1) * quantile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def phase_statistics(
    compose_file: str,
    order_prefix: str,
    workload: list[tuple[str, dict]],
    elapsed_seconds: float,
    publish_seconds: float | None,
) -> dict:
    timestamps = processed_timestamps(compose_file, order_prefix)
    occurred_at_by_id = {
        event["eventId"]: datetime.fromisoformat(
            event["occurredAt"].replace("Z", "+00:00")
        ).timestamp()
        for _, event in workload
    }
    missing = set(occurred_at_by_id) - set(timestamps)
    if missing:
        raise RuntimeError(f"{len(missing)} unique events are missing processed timestamps")

    latencies_ms = [
        (timestamps[event_id] - occurred_at) * 1000
        for event_id, occurred_at in occurred_at_by_id.items()
    ]
    ordered_processing_times = sorted(timestamps.values())
    processing_window = (
        ordered_processing_times[-1] - ordered_processing_times[0]
        if len(ordered_processing_times) > 1 else 0.0
    )
    processing_rate = (
        (len(ordered_processing_times) - 1) / processing_window
        if processing_window > 0 else 0.0
    )
    result = {
        "deliveredEvents": len(workload),
        "uniqueEvents": len(occurred_at_by_id),
        "elapsedSeconds": round(elapsed_seconds, 3),
        "eventsPerSecond": round(len(workload) / elapsed_seconds, 1),
        "processingWindowSeconds": round(processing_window, 3),
        "databaseProcessingEventsPerSecond": round(processing_rate, 1),
        "latencyMilliseconds": {
            "p50": round(percentile(latencies_ms, 0.50), 3),
            "p99": round(percentile(latencies_ms, 0.99), 3),
        },
    }
    if publish_seconds is not None:
        result["producer"] = {
            "elapsedSeconds": round(publish_seconds, 3),
            "eventsPerSecond": round(len(workload) / publish_seconds, 1),
        }
    return result


def metric_count(outcome: str) -> float:
    with urllib.request.urlopen("http://localhost:8080/actuator/prometheus", timeout=2) as response:
        body = response.read().decode("utf-8")
    pattern = re.compile(
        rf'^order_ledger_events_total\{{[^}}]*outcome="{re.escape(outcome)}"[^}}]*}}\s+([0-9.eE+-]+)$',
        re.MULTILINE,
    )
    match = pattern.search(body)
    return float(match.group(1)) if match else 0.0


def wait_until(predicate, timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (OSError, ValueError, subprocess.CalledProcessError):
            pass
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for {description}")


def start_service(java: str, jar: str) -> tuple[subprocess.Popen[bytes], float]:
    environment = os.environ.copy()
    environment.update({
        "DATABASE_URL": DATABASE_URL,
        "DATABASE_USERNAME": DATABASE_USERNAME,
        "DATABASE_PASSWORD": DATABASE_PASSWORD,
        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
        "ORDERS_TOPIC": ORDERS_TOPIC,
    })
    started = time.monotonic()
    process = subprocess.Popen(
        [
            java, "-jar", jar,
            "--debug=false",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        env=environment,
    )
    wait_until(lambda: service_is_healthy(process), 60, "service health")
    return process, time.monotonic() - started


def health_is_up() -> bool:
    with urllib.request.urlopen("http://localhost:8080/actuator/health", timeout=2) as response:
        return response.status == 200


def service_is_healthy(process: subprocess.Popen[bytes]) -> bool:
    if process.poll() is not None:
        raise RuntimeError(f"Service exited during startup with code {process.returncode}")
    return health_is_up()


def stop_service(process: subprocess.Popen[bytes]) -> None:
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def wait_for_phase(
    compose_file: str,
    expected_processed: int,
    expected_duplicates: int,
    timeout: float,
) -> None:
    wait_until(
        lambda: processed_count(compose_file) == expected_processed
        and metric_count("duplicate") >= expected_duplicates,
        timeout,
        f"{expected_processed} processed events and {expected_duplicates} duplicates",
    )


def run_live_phase(
    compose_file: str,
    topic: str,
    workload: list[tuple[str, dict]],
    order_prefix: str,
    duplicate_baseline: int,
    duplicates: int,
    timeout: float,
) -> dict:
    expected_processed = processed_count(compose_file) + len({
        event["eventId"] for _, event in workload
    })
    started = time.monotonic()
    publish_started = time.monotonic()
    publish(compose_file, topic, workload)
    publish_seconds = time.monotonic() - publish_started
    wait_for_phase(
        compose_file,
        expected_processed,
        duplicate_baseline + duplicates,
        timeout,
    )
    elapsed_seconds = time.monotonic() - started
    return phase_statistics(
        compose_file, order_prefix, workload, elapsed_seconds, publish_seconds
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", default="java")
    parser.add_argument("--jar", default="target/order-ledger-0.0.1-SNAPSHOT.jar")
    parser.add_argument("--compose-file", default="compose.yaml")
    parser.add_argument("--cold-orders", type=int, default=200)
    parser.add_argument("--cold-duplicates", type=int, default=20)
    parser.add_argument("--warmup-orders", type=int, default=1000)
    parser.add_argument("--warmup-duplicates", type=int, default=100)
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument("--duplicates", type=int, default=500)
    parser.add_argument("--out-of-order", type=int, default=500)
    parser.add_argument("--restart-orders", type=int, default=500)
    parser.add_argument("--restart-duplicates", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", default="benchmark-results.json")
    parser.add_argument(
        "--confirm-reset",
        action="store_true",
        help="required: truncates only the local Compose order-ledger tables",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.confirm_reset:
        raise SystemExit("Refusing to reset demo data without --confirm-reset")

    command("docker", "compose", "-f", args.compose_file, "up", "-d", "--wait")
    restore_demo_credentials(args.compose_file)
    bootstrap_service, _ = start_service(args.java, args.jar)
    stop_service(bootstrap_service)
    service = bootstrap_service
    try:
        reset_demo_database(args.compose_file)
        run_id = int(time.time())

        cold_prefix = f"cold-{run_id}"
        cold_workload = build_workload(
            args.cold_orders, args.cold_duplicates, 0, 11, cold_prefix
        )
        publish(args.compose_file, ORDERS_TOPIC, cold_workload)
        cold_started = time.monotonic()
        service, cold_startup_seconds = start_service(args.java, args.jar)
        wait_for_phase(
            args.compose_file,
            len(cold_workload) - args.cold_duplicates,
            args.cold_duplicates,
            args.timeout,
        )
        cold_elapsed = time.monotonic() - cold_started
        cold_result = phase_statistics(
            args.compose_file, cold_prefix, cold_workload, cold_elapsed, None
        )
        cold_result["startupSeconds"] = round(cold_startup_seconds, 3)
        cold_result["duplicatesRejected"] = args.cold_duplicates

        warmup_prefix = f"warmup-{run_id}"
        warmup = build_workload(
            args.warmup_orders, args.warmup_duplicates, 0, 51, warmup_prefix
        )
        run_live_phase(
            args.compose_file,
            ORDERS_TOPIC,
            warmup,
            warmup_prefix,
            args.cold_duplicates,
            args.warmup_duplicates,
            args.timeout,
        )

        steady_prefix = f"steady-{run_id}"
        workload = build_workload(
            args.orders, args.duplicates, args.out_of_order, 101,
            steady_prefix,
        )
        if len(workload) < 10_000:
            raise ValueError(
                f"steady-state workload has {len(workload)} records; at least 10,000 are required"
            )
        expected_unique = len(workload) - args.duplicates
        steady_result = run_live_phase(
            args.compose_file,
            ORDERS_TOPIC,
            workload,
            steady_prefix,
            args.cold_duplicates + args.warmup_duplicates,
            args.duplicates,
            args.timeout,
        )
        steady_result["duplicatesRejected"] = args.duplicates

        stop_service(service)
        recovery_prefix = f"recovery-{run_id}"
        recovery = build_workload(
            args.restart_orders, args.restart_duplicates, 0, 202,
            recovery_prefix,
        )
        expected_after_recovery = (
            len(cold_workload) - args.cold_duplicates
            + len(warmup) - args.warmup_duplicates
            + expected_unique
            + len(recovery) - args.restart_duplicates
        )
        publish(args.compose_file, ORDERS_TOPIC, recovery)

        recovery_started = time.monotonic()
        service, recovery_startup_seconds = start_service(args.java, args.jar)
        wait_for_phase(
            args.compose_file,
            expected_after_recovery,
            args.restart_duplicates,
            args.timeout,
        )
        recovery_seconds = time.monotonic() - recovery_started
        recovery_result = phase_statistics(
            args.compose_file,
            recovery_prefix,
            recovery,
            recovery_seconds,
            None,
        )
        recovery_result["startupSeconds"] = round(recovery_startup_seconds, 3)
        recovery_result["duplicatesRejected"] = args.restart_duplicates

        result = {
            "measuredAt": datetime.now(timezone.utc).isoformat(),
            "gitCommit": command("git", "rev-parse", "HEAD", capture=True).stdout.strip(),
            "gitWorkingTreeDirty": bool(
                command("git", "status", "--porcelain", capture=True).stdout.strip()
            ),
            "machine": platform.platform(),
            "methodology": {
                "latencyDefinition": "event occurredAt to durable processed_at for each unique event",
                "steadyStateMinimumDeliveredEvents": 10_000,
            },
            "coldStart": cold_result,
            "warmup": {
                "deliveredEvents": len(warmup),
                "discarded": True,
            },
            "steadyState": steady_result,
            "restartRecovery": recovery_result,
        }
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
    finally:
        if service.poll() is None:
            stop_service(service)


if __name__ == "__main__":
    main()
