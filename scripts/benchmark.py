#!/usr/bin/env python3
"""Run a local throughput, duplicate, and consumer-recovery benchmark."""

from __future__ import annotations

import argparse
import json
import platform
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from scripts.producer import build_workload, publish, seed_inventory


def command(*parts: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(parts, check=True, text=True, capture_output=capture)


def psql(compose_file: str, sql: str) -> str:
    result = command(
        "docker", "compose", "-f", compose_file, "exec", "-T", "postgres",
        "psql", "-At", "-v", "ON_ERROR_STOP=1", "-U", "order_ledger",
        "-d", "order_ledger", "-c", sql,
        capture=True,
    )
    return result.stdout.strip()


def reset_demo_database(compose_file: str) -> None:
    psql(compose_file, "TRUNCATE order_lines, processed_events; DELETE FROM inventory;")
    seed_inventory(compose_file)


def restore_demo_credentials(compose_file: str) -> None:
    psql(compose_file, "ALTER ROLE order_ledger WITH PASSWORD 'order_ledger';")


def processed_count(compose_file: str) -> int:
    return int(psql(compose_file, "SELECT COUNT(*) FROM processed_events;"))


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


def start_service(java: str, jar: str) -> subprocess.Popen[bytes]:
    process = subprocess.Popen(
        [
            java, "-jar", jar,
            "--spring.datasource.url=jdbc:postgresql://localhost:15432/order_ledger",
            "--spring.datasource.username=order_ledger",
            "--spring.datasource.password=order_ledger",
            "--spring.kafka.bootstrap-servers=localhost:9092",
            "--debug=false",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    wait_until(lambda: service_is_healthy(process), 60, "service health")
    return process


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", default="java")
    parser.add_argument("--jar", default="target/order-ledger-0.0.1-SNAPSHOT.jar")
    parser.add_argument("--compose-file", default="compose.yaml")
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--duplicates", type=int, default=100)
    parser.add_argument("--out-of-order", type=int, default=100)
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
    service = start_service(args.java, args.jar)
    try:
        reset_demo_database(args.compose_file)
        run_id = int(time.time())
        workload = build_workload(
            args.orders, args.duplicates, args.out_of_order, 101,
            f"benchmark-{run_id}",
        )
        expected_unique = len(workload) - args.duplicates
        started = time.monotonic()
        publish(args.compose_file, "orders.events", workload)
        wait_for_phase(args.compose_file, expected_unique, args.duplicates, args.timeout)
        throughput_seconds = time.monotonic() - started

        stop_service(service)
        recovery = build_workload(
            args.restart_orders, args.restart_duplicates, 0, 202,
            f"recovery-{run_id}",
        )
        expected_after_recovery = expected_unique + len(recovery) - args.restart_duplicates
        publish(args.compose_file, "orders.events", recovery)

        recovery_started = time.monotonic()
        service = start_service(args.java, args.jar)
        wait_for_phase(
            args.compose_file,
            expected_after_recovery,
            args.restart_duplicates,
            args.timeout,
        )
        recovery_seconds = time.monotonic() - recovery_started

        result = {
            "measuredAt": datetime.now(timezone.utc).isoformat(),
            "gitCommit": command("git", "rev-parse", "HEAD", capture=True).stdout.strip(),
            "machine": platform.platform(),
            "throughput": {
                "deliveredEvents": len(workload),
                "elapsedSeconds": round(throughput_seconds, 3),
                "eventsPerSecond": round(len(workload) / throughput_seconds, 1),
                "duplicatesRequested": args.duplicates,
                "duplicatesRejected": args.duplicates,
            },
            "restartRecovery": {
                "backlogEvents": len(recovery),
                "elapsedSeconds": round(recovery_seconds, 3),
                "duplicatesRequested": args.restart_duplicates,
                "duplicatesRejected": args.restart_duplicates,
            },
        }
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
    finally:
        if service.poll() is None:
            stop_service(service)


if __name__ == "__main__":
    main()
