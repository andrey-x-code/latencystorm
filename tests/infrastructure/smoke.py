#!/usr/bin/env python3
"""Check local infrastructure, Kafka failure detection and readiness recovery."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[2]


def status(url: str) -> int:
    try:
        with urlopen(url, timeout=8) as response:
            return response.status
    except HTTPError as error:
        with error:
            return error.code


def wait_ready(expected: int, timeout: float, interval: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        codes = [status(f"http://127.0.0.1:{port}/ready") for port in (8080, 8081)]
        if all(code == expected for code in codes):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(f"Readiness did not reach {expected}: {codes}")
        time.sleep(min(interval, remaining))


def smoke(compose_file: str, env_file: str) -> None:
    command = ["docker", "compose", "--env-file", env_file, "-f", compose_file]

    def compose(action: str) -> None:
        subprocess.run(command + [action, "kafka"], cwd=ROOT, check=True, timeout=60)

    endpoints = [
        f"http://127.0.0.1:{port}/{path}"
        for port in range(8080, 8084) for path in ("health", "ready")
    ] + ["http://127.0.0.1:9090/-/ready"]
    for url in endpoints:
        if status(url) != 200:
            raise RuntimeError(f"Endpoint is not ready: {url}")
    with urlopen("http://127.0.0.1:8083/", timeout=8) as response:
        demo = json.load(response)
    if demo.get("status") != 200:
        raise RuntimeError("Demo Target success response failed.")

    # Restore Kafka even if stop partially succeeds or the test is interrupted.
    kafka_stopped = True
    try:
        compose("stop")
        wait_ready(503, timeout=30, interval=1)
        compose("start")
        kafka_stopped = False
        wait_ready(200, timeout=45, interval=3)
    finally:
        if kafka_stopped:
            compose("start")
    print("PASS: infrastructure smoke test.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose-file", default="deploy/compose/compose.yaml")
    parser.add_argument("--env-file", default="deploy/compose/.env.example")
    args = parser.parse_args()
    try:
        smoke(args.compose_file, args.env_file)
    except (OSError, URLError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
