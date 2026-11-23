#!/usr/bin/env python3
"""Verify stage 5 runs from the REST API to persisted Kafka measurements."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from uuid import UUID


ROOT = Path(__file__).resolve().parents[2]
CONTROL = "http://127.0.0.1:8080"
TERMINAL_STATES = {"failed", "completed", "cancelled"}
STOP_TEST_DURATION = 60
TARGET_RPS = 10


def api(path: str, method: str = "GET", body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = Request(CONTROL + path, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    with urlopen(request, timeout=5) as response:
        return json.load(response)


def new_run(duration: int) -> UUID:
    scenario = api("/scenarios", "POST", {
        "name": "stage05-smoke",
        "method": "GET",
        "url": "http://demo-target:8080/",
        "headers": {"X-Smoke": "stage05"},
        "targetRps": TARGET_RPS,
        "durationSeconds": duration,
        "requestTimeoutMs": 1000,
        "concurrencyLimit": 4,
    })
    return UUID(api("/runs", "POST", {"scenarioId": scenario["id"]})["id"])


def wait_run_state(run: UUID, expected: str) -> None:
    deadline = time.monotonic() + 45
    state = None
    while time.monotonic() < deadline:
        state = api(f"/runs/{run}")["state"]
        if state == expected:
            return
        if state in TERMINAL_STATES:
            raise RuntimeError(f"Run {run} reached {state}, expected {expected}")
        time.sleep(0.2)
    raise RuntimeError(f"Run {run} did not reach {expected} (last state: {state})")


def assert_batches(
    compose: list[str], run: UUID, final_state: str, *, require_http: bool = False,
    max_started: int | None = None,
) -> None:
    sql = """
SELECT count(*), count(*) FILTER (WHERE payload->>'isFinal' = 'true'
    AND payload #>> '{summary,state}' = :'final_state'),
    coalesce(sum((payload #>> '{counters,httpResponses}')::bigint), 0),
    coalesce(count(*) = max(sequence), false)::int,
    coalesce(sum((payload #>> '{counters,started}')::bigint), 0),
    (SELECT count(*) FROM run_manifests WHERE run_id = :'run' AND finalization IS NOT NULL)
FROM result_batches WHERE run_id = :'run';
"""
    command = compose + [
        "exec", "-T", "postgres", "psql", "-X", "-U", "postgres", "-d", "results",
        "-v", "ON_ERROR_STOP=1", "-v", f"run={run}", "-v", f"final_state={final_state}", "-At",
    ]
    deadline = time.monotonic() + 15
    row = ""
    while time.monotonic() < deadline:
        row = subprocess.run(command, input=sql, text=True, stdout=subprocess.PIPE,
                             cwd=ROOT, check=True, timeout=5).stdout.strip()
        batches, finals, responses, contiguous, started, finalized = map(int, row.split("|"))
        if finals == contiguous == finalized == 1 and (not require_http or responses > 0):
            if require_http and batches < 2:
                raise RuntimeError(f"Run {run} did not persist both interval and final batches")
            if max_started is not None and started > max_started:
                raise RuntimeError(f"Run {run} started {started} requests, expected at most {max_started}")
            return
        time.sleep(0.2)
    raise RuntimeError(f"Run {run} has no contiguous {final_state} batches (last row: {row})")


def smoke(project: str) -> None:
    compose = ["docker", "compose", "-p", project, "--env-file", "deploy/compose/.env.example",
               "-f", "deploy/compose/compose.yaml"]

    natural = new_run(3)
    wait_run_state(natural, "completed")
    assert_batches(compose, natural, "ASSIGNMENT_STATE_COMPLETED", require_http=True)

    stopped = new_run(STOP_TEST_DURATION)
    wait_run_state(stopped, "running")
    api(f"/runs/{stopped}/stop", "POST")
    api(f"/runs/{stopped}/stop", "POST")
    wait_run_state(stopped, "cancelled")
    assert_batches(compose, stopped, "ASSIGNMENT_STATE_STOPPED",
                   max_started=STOP_TEST_DURATION * TARGET_RPS - 1)

    early = new_run(STOP_TEST_DURATION)
    api(f"/runs/{early}/stop", "POST")
    wait_run_state(early, "cancelled")
    assert_batches(compose, early, "ASSIGNMENT_STATE_STOPPED", max_started=0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="latencystorm-stage05")
    args = parser.parse_args()
    try:
        smoke(args.project)
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("PASS: stage 5 completed, stopped and pre-start stop.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
