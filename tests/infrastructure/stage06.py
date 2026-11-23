#!/usr/bin/env python3
"""Verify report aggregation and recovery against a running Compose stack."""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import time
from urllib.request import urlopen
from urllib.error import URLError
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
COMPLETED = "ASSIGNMENT_STATE_COMPLETED"


def timestamp(value):
    return value.isoformat().replace("+00:00", "Z")


def get(run, endpoint="report"):
    with urlopen(f"http://127.0.0.1:8081/runs/{run}/{endpoint}", timeout=5) as response:
        return json.load(response)


def wait_report(run, predicate, timeout=60):
    deadline = time.monotonic() + timeout
    report = None
    while time.monotonic() < deadline:
        report = get(run)
        if predicate(report):
            return report
        time.sleep(0.25)
    raise RuntimeError(f"Unexpected report for {run}: {report}")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_ready(port):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urlopen(f"http://127.0.0.1:{port}/ready", timeout=3) as response:
                if response.status == 200:
                    return
        except (URLError, OSError):
            pass
        time.sleep(0.25)
    raise RuntimeError(f"Service on port {port} did not become ready")


def smoke(project):
    compose = ["docker", "compose", "-p", project, "--env-file", "deploy/compose/.env.example",
               "-f", "deploy/compose/compose.yaml"]

    def run_command(args, data=None, timeout=45):
        subprocess.run(compose + args, input=data, text=True, cwd=ROOT, check=True, timeout=timeout)

    def fixture(seconds, delivery):
        run, assignment = str(uuid4()), str(uuid4())
        start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=seconds)
        manifest = {"schemaVersion": 1, "runId": run, "startAt": timestamp(start),
                    "resultDeadline": timestamp(datetime.now(timezone.utc) + timedelta(seconds=delivery)),
                    "snapshot": {"totalRps": 1, "durationMs": seconds * 1000},
                    "assignments": [{"assignmentId": assignment, "agentId": "fixture", "assignedRps": 1,
                                     "plannedRequests": str(seconds)}]}
        run_command(["exec", "-T", "postgres", "psql", "-X", "-U", "postgres", "-d", "results", "-v",
                     "ON_ERROR_STOP=1", "-v", f"manifest={json.dumps(manifest)}", "-v", f"run={run}"],
                    "INSERT INTO run_manifests(run_id,payload) VALUES (:'run', :'manifest');\n")
        return run, assignment, start

    def batch(run, assignment, start, sequence, bucket, final=False):
        begin = start + timedelta(seconds=sequence - 1)
        end = begin + timedelta(seconds=1)
        buckets = ["0"] * 20
        buckets[bucket] = "1"
        payload = {"schemaVersion": 1, "runId": run, "assignmentId": assignment, "agentId": "fixture",
                   "sequence": str(sequence), "interval": {"startAt": timestamp(begin), "endAt": timestamp(end)},
                   "counters": {key: "1" for key in ("scheduled", "started", "finished", "httpResponses")},
                   "httpStatusCounts": [{"statusCode": 200, "count": "1"}],
                   "httpLatency": {"layoutId": "latency-us-v1", "bucketCounts": buckets},
                   "failureLatency": {"layoutId": "latency-us-v1", "bucketCounts": ["0"] * 20}}
        if final:
            payload.update(isFinal=True, summary={"state": COMPLETED, "actualStartAt": timestamp(start),
                           "generationStoppedAt": timestamp(end), "finishedAt": timestamp(end),
                           "generationDurationUs": str(sequence * 1_000_000)})
        return payload

    def publish(assignment, *payloads):
        run_command(["exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server",
                     "kafka:9092", "--topic", "latencystorm.results.v1", "--property", "parse.key=true",
                     "--property", "key.separator=|"],
                    "".join(f"{assignment}|{json.dumps(payload)}\n" for payload in payloads))

    normal, agent, start = fixture(2, 120)
    first = batch(normal, agent, start, 1, 2)
    final = batch(normal, agent, start, 2, 3, True)
    publish(agent, final, first, dict(reversed(list(first.items()))))
    report = wait_report(normal, lambda value: value["reportState"] == "Complete")
    require(int(report["counters"]["started"]) == 2, "Duplicate inflated observed starts")
    require([report["httpLatency"][key]["valueUs"] for key in ("p50", "p95", "p99")] == [500, 1000, 1000],
            "Unexpected merged nearest-rank percentiles")
    require(report["observedStartRps"] == 1, "Unexpected generation RPS")
    require(get(normal, "timeline?limit=1")["nextAfter"] == "1", "Timeline cursor missing")
    require(get(normal, "timeline?after=1")["points"][0]["isFinal"], "Timeline final missing")

    gap, agent, start = fixture(2, 18)
    publish(agent, batch(gap, agent, start, 2, 3, True))
    report = wait_report(gap, lambda value: value["reportState"] == "Incomplete")
    require("MissingBatch" in report["reasons"], "Final packet hid a missing sequence")
    before = report["counters"]

    invalid, other, other_start = fixture(1, 120)
    bad = batch(invalid, other, other_start, 1, 2, True)
    publish(other, {**bad, "schemaVersion": 2}, bad)
    publish(agent, batch(gap, agent, start, 1, 2))
    report = wait_report(invalid, lambda value: value["counters"] is not None)
    require(report["reportState"] == "Incomplete" and "InvalidBatch" in report["reasons"],
            "Invalid schema was not recorded or blocked the next valid batch")

    run_command(["restart", "results-service", "control-api"])
    wait_ready(8080)
    wait_ready(8081)
    require(get(normal)["reportState"] == "Complete" and int(get(normal)["counters"]["started"]) == 2,
            "Report changed after restart")
    require(get(gap)["counters"] == before and get(gap)["reportState"] == "Incomplete",
            "Late packet promoted the expired report")
    print("PASS: stage 6 aggregation, deduplication, deadline, quarantine and restart.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="latencystorm-stage06")
    smoke(parser.parse_args().project)
