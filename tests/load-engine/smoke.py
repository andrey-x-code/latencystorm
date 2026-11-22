#!/usr/bin/env python3
"""Check standalone load generation against the local Demo Target processes."""

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
from urllib.error import URLError
from urllib.request import urlopen


def wait_ready(process):
    """Wait for readiness with a deadline and detect early process exit."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Demo Target exited before readiness")
        try:
            with urlopen("http://127.0.0.1:8080/ready", timeout=1) as response:
                if response.status == 200:
                    return
        except (URLError, TimeoutError):
            pass
        time.sleep(0.05)
    raise RuntimeError("Demo Target readiness deadline exceeded")


def check_result(records):
    """Check counter, histogram and interval invariants on observable NDJSON output."""
    result = records[-1]["result"]
    totals = {key: int(value) for key, value in result["totals"].items()}
    intervals = [record["interval"] for record in records[:-1]] + [result["final"]]
    sums = dict.fromkeys(totals, 0)
    previous = None
    for interval in intervals:
        if previous is not None and previous != interval["startAt"]:
            raise RuntimeError("Measurement interval gap")
        previous = interval["endAt"]
        counters = {key: int(value) for key, value in interval["counters"].items()}
        assert counters["scheduled"] == counters["started"] + counters["skipped"]
        assert counters["finished"] == sum(counters[key] for key in (
            "httpResponses", "transportErrors", "timeouts", "cancelled",
            "responseBodyLimitExceeded"))
        assert sum(interval["httpStatusCounts"].values()) == counters["httpResponses"]
        for name, expected in (("httpLatency", counters["httpResponses"]),
                               ("failureLatency", counters["finished"] - counters["httpResponses"])):
            histogram = interval[name]
            assert histogram["layoutId"] == "latency-us-v1"
            assert len(histogram["bucketCounts"]) == 20
            assert sum(histogram["bucketCounts"]) + int(histogram["overflowCount"]) == expected
        for key, value in counters.items():
            sums[key] += value
    assert sums == totals
    assert totals["started"] == totals["finished"]
    assert not result["measurementsLost"]
    return result, totals


def scenario(name, delay_ms, concurrency, timeout_ms, stop=False):
    """Run one bounded local scenario and always stop its child processes."""
    target = subprocess.Popen(
        ["/bin/demo-target"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env={**os.environ, "DEMO_DELAY_MS": str(delay_ms)})
    agent = None
    try:
        wait_ready(target)
        config = {
            "method": "GET", "url": "http://127.0.0.1:8080/", "headers": {},
            "rps": 20, "durationMs": 10000 if stop else 2000,
            "requestTimeoutMs": timeout_ms, "maxConcurrency": concurrency,
            "maxResponseBodyBytes": 1048576, "drainTimeoutMs": 200,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            agent = subprocess.Popen(["/bin/load-agent", "--local", str(path)],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            first = None
            if stop:
                first = agent.stdout.readline()
                assert json.loads(first)["type"] == "interval"
                agent.send_signal(signal.SIGTERM)
            output, error = agent.communicate(timeout=15)
            assert agent.returncode == 0, error
            records = [json.loads(line) for line in ((first or "") + output).splitlines()]
            result, totals = check_result(records)
            if stop:
                assert result["stopped"] and totals["started"] < int(result["plannedRequests"])
            else:
                assert totals["scheduled"] == 40
                if name == "success":
                    assert totals["httpResponses"] >= 38 and totals["skipped"] <= 2
                elif name == "timeouts":
                    assert totals["timeouts"] > 0 and totals["httpResponses"] == 0
                elif name == "concurrency":
                    assert totals["skipped"] > totals["started"] > 0
            print(f"PASS {name}: {json.dumps(totals, sort_keys=True)}", flush=True)
    finally:
        if agent is not None and agent.poll() is None:
            agent.kill()
            agent.wait(timeout=5)
        target.terminate()
        try:
            target.wait(timeout=5)
        except subprocess.TimeoutExpired:
            target.kill()
            target.wait(timeout=5)


if __name__ == "__main__":
    scenario("success", 0, 10, 1000)
    scenario("timeouts", 200, 10, 50)
    scenario("concurrency", 200, 1, 1000)
    scenario("manual stop", 200, 10, 1000, stop=True)
