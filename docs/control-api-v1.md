# Control API v1

Control API stores scenarios and run state in the control PostgreSQL database. Register an available agent before creating a run:

```powershell
$agent = [guid]::NewGuid()
Invoke-RestMethod http://localhost:8080/agents/$agent/ready -Method Post
$scenario = Invoke-RestMethod http://localhost:8080/scenarios -Method Post -ContentType 'application/json' -Body (@{
  name='demo'; method='GET'; url='http://demo-target:8080/'; headers=@{};
  targetRps=10; durationSeconds=30; requestTimeoutMs=1000; concurrencyLimit=10
} | ConvertTo-Json)
Invoke-RestMethod http://localhost:8080/runs -Method Post -ContentType 'application/json' -Body (@{ scenarioId=$scenario.id } | ConvertTo-Json)
```

Scenario URLs must be absolute HTTP or HTTPS URLs. RPS, duration, timeout, and concurrency are bounded at the API boundary. A run stores an immutable JSON snapshot of its scenario. Only one `pending`, `running`, or `stopping` run can exist; PostgreSQL enforces this invariant. `POST /runs/{id}/stop` is idempotent and transitions active runs to `stopping`.

The current stage exposes a small agent registration endpoint for local orchestration. Agent gRPC assignment and load execution are implemented in later stages.
