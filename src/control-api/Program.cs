using System.Text.Json;
using Dapper;
using LatencyStorm.ServiceDefaults;
using Npgsql;

await ServiceHost.RunAsync(args, "control-api", app =>
{
    app.MapPost("/scenarios", async (ScenarioRequest request, NpgsqlDataSource db, CancellationToken ct) =>
    {
        var validation = request.Validate();
        if (validation is not null)
        {
            return Results.BadRequest(new { error = validation });
        }

        var id = Guid.NewGuid();
        await using var connection = await db.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO scenarios (
                id, name, method, url, headers, body, target_rps,
                duration_seconds, request_timeout_ms, concurrency_limit
            )
            VALUES (
                @Id, @Name, @Method, @Url, CAST(@Headers AS jsonb), @Body, @TargetRps,
                @DurationSeconds, @RequestTimeoutMs, @ConcurrencyLimit
            )
            """,
            new
            {
                Id = id,
                request.Name,
                request.Method,
                request.Url,
                Headers = JsonSerializer.Serialize(request.Headers),
                request.Body,
                request.TargetRps,
                request.DurationSeconds,
                request.RequestTimeoutMs,
                request.ConcurrencyLimit
            },
            cancellationToken: ct));

        return Results.Created($"/scenarios/{id}", new
        {
            id,
            request.Name,
            request.Method,
            request.Url,
            request.Headers,
            request.Body,
            request.TargetRps,
            request.DurationSeconds,
            request.RequestTimeoutMs,
            request.ConcurrencyLimit
        });
    }).WithName("CreateScenario");

    app.MapGet("/scenarios", async (NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        var scenarios = await connection.QueryAsync<ScenarioRow>(new CommandDefinition(
            ScenarioRow.SelectSql + " ORDER BY created_at DESC", cancellationToken: ct));
        return Results.Ok(scenarios);
    });

    app.MapGet("/scenarios/{id:guid}", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        var scenario = await connection.QuerySingleOrDefaultAsync<ScenarioRow>(new CommandDefinition(
            ScenarioRow.SelectSql + " WHERE id = @Id", new { Id = id }, cancellationToken: ct));
        return scenario is null ? Results.NotFound() : Results.Ok(scenario);
    });

    app.MapPost("/runs", async (RunRequest request, NpgsqlDataSource db, CancellationToken ct) =>
    {
        if (request.ScenarioId == Guid.Empty)
        {
            return Results.BadRequest(new { error = "scenarioId is required" });
        }

        await using var connection = await db.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var scenario = await connection.QuerySingleOrDefaultAsync<ScenarioRow>(new CommandDefinition(
            ScenarioRow.SelectSql + " WHERE id = @ScenarioId FOR SHARE",
            new { request.ScenarioId }, transaction, cancellationToken: ct));
        if (scenario is null)
        {
            return Results.NotFound(new { error = "scenario not found" });
        }

        var snapshot = new
        {
            name = scenario.Name,
            method = scenario.Method,
            url = scenario.Url,
            headers = scenario.Headers,
            body = scenario.Body,
            targetRps = scenario.TargetRps,
            durationSeconds = scenario.DurationSeconds,
            requestTimeoutMs = scenario.RequestTimeoutMs,
            concurrencyLimit = scenario.ConcurrencyLimit
        };
        var active = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(
            "SELECT EXISTS (SELECT 1 FROM runs WHERE state IN ('pending', 'running', 'stopping'))",
            transaction: transaction, cancellationToken: ct));
        if (active)
        {
            return Results.Conflict(new { error = "an active run already exists" });
        }

        var agentId = await connection.QuerySingleOrDefaultAsync<Guid?>(new CommandDefinition(
            """
            SELECT id FROM agents
            WHERE state = 'ready' AND reserved_run_id IS NULL
            ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
            """,
            transaction: transaction, cancellationToken: ct));
        if (agentId is null)
        {
            return Results.Conflict(new { error = "no ready agents available" });
        }

        var id = Guid.NewGuid();
        await connection.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO runs (id, scenario_id, scenario_snapshot, state)
            VALUES (@Id, @ScenarioId, CAST(@Snapshot AS jsonb), 'pending');

            INSERT INTO assignments (id, run_id, agent_id, target_rps, state)
            VALUES (@AssignmentId, @Id, @AgentId, @TargetRps, 'assigned');

            UPDATE agents SET reserved_run_id = @Id WHERE id = @AgentId;
            """,
            new
            {
                Id = id,
                request.ScenarioId,
                Snapshot = JsonSerializer.Serialize(snapshot),
                AssignmentId = Guid.NewGuid(),
                AgentId = agentId.Value,
                scenario.TargetRps
            },
            transaction, cancellationToken: ct));
        await transaction.CommitAsync(ct);

        return Results.Created($"/runs/{id}", new { id, scenarioId = request.ScenarioId, state = "pending", snapshot });
    });

    app.MapPost("/agents/{id:guid}/ready", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO agents (id, state) VALUES (@Id, 'ready')
            ON CONFLICT (id) DO UPDATE SET state = 'ready', reserved_run_id = NULL
            """,
            new { Id = id }, cancellationToken: ct));
        return Results.Ok(new { id, state = "ready" });
    });

    app.MapGet("/runs", async (NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        var runs = await connection.QueryAsync<RunRow>(new CommandDefinition(
            RunRow.SelectSql + " ORDER BY created_at DESC", cancellationToken: ct));
        return Results.Ok(runs);
    });

    app.MapGet("/runs/{id:guid}", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        var run = await connection.QuerySingleOrDefaultAsync<RunDetailsRow>(new CommandDefinition(
            """
            SELECT id, scenario_id AS "ScenarioId", state, created_at AS "CreatedAt",
                stopped_at AS "StoppedAt", scenario_snapshot AS "Snapshot"
            FROM runs WHERE id = @Id
            """,
            new { Id = id }, cancellationToken: ct));
        return run is null ? Results.NotFound() : Results.Ok(run);
    });

    app.MapPost("/runs/{id:guid}/stop", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        var state = await connection.QuerySingleOrDefaultAsync<string>(new CommandDefinition(
            """
            UPDATE runs
            SET state = CASE WHEN state IN ('pending', 'running') THEN 'stopping' ELSE state END,
                stopped_at = CASE WHEN state IN ('pending', 'running') THEN now() ELSE stopped_at END
            WHERE id = @Id
            RETURNING state
            """,
            new { Id = id }, cancellationToken: ct));
        return state is null ? Results.NotFound() : Results.Ok(new { id, state });
    });

    app.MapPost("/runs/{id:guid}/complete", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var state = await connection.QuerySingleOrDefaultAsync<string>(new CommandDefinition(
            """
            UPDATE runs SET state = 'completed'
            WHERE id = @Id AND state IN ('pending', 'running', 'stopping')
            RETURNING state
            """,
            new { Id = id }, transaction, cancellationToken: ct));
        if (state is null)
        {
            return Results.NotFound();
        }

        await connection.ExecuteAsync(new CommandDefinition(
            "UPDATE agents SET reserved_run_id = NULL, state = 'ready' WHERE reserved_run_id = @Id",
            new { Id = id }, transaction, cancellationToken: ct));
        await transaction.CommitAsync(ct);
        return Results.Ok(new { id, state });
    });

    app.MapGet("/openapi.json", () => Results.Json(new
    {
        openapi = "3.0.3",
        info = new { title = "LatencyStorm Control API", version = "v1" },
        paths = new
        {
            scenarios = new { post = new { summary = "Create scenario" }, get = new { summary = "List scenarios" } },
            runs = new { post = new { summary = "Create run" }, get = new { summary = "List runs" } }
        }
    }));
});
