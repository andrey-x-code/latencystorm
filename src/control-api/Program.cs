using LatencyStorm.ServiceDefaults;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

await ServiceHost.RunAsync(args, "control-api", app =>
{
    app.MapPost("/scenarios", async (ScenarioRequest request, NpgsqlDataSource db, CancellationToken ct) =>
    {
        var validation = request.Validate();
        if (validation is not null) return Results.BadRequest(new { error = validation });
        var id = Guid.NewGuid();
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand("INSERT INTO scenarios(id, name, method, url, headers, body, target_rps, duration_seconds, request_timeout_ms, concurrency_limit) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", connection);
        command.Parameters.AddWithValue(id); command.Parameters.AddWithValue(request.Name); command.Parameters.AddWithValue(request.Method); command.Parameters.AddWithValue(request.Url);
        command.Parameters.Add(new NpgsqlParameter { Value = JsonSerializer.Serialize(request.Headers), NpgsqlDbType = NpgsqlDbType.Jsonb });
        command.Parameters.AddWithValue((object?)request.Body ?? DBNull.Value); command.Parameters.AddWithValue(request.TargetRps); command.Parameters.AddWithValue(request.DurationSeconds); command.Parameters.AddWithValue(request.RequestTimeoutMs); command.Parameters.AddWithValue(request.ConcurrencyLimit);
        await command.ExecuteNonQueryAsync(ct);
        return Results.Created($"/scenarios/{id}", new { id, request.Name, request.Method, request.Url, request.Headers, request.Body, request.TargetRps, request.DurationSeconds, request.RequestTimeoutMs, request.ConcurrencyLimit });
    }).WithName("CreateScenario");

    app.MapGet("/scenarios", async (NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var command = db.CreateCommand("SELECT id,name,method,url,headers,body,target_rps,duration_seconds,request_timeout_ms,concurrency_limit,created_at FROM scenarios ORDER BY created_at DESC");
        await using var reader = await command.ExecuteReaderAsync(ct); var items = new List<object>();
        while (await reader.ReadAsync(ct)) items.Add(ReadScenario(reader));
        return Results.Ok(items);
    });

    app.MapGet("/scenarios/{id:guid}", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var command = db.CreateCommand("SELECT id,name,method,url,headers,body,target_rps,duration_seconds,request_timeout_ms,concurrency_limit,created_at FROM scenarios WHERE id=$1"); command.Parameters.AddWithValue(id);
        await using var reader = await command.ExecuteReaderAsync(ct); return await reader.ReadAsync(ct) ? Results.Ok(ReadScenario(reader)) : Results.NotFound();
    });

    app.MapPost("/runs", async (RunRequest request, NpgsqlDataSource db, CancellationToken ct) =>
    {
        if (request.ScenarioId == Guid.Empty) return Results.BadRequest(new { error = "scenarioId is required" });
        await using var connection = await db.OpenConnectionAsync(ct); await using var tx = await connection.BeginTransactionAsync(ct);
        await using var scenario = new NpgsqlCommand("SELECT name,method,url,headers,body,target_rps,duration_seconds,request_timeout_ms,concurrency_limit FROM scenarios WHERE id=$1 FOR SHARE", connection, tx); scenario.Parameters.AddWithValue(request.ScenarioId);
        await using var sr = await scenario.ExecuteReaderAsync(ct); if (!await sr.ReadAsync(ct)) return Results.NotFound(new { error = "scenario not found" });
        var snapshot = new { name = sr.GetString(0), method = sr.GetString(1), url = sr.GetString(2), headers = sr.GetString(3), body = sr.IsDBNull(4) ? null : sr.GetString(4), targetRps = sr.GetInt32(5), durationSeconds = sr.GetInt32(6), requestTimeoutMs = sr.GetInt32(7), concurrencyLimit = sr.GetInt32(8) }; await sr.CloseAsync();
        await using var active = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM runs WHERE state IN ('pending','running','stopping'))", connection, tx); if ((bool)(await active.ExecuteScalarAsync(ct) ?? false)) return Results.Conflict(new { error = "an active run already exists" });
        await using var slot = new NpgsqlCommand("SELECT id FROM agents WHERE state='ready' AND reserved_run_id IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1", connection, tx); var agent = await slot.ExecuteScalarAsync(ct); if (agent is null) return Results.Conflict(new { error = "no ready agents available" });
        var id = Guid.NewGuid(); await using var insert = new NpgsqlCommand("INSERT INTO runs(id,scenario_id,scenario_snapshot,state) VALUES ($1,$2,$3,'pending'); INSERT INTO assignments(id,run_id,agent_id,target_rps,state) VALUES ($4,$1,$5,$6,'assigned'); UPDATE agents SET reserved_run_id=$1 WHERE id=$5", connection, tx); insert.Parameters.AddWithValue(id); insert.Parameters.AddWithValue(request.ScenarioId); insert.Parameters.Add(new NpgsqlParameter { Value = JsonSerializer.Serialize(snapshot), NpgsqlDbType = NpgsqlDbType.Jsonb }); insert.Parameters.AddWithValue(Guid.NewGuid()); insert.Parameters.AddWithValue((Guid)agent); insert.Parameters.AddWithValue(snapshot.targetRps); await insert.ExecuteNonQueryAsync(ct); await tx.CommitAsync(ct);
        return Results.Created($"/runs/{id}", new { id, scenarioId = request.ScenarioId, state = "pending", snapshot });
    });
    app.MapPost("/agents/{id:guid}/ready", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var command = db.CreateCommand("INSERT INTO agents(id,state) VALUES ($1,'ready') ON CONFLICT (id) DO UPDATE SET state='ready', reserved_run_id=NULL");
        command.Parameters.AddWithValue(id);
        await command.ExecuteNonQueryAsync(ct);
        return Results.Ok(new { id, state = "ready" });
    });

    app.MapGet("/runs", async (NpgsqlDataSource db, CancellationToken ct) => { await using var c=db.CreateCommand("SELECT id,scenario_id,state,created_at,stopped_at FROM runs ORDER BY created_at DESC"); await using var r=await c.ExecuteReaderAsync(ct); var list=new List<object>(); while(await r.ReadAsync(ct)) list.Add(new { id=r.GetGuid(0), scenarioId=r.GetGuid(1), state=r.GetString(2), createdAt=r.GetDateTime(3), stoppedAt=r.IsDBNull(4)?(DateTime?)null:r.GetDateTime(4) }); return Results.Ok(list); });
    app.MapGet("/runs/{id:guid}", async (Guid id,NpgsqlDataSource db,CancellationToken ct) => { await using var c=db.CreateCommand("SELECT id,scenario_id,state,created_at,stopped_at,scenario_snapshot FROM runs WHERE id=$1"); c.Parameters.AddWithValue(id); await using var r=await c.ExecuteReaderAsync(ct); return await r.ReadAsync(ct)?Results.Ok(new { id=r.GetGuid(0),scenarioId=r.GetGuid(1),state=r.GetString(2),createdAt=r.GetDateTime(3),stoppedAt=r.IsDBNull(4)?(DateTime?)null:r.GetDateTime(4),snapshot=r.GetString(5)}):Results.NotFound(); });
    app.MapPost("/runs/{id:guid}/stop", async (Guid id,NpgsqlDataSource db,CancellationToken ct) => { await using var c=db.CreateCommand("UPDATE runs SET state=CASE WHEN state IN ('pending','running') THEN 'stopping' ELSE state END, stopped_at=CASE WHEN state IN ('pending','running') THEN now() ELSE stopped_at END WHERE id=$1 RETURNING state"); c.Parameters.AddWithValue(id); var state=await c.ExecuteScalarAsync(ct); return state is null?Results.NotFound():Results.Ok(new { id,state }); });
    app.MapPost("/runs/{id:guid}/complete", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var update = new NpgsqlCommand("UPDATE runs SET state='completed' WHERE id=$1 AND state IN ('pending','running','stopping') RETURNING state", connection, transaction);
        update.Parameters.AddWithValue(id);
        var state = await update.ExecuteScalarAsync(ct);
        if (state is null) return Results.NotFound();
        await using var release = new NpgsqlCommand("UPDATE agents SET reserved_run_id=NULL, state='ready' WHERE reserved_run_id=$1", connection, transaction);
        release.Parameters.AddWithValue(id);
        await release.ExecuteNonQueryAsync(ct);
        await transaction.CommitAsync(ct);
        return Results.Ok(new { id, state });
    });
    app.MapGet("/openapi.json", () => Results.Json(new { openapi="3.0.3", info=new { title="LatencyStorm Control API", version="v1" }, paths=new { scenarios=new { post=new { summary="Create scenario" }, get=new { summary="List scenarios" } }, runs=new { post=new { summary="Create run" }, get=new { summary="List runs" } } } }));
});

static object ReadScenario(NpgsqlDataReader r) => new { id=r.GetGuid(0), name=r.GetString(1), method=r.GetString(2), url=r.GetString(3), headers=r.GetString(4), body=r.IsDBNull(5)?null:r.GetString(5), targetRps=r.GetInt32(6), durationSeconds=r.GetInt32(7), requestTimeoutMs=r.GetInt32(8), concurrencyLimit=r.GetInt32(9), createdAt=r.GetDateTime(10) };
record RunRequest(Guid ScenarioId);
record ScenarioRequest(string Name,string Method,string Url,Dictionary<string,string>? Headers,string? Body,int TargetRps,int DurationSeconds,int RequestTimeoutMs,int ConcurrencyLimit)
{ public string? Validate() { if(string.IsNullOrWhiteSpace(Name)||Name.Length>200)return "name is required and must be <= 200 characters"; if(Headers is null)return "headers is required"; if(Headers.Count>100)return "headers must contain at most 100 entries"; if(Headers.Any(h => h.Key.Length>256 || h.Value.Length>8192))return "header names must be <= 256 and values <= 8192 characters"; if(Body is not null && Body.Length>1_048_576)return "body must be <= 1048576 characters"; if(!Uri.TryCreate(Url,UriKind.Absolute,out var uri)||uri.Scheme is not ("http" or "https")||string.IsNullOrWhiteSpace(uri.Host))return "url must be an absolute HTTP or HTTPS URL"; if(Method is not ("GET" or "POST" or "PUT" or "PATCH" or "DELETE" or "HEAD"))return "unsupported method"; if(TargetRps is <1 or >1000000)return "targetRps must be 1..1000000"; if(DurationSeconds is <1 or >86400)return "durationSeconds must be 1..86400"; if(RequestTimeoutMs is <1 or >300000)return "requestTimeoutMs must be 1..300000"; if(ConcurrencyLimit is <1 or >100000)return "concurrencyLimit must be 1..100000"; return null; } }
