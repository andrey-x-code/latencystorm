using Dapper;
using Google.Protobuf;
using Grpc.Core;
using LatencyStorm.Contracts.V1;
using Npgsql;

/// <summary>Сохраняет неизменяемый план запуска и его терминальные метаданные.</summary>
sealed class ManifestService(NpgsqlDataSource db) : ResultsIngestion.ResultsIngestionBase
{
    /// <summary>Подтверждает запись только после сохранения манифеста в PostgreSQL.</summary>
    public override async Task<ManifestAccepted> PutRunManifest(RunManifest request, ServerCallContext context)
    {
        if (!Guid.TryParseExact(request.RunId, "D", out var id) || request.SchemaVersion != 1 ||
            request.Snapshot?.Request is null || request.Assignments.Count != 1 ||
            request.StartAt is null || request.ResultDeadline is null)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid manifest"));
        }

        var assignment = request.Assignments[0];
        if (!Guid.TryParseExact(assignment.AssignmentId, "D", out _) || string.IsNullOrEmpty(assignment.AgentId) ||
            assignment.AssignedRps == 0 || assignment.AssignedRps != request.Snapshot.TotalRps ||
            request.Snapshot.DurationMs == 0 || assignment.PlannedRequests !=
                ((ulong)assignment.AssignedRps * request.Snapshot.DurationMs + 999) / 1000)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid assignment plan"));
        }

        await using var connection = await db.OpenConnectionAsync(context.CancellationToken);
        var accepted = await connection.QuerySingleOrDefaultAsync<Guid?>(new CommandDefinition("""
            INSERT INTO run_manifests (run_id, payload) VALUES (@Id, CAST(@Payload AS jsonb))
            ON CONFLICT (run_id) DO UPDATE SET payload = EXCLUDED.payload
            WHERE run_manifests.payload = EXCLUDED.payload RETURNING run_id
            """, new { Id = id, Payload = JsonFormatter.Default.Format(request) },
            cancellationToken: context.CancellationToken));
        return accepted is null
            ? throw new RpcException(new Status(StatusCode.AlreadyExists, "Conflicting manifest"))
            : new ManifestAccepted { RunId = request.RunId };
    }

    /// <summary>Подтверждает терминальное состояние отдельно от доставки результатов.</summary>
    public override async Task<ManifestAccepted> FinalizeRun(RunFinalization request, ServerCallContext context)
    {
        if (!Guid.TryParseExact(request.RunId, "D", out var id) || request.FinishedAt is null ||
            request.State is not (RunState.Completed or RunState.Stopped or RunState.Failed))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid finalization"));
        }

        await using var connection = await db.OpenConnectionAsync(context.CancellationToken);
        var count = await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE run_manifests SET finalization = CAST(@Payload AS jsonb)
            WHERE run_id = @Id AND (finalization IS NULL OR finalization = CAST(@Payload AS jsonb))
            """, new { Id = id, Payload = JsonFormatter.Default.Format(request) },
            cancellationToken: context.CancellationToken));
        if (count == 0)
        {
            var exists = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(
                "SELECT EXISTS (SELECT 1 FROM run_manifests WHERE run_id = @Id)", new { Id = id },
                cancellationToken: context.CancellationToken));
            throw new RpcException(new Status(exists ? StatusCode.AlreadyExists : StatusCode.NotFound, "Finalization not accepted"));
        }

        return new ManifestAccepted { RunId = request.RunId };
    }
}
