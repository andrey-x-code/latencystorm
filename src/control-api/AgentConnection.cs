using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Dapper;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using LatencyStorm.Contracts.V1;
using Npgsql;

/// <summary>Ограничивает координатор MVP одной активной сессией агента.</summary>
sealed class AgentSlot : SemaphoreSlim
{
    private volatile string? agentId;

    /// <summary>Создаёт единственное место для подключения агента.</summary>
    public AgentSlot() : base(1, 1) { }

    public Guid? AgentId
    {
        get => agentId is string value ? Guid.Parse(value) : null;
        set => agentId = value?.ToString();
    }
}

/// <summary>Сопоставляет сохранённое назначение с командами единственной живой сессии.</summary>
sealed partial class AgentConnection(NpgsqlDataSource db, AgentSlot slot, ResultsIngestion.ResultsIngestionClient results,
    ILogger<AgentConnection> logger) : AgentControl.AgentControlBase
{
    /// <summary>Проверяет допустимые символы стабильного идентификатора агента.</summary>
    [GeneratedRegex("^[A-Za-z0-9._-]{1,128}\\z")]
    private static partial Regex AgentIdPattern();

    /// <summary>Регистрирует поток и последовательно обслуживает команды и ответы.</summary>
    public override async Task Connect(IAsyncStreamReader<AgentMessage> input, IServerStreamWriter<CoordinatorMessage> output,
        ServerCallContext context)
    {
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        lifetime.CancelAfter(TimeSpan.FromSeconds(5));
        if (!await input.MoveNext(lifetime.Token))
        {
            return;
        }

        var registration = input.Current;
        if (registration.Register?.ProtocolVersion != 1 || !AgentIdPattern().IsMatch(registration.AgentId) ||
            !Guid.TryParseExact(registration.SessionId, "D", out var sessionId) || sessionId == Guid.Empty)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid registration"));
        }

        if (!await slot.WaitAsync(0, lifetime.Token))
        {
            throw new RpcException(new Status(StatusCode.ResourceExhausted, "Agent slot occupied"));
        }

        lifetime.CancelAfter(Timeout.InfiniteTimeSpan);
        Guid? agent = null;
        Task<bool>? read = null;
        try
        {
            agent = await RegisterAsync(registration.AgentId, lifetime.Token);
            await output.WriteAsync(new CoordinatorMessage
            {
                SessionId = registration.SessionId,
                CommandId = Guid.NewGuid().ToString(),
                Registered = new RegistrationAccepted
                {
                    ProtocolVersion = 1,
                    HeartbeatIntervalMs = 1000,
                    LeaseDurationMs = 5000,
                    CoordinatorTime = Timestamp.FromDateTime(DateTime.UtcNow)
                }
            }, lifetime.Token);
            slot.AgentId = agent;

            var heartbeat = Stopwatch.GetTimestamp();
            ActiveAssignment? active = null;
            read = input.MoveNext(lifetime.Token);
            while (!lifetime.IsCancellationRequested)
            {
                if (read.IsCompleted)
                {
                    if (!await read)
                    {
                        break;
                    }

                    var message = input.Current;
                    if (message.SessionId != registration.SessionId || message.AgentId != registration.AgentId)
                    {
                        throw new RpcException(new Status(StatusCode.InvalidArgument, "Wrong agent session"));
                    }

                    if (message.Heartbeat is not null)
                    {
                        heartbeat = Stopwatch.GetTimestamp();
                    }

                    if (active is not null && await HandleAsync(active, message, output, registration.SessionId, lifetime.Token))
                    {
                        active = null;
                    }

                    read = input.MoveNext(lifetime.Token);
                }

                if (Stopwatch.GetElapsedTime(heartbeat) >= TimeSpan.FromSeconds(5))
                {
                    break;
                }

                active = await AdvanceAsync(active, agent.Value, output, registration.SessionId, lifetime.Token);
                await Task.WhenAny(read, Task.Delay(200, lifetime.Token));
            }
        }
        finally
        {
            slot.AgentId = null;
            lifetime.Cancel();
            try
            {
                if (read is not null)
                {
                    try
                    {
                        await read;
                    }
                    catch (Exception exception) when (exception is OperationCanceledException or IOException or RpcException)
                    {
                        logger.LogDebug("Agent stream closed: {AgentId} {SessionId}", registration.AgentId, registration.SessionId);
                    }
                }

                if (agent is Guid id)
                {
                    using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    await using var connection = await db.OpenConnectionAsync(cleanup.Token);
                    await connection.ExecuteAsync(new CommandDefinition("""
                        UPDATE agents SET state = 'offline' WHERE id = @Id;
                        UPDATE assignments SET state = 'failed' WHERE id IN (
                            SELECT a.id FROM assignments a JOIN agents g ON g.reserved_run_id = a.run_id WHERE g.id = @Id);
                        UPDATE runs SET state = 'failed' WHERE id = (
                            SELECT reserved_run_id FROM agents WHERE id = @Id) AND state IN ('pending','running','stopping');
                        """, new { Id = id }, cancellationToken: cleanup.Token));
                }
            }
            finally
            {
                slot.Release();
            }
        }
    }

    /// <summary>При переподключении освобождает старое назначение только после истечения безопасного срока.</summary>
    private async Task<Guid> RegisterAsync(string agentId, CancellationToken ct)
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition("SELECT pg_advisory_xact_lock(71294022)",
            transaction: tx, cancellationToken: ct));
        var unsafeRun = await connection.ExecuteScalarAsync<bool>(new CommandDefinition("""
            SELECT EXISTS (SELECT 1 FROM assignments WHERE safe_after > now())
            """, transaction: tx, cancellationToken: ct));
        if (unsafeRun)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, "Previous lease has not drained"));
        }

        await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE agents SET state = 'offline', reserved_run_id = NULL;
            UPDATE assignments SET state = 'failed' WHERE run_id IN (
                SELECT id FROM runs WHERE state IN ('pending','running','stopping'));
            UPDATE runs SET state = 'failed' WHERE state IN ('pending','running','stopping');
            """, transaction: tx, cancellationToken: ct));
        var id = await connection.QuerySingleAsync<Guid>(new CommandDefinition("""
            INSERT INTO agents (id, external_id, state) VALUES (@Id, @ExternalId, 'ready')
            ON CONFLICT (external_id) DO UPDATE SET state = 'ready' RETURNING id
            """, new { Id = Guid.NewGuid(), ExternalId = agentId }, tx, cancellationToken: ct));
        await tx.CommitAsync(ct);
        return id;
    }

    /// <summary>Читает резервирование и остановку из БД; продлевает только действующее разрешение.</summary>
    private async Task<ActiveAssignment?> AdvanceAsync(ActiveAssignment? active, Guid agent,
        IServerStreamWriter<CoordinatorMessage> output, string session, CancellationToken ct)
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        if (active is null)
        {
            var row = await connection.QuerySingleOrDefaultAsync<AssignmentRow>(new CommandDefinition("""
                SELECT a.id AS "AssignmentId", r.id AS "RunId", r.scenario_id AS "ScenarioId",
                    r.scenario_snapshot AS "Snapshot", g.external_id AS "AgentId"
                FROM agents g JOIN runs r ON g.reserved_run_id = r.id
                JOIN assignments a ON a.run_id = r.id AND a.agent_id = g.id WHERE g.id = @Agent
                """, new { Agent = agent }, cancellationToken: ct));
            if (row is null)
            {
                return null;
            }

            var snapshot = row.ToContract();
            active = new ActiveAssignment(row, snapshot);
            await output.WriteAsync(new CoordinatorMessage
            {
                SessionId = session,
                CommandId = active.PrepareId,
                Prepare = new PrepareAssignment
                {
                    RunId = active.RunId,
                    AssignmentId = active.AssignmentId,
                    Snapshot = snapshot,
                    AssignedRps = snapshot.TotalRps,
                    PrepareDeadline = Timestamp.FromDateTime(DateTime.UtcNow.AddSeconds(5))
                }
            }, ct);
            return active;
        }

        if (!active.Ready && Stopwatch.GetElapsedTime(active.PreparedAt) >= TimeSpan.FromSeconds(5))
        {
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Agent did not prepare"));
        }

        if (active.FinishDeadline is DateTime deadline && DateTime.UtcNow >= deadline)
        {
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Assignment did not finish"));
        }

        if (!active.Ready || active.StopSent)
        {
            return active;
        }

        var state = await connection.QuerySingleAsync<string>(new CommandDefinition("SELECT state FROM runs WHERE id = @Id",
            new { Id = active.Row.RunId }, cancellationToken: ct));
        if (state == "stopping")
        {
            await SendStopAsync(active, output, session, StopReason.UserRequest, ct);
        }
        else if (active.Lease > 0 && Stopwatch.GetElapsedTime(active.RenewedAt) >= TimeSpan.FromSeconds(1))
        {
            var expires = DateTime.UtcNow.AddSeconds(5);
            await PersistLeaseAsync(active, expires, ct);
            await output.WriteAsync(new CoordinatorMessage
            {
                SessionId = session,
                CommandId = Guid.NewGuid().ToString(),
                RenewLease = new RenewLease
                {
                    RunId = active.RunId,
                    AssignmentId = active.AssignmentId,
                    ExpiresAt = Timestamp.FromDateTime(expires),
                    LeaseSequence = ++active.Lease
                }
            }, ct);
        }

        return active;
    }

    /// <summary>Принимает только события текущего назначения, завершая его по подтверждению агента.</summary>
    private async Task<bool> HandleAsync(ActiveAssignment active, AgentMessage message,
        IServerStreamWriter<CoordinatorMessage> output, string session, CancellationToken ct)
    {
        if (message.Ready is { } ready && active.Matches(ready.RunId, ready.AssignmentId) &&
            ready.PrepareCommandId == active.PrepareId && !active.Ready)
        {
            await StartAsync(active, output, session, ct);
        }
        else if (message.Started is { } started && active.Matches(started.RunId, started.AssignmentId) && active.Lease > 0)
        {
            await using var connection = await db.OpenConnectionAsync(ct);
            await connection.ExecuteAsync(new CommandDefinition("UPDATE runs SET state = 'running' WHERE id = @Id AND state = 'pending'",
                new { Id = active.Row.RunId }, cancellationToken: ct));
        }
        else if (message.Finished is { } finished && active.Matches(finished.RunId, finished.AssignmentId) &&
            finished.FinishedAt is not null && finished.State is AssignmentState.Completed or AssignmentState.Stopped or AssignmentState.Failed)
        {
            await FinishAsync(active, finished, ct);
            return true;
        }
        else if (message.Rejected is { } rejected && active.Matches(rejected.RunId, rejected.AssignmentId))
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Assignment command rejected"));
        }

        return false;
    }

    /// <summary>Записывает манифест до отправки Start и учитывает Stop во время подготовки.</summary>
    private async Task StartAsync(ActiveAssignment active, IServerStreamWriter<CoordinatorMessage> output, string session, CancellationToken ct)
    {
        var start = DateTime.UtcNow.AddSeconds(6);
        var snapshot = active.Snapshot;
        var manifest = new RunManifest
        {
            SchemaVersion = 1,
            RunId = active.RunId,
            Snapshot = snapshot,
            StartAt = Timestamp.FromDateTime(start),
            ResultDeadline = Timestamp.FromDateTime(start.AddMilliseconds(snapshot.DurationMs + snapshot.DrainTimeoutMs + 30000)),
            Assignments = { new ExpectedAssignment
            {
                AssignmentId = active.AssignmentId, AgentId = active.Row.AgentId, AssignedRps = snapshot.TotalRps,
                PlannedRequests = ((ulong)snapshot.TotalRps * snapshot.DurationMs + 999) / 1000
            } }
        };
        var ack = await results.PutRunManifestAsync(manifest, deadline: start.AddSeconds(-1), cancellationToken: ct);
        if (ack.RunId != active.RunId)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Wrong manifest acknowledgement"));
        }

        active.Ready = true;
        await using var connection = await db.OpenConnectionAsync(ct);
        var state = await connection.QuerySingleAsync<string>(new CommandDefinition("SELECT state FROM runs WHERE id = @Id",
            new { Id = active.Row.RunId }, cancellationToken: ct));
        if (state == "stopping")
        {
            await SendStopAsync(active, output, session, StopReason.UserRequest, ct);
            return;
        }

        var expires = start.AddSeconds(5);
        active.FinishDeadline = manifest.ResultDeadline.ToDateTime().AddSeconds(2);
        await PersistLeaseAsync(active, expires, ct);
        active.Lease = 1;
        active.RenewedAt = Stopwatch.GetTimestamp();
        await output.WriteAsync(new CoordinatorMessage
        {
            SessionId = session,
            CommandId = Guid.NewGuid().ToString(),
            Start = new StartAssignment
            {
                RunId = active.RunId,
                AssignmentId = active.AssignmentId,
                StartAt = Timestamp.FromDateTime(start),
                LeaseExpiresAt = Timestamp.FromDateTime(expires),
                LeaseSequence = active.Lease
            }
        }, ct);
    }

    /// <summary>Отправляет остановку один раз по упорядоченному потоку.</summary>
    private static async Task SendStopAsync(ActiveAssignment active, IServerStreamWriter<CoordinatorMessage> output,
        string session, StopReason reason, CancellationToken ct)
    {
        await output.WriteAsync(new CoordinatorMessage
        {
            SessionId = session,
            CommandId = Guid.NewGuid().ToString(),
            Stop = new StopAssignment { RunId = active.RunId, AssignmentId = active.AssignmentId, Reason = reason }
        }, ct);
        active.StopSent = true;
        active.FinishDeadline = DateTime.UtcNow.AddMilliseconds(active.Snapshot.DrainTimeoutMs + 32000);
    }

    /// <summary>Сохраняет верхнюю границу аренды до отправки разрешения агенту.</summary>
    private async Task PersistLeaseAsync(ActiveAssignment active, DateTime expires, CancellationToken ct)
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE assignments SET safe_after = GREATEST(safe_after, @SafeAfter) WHERE id = @Id
            """, new { Id = active.Row.AssignmentId, SafeAfter = expires.AddMilliseconds(active.Snapshot.DrainTimeoutMs + 100) },
            cancellationToken: ct));
        active.RenewedAt = Stopwatch.GetTimestamp();
    }

    /// <summary>Фиксирует терминальное состояние и освобождает агент только после завершения генерации.</summary>
    private async Task FinishAsync(ActiveAssignment active, AssignmentFinished finished, CancellationToken ct)
    {
        var (state, terminal) = finished.State switch
        {
            AssignmentState.Completed => ("completed", RunState.Completed),
            AssignmentState.Stopped => ("cancelled", RunState.Stopped),
            _ => ("failed", RunState.Failed)
        };
        await using var connection = await db.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE runs SET state = @State WHERE id = @Run;
            UPDATE assignments SET state = @State, safe_after = NULL WHERE id = @Assignment;
            UPDATE agents SET reserved_run_id = NULL WHERE reserved_run_id = @Run;
            """, new { State = state, Run = active.Row.RunId, Assignment = active.Row.AssignmentId }, cancellationToken: ct));
        try
        {
            await results.FinalizeRunAsync(new RunFinalization
            {
                RunId = active.RunId,
                State = terminal,
                FailureReason = finished.FailureReason,
                GenerationStoppedAt = finished.GenerationStoppedAt,
                FinishedAt = finished.FinishedAt
            }, deadline: DateTime.UtcNow.AddSeconds(2), cancellationToken: ct);
        }
        catch (RpcException exception)
        {
            logger.LogWarning("Finalization not acknowledged: {RunId} {AssignmentId} {AgentId} {Status}",
                active.RunId, active.AssignmentId, active.Row.AgentId, exception.StatusCode);
        }
    }

    /// <summary>Хранит состояние одного назначения в пределах текущего потока.</summary>
    private sealed class ActiveAssignment(AssignmentRow row, ScenarioSnapshot snapshot)
    {
        public AssignmentRow Row { get; } = row;
        public ScenarioSnapshot Snapshot { get; } = snapshot;
        public string RunId { get; } = row.RunId.ToString();
        public string AssignmentId { get; } = row.AssignmentId.ToString();
        public string PrepareId { get; } = Guid.NewGuid().ToString();
        public long PreparedAt { get; } = Stopwatch.GetTimestamp();
        public long RenewedAt { get; set; }
        public ulong Lease { get; set; }
        public bool Ready { get; set; }
        public bool StopSent { get; set; }
        public DateTime? FinishDeadline { get; set; }

        /// <summary>Проверяет принадлежность входящего события текущему назначению.</summary>
        public bool Matches(string run, string assignment) => run == RunId && assignment == AssignmentId;
    }

    /// <summary>Читает исходный сохранённый снимок без обращения к изменяемому сценарию.</summary>
    private sealed record AssignmentRow(Guid AssignmentId, Guid RunId, Guid ScenarioId, string Snapshot, string AgentId)
    {
        /// <summary>Переводит параметры REST в согласованные единицы протокола.</summary>
        public ScenarioSnapshot ToContract()
        {
            var scenario = JsonSerializer.Deserialize<ScenarioRequest>(Snapshot,
                new JsonSerializerOptions(JsonSerializerDefaults.Web))!;
            var request = new HttpRequestSpec { Method = scenario.Method, Url = scenario.Url };
            if (scenario.Body is string body)
            {
                request.Body = ByteString.CopyFromUtf8(body);
            }

            foreach (var header in scenario.Headers!.OrderBy(header => header.Key, StringComparer.Ordinal))
            {
                request.Headers.Add(new HttpHeader { Name = header.Key, Values = { header.Value } });
            }

            return new ScenarioSnapshot
            {
                ScenarioId = ScenarioId.ToString(),
                Revision = 1,
                Request = request,
                TotalRps = (uint)scenario.TargetRps,
                DurationMs = (uint)scenario.DurationSeconds * 1000,
                RequestTimeoutMs = (uint)scenario.RequestTimeoutMs,
                MaxConcurrencyPerAgent = (uint)scenario.ConcurrencyLimit,
                MaxResponseBodyBytes = 1048576,
                DrainTimeoutMs = 2000
            };
        }
    }
}
