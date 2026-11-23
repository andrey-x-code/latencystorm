using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Dapper;
using Google.Protobuf;
using LatencyStorm.Contracts.V1;
using Npgsql;

/// <summary>Хранит пакеты, агрегаты и диагностику в базе Results Service.</summary>
sealed class ReportStore(NpgsqlDataSource db, ILogger<ReportStore> logger)
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>Сохраняет пакет и его вклад атомарно; точный повтор не меняет агрегаты.</summary>
    public async Task SaveAsync(ConsumeResult<byte[], byte[]> record, CancellationToken ct)
    {
        var (batch, id, reason) = Decode(record.Message.Key, record.Message.Value);
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        CommandDefinition Command(string sql, object? args = null) => new(sql, args, transaction, cancellationToken: ct);

        var row = id is Guid run ? await LockAsync(connection, transaction, run, ct) : null;
        if (row is not null && id is Guid runId)
        {
            var manifest = JsonParser.Default.Parse<RunManifest>(row.Payload);
            var progress = await LoadAsync(connection, transaction, runId, row, manifest, ct);
            if (batch is not null)
            {
                var identity = new { Run = runId, Assignment = Guid.Parse(batch.AssignmentId), Sequence = (decimal)batch.Sequence };
                var previous = await connection.QuerySingleOrDefaultAsync<string>(Command("""
                    SELECT payload::text FROM result_batches
                    WHERE run_id = @Run AND assignment_id = @Assignment AND sequence = @Sequence
                    """, identity));
                if (previous is not null)
                {
                    reason = Equivalent(previous, batch) ? null : "ConflictingDuplicate";
                }
                else if (manifest.Assignments.SingleOrDefault(item => item.AssignmentId == batch.AssignmentId &&
                    item.AgentId == batch.AgentId) is not { } expected)
                {
                    reason = "UnexpectedAssignment";
                }
                else if (row.Now >= manifest.ResultDeadline.ToDateTime())
                {
                    reason = "LateBatch";
                }
                else
                {
                    var neighbors = await connection.QueryAsync<string>(Command("""
                        SELECT payload::text FROM result_batches WHERE run_id = @Run AND assignment_id = @Assignment
                            AND sequence IN (@Sequence - 1, @Sequence + 1)
                        """, identity));
                    try
                    {
                        CheckNeighbors(batch, neighbors.Select(JsonParser.Default.Parse<ResultBatch>));
                        progress.Add(manifest, batch, expected);
                        await connection.ExecuteAsync(Command("""
                            INSERT INTO result_batches (run_id, assignment_id, sequence, payload, received_at)
                            VALUES (@Run, @Assignment, @Sequence, CAST(@Payload AS jsonb), @Received)
                            """, new
                        {
                            identity.Run,
                            identity.Assignment,
                            identity.Sequence,
                            Payload = JsonFormatter.Default.Format(batch),
                            Received = row.Now
                        }));
                    }
                    catch (Exception exception) when (exception is OverflowException or InvalidDataException)
                    {
                        reason = "InvalidMeasurements";
                    }
                }
            }

            progress.InvalidBatch |= reason is not (null or "LateBatch");
            await PersistAsync(connection, transaction, runId, progress, ct);
        }
        else if (batch is not null)
        {
            reason = "MissingManifest";
        }

        if (reason is not null)
        {
            await connection.ExecuteAsync(Command("""
                INSERT INTO rejected_results (partition, kafka_offset, run_id, reason)
                VALUES (@Partition, @Offset, @Run, @Reason) ON CONFLICT DO NOTHING
                """, new { Partition = record.Partition.Value, Offset = record.Offset.Value, Run = id, Reason = reason }));
        }

        await transaction.CommitAsync(ct);
        if (reason is not null)
        {
            logger.LogWarning("Result quarantined: {RunId} {Partition} {Offset} {Reason}", id, record.Partition.Value, record.Offset.Value, reason);
        }
    }

    /// <summary>Читает состояние и страницу дельт из одного согласованного снимка.</summary>
    public async Task<ReportSnapshot?> ReadAsync(Guid id, decimal after, int limit, CancellationToken ct)
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var row = await LockAsync(connection, transaction, id, ct);
        if (row is null)
        {
            var first = await connection.QuerySingleAsync<DateTime?>(new CommandDefinition("""
                SELECT min(received_at) FROM rejected_results WHERE run_id = @Id
                """, new { Id = id }, transaction, cancellationToken: ct));
            return first is null ? null : ReportSnapshot.Orphan(first.Value);
        }

        var manifest = JsonParser.Default.Parse<RunManifest>(row.Payload);
        var progress = await LoadAsync(connection, transaction, id, row, manifest, ct);
        if (row.ReportState is null)
        {
            await PersistAsync(connection, transaction, id, progress, ct);
        }

        var batches = limit == 0 ? [] : (await connection.QueryAsync<string>(new CommandDefinition("""
            SELECT payload::text FROM result_batches WHERE run_id = @Id AND sequence > @After
            ORDER BY sequence LIMIT @Limit
            """, new { Id = id, After = after, Limit = limit }, transaction, cancellationToken: ct)))
            .Select(JsonParser.Default.Parse<ResultBatch>).ToArray();
        await transaction.CommitAsync(ct);
        var finalization = row.Finalization is null ? null : JsonParser.Default.Parse<RunFinalization>(row.Finalization);
        var (state, reasons) = progress.Status(manifest, row.Now);
        return new ReportSnapshot(manifest, finalization, progress, state, reasons, batches);
    }

    /// <summary>Разбирает UTF-8 ProtoJSON и извлекает только безопасный UUID для диагностики.</summary>
    private static (ResultBatch?, Guid?, string?) Decode(byte[]? key, byte[]? payload)
    {
        if (payload is not { Length: > 0 and <= 262144 })
        {
            return (null, null, "InvalidSize");
        }

        Guid? run = null;
        try
        {
            using var json = JsonDocument.Parse(payload);
            if (json.RootElement.ValueKind == JsonValueKind.Object &&
                (json.RootElement.TryGetProperty("runId", out var value) || json.RootElement.TryGetProperty("run_id", out value)) &&
                value.ValueKind == JsonValueKind.String && Guid.TryParseExact(value.GetString(), "D", out var parsed))
            {
                run = parsed;
            }

            var batch = JsonParser.Default.Parse<ResultBatch>(StrictUtf8.GetString(payload));
            if (batch.SchemaVersion != 1)
            {
                return (null, run, "UnsupportedVersion");
            }

            return Measurements.Validate(batch, key is { Length: 36 } ? StrictUtf8.GetString(key) : null)
                ? (batch, Guid.Parse(batch.RunId), null) : (null, run, "InvalidBatch");
        }
        catch (Exception exception) when (exception is JsonException or InvalidJsonException or InvalidProtocolBufferException or
            ArgumentException or InvalidOperationException or OverflowException)
        {
            return (null, run, "MalformedPayload");
        }
    }

    /// <summary>Сравнивает разобранные сообщения, игнорируя порядок кодов HTTP.</summary>
    private static bool Equivalent(string previous, ResultBatch batch)
    {
        var parsed = JsonParser.Default.Parse<ResultBatch>(previous);
        return Measurements.Validate(parsed, batch.AssignmentId) && parsed.Equals(batch);
    }

    /// <summary>Отклоняет разрыв или перекрытие между соседними номерами.</summary>
    private static void CheckNeighbors(ResultBatch batch, IEnumerable<ResultBatch> neighbors)
    {
        foreach (var other in neighbors)
        {
            var adjacent = other.Sequence < batch.Sequence ? other.Interval.EndAt.Equals(batch.Interval.StartAt) :
                batch.Interval.EndAt.Equals(other.Interval.StartAt);
            if (!adjacent)
            {
                throw new InvalidDataException("Noncontiguous intervals");
            }
        }
    }

    /// <summary>Сериализует изменения одного запуска с финализацией и чтением.</summary>
    private static Task<ManifestRow?> LockAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid id, CancellationToken ct) =>
        connection.QuerySingleOrDefaultAsync<ManifestRow>(new CommandDefinition("""
            SELECT payload::text AS "Payload", finalization::text AS "Finalization", report_state::text AS "ReportState",
                clock_timestamp() AS "Now" FROM run_manifests WHERE run_id = @Id FOR UPDATE
            """, new { Id = id }, transaction, cancellationToken: ct));

    /// <summary>Один раз восстанавливает агрегаты по пакетам, записанным до применения миграции.</summary>
    private static async Task<ReportProgress> LoadAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid id, ManifestRow row, RunManifest manifest, CancellationToken ct)
    {
        if (row.ReportState is not null)
        {
            return JsonSerializer.Deserialize<ReportProgress>(row.ReportState)!;
        }

        var progress = new ReportProgress();
        var payloads = await connection.QueryAsync<string>(new CommandDefinition("""
            SELECT payload::text FROM result_batches WHERE run_id = @Id ORDER BY sequence
            """, new { Id = id }, transaction, cancellationToken: ct));
        ResultBatch? previous = null;
        foreach (var payload in payloads)
        {
            var batch = JsonParser.Default.Parse<ResultBatch>(payload);
            var expected = manifest.Assignments.SingleOrDefault(item => item.AssignmentId == batch.AssignmentId && item.AgentId == batch.AgentId);
            try
            {
                if (expected is null || !Measurements.Validate(batch, batch.AssignmentId))
                {
                    throw new InvalidDataException("Invalid historical batch");
                }

                if (previous is not null && batch.Sequence - 1 == previous.Sequence)
                {
                    CheckNeighbors(batch, [previous]);
                }

                progress.Add(manifest, batch, expected);
                previous = batch;
            }
            catch (Exception exception) when (exception is OverflowException or InvalidDataException)
            {
                progress.InvalidBatch = true;
            }
        }

        progress.InvalidBatch |= await connection.ExecuteScalarAsync<bool>(new CommandDefinition("""
            SELECT EXISTS (SELECT 1 FROM rejected_results WHERE run_id = @Id AND reason <> 'LateBatch')
            """, new { Id = id }, transaction, cancellationToken: ct));
        return progress;
    }

    /// <summary>Записывает агрегаты и прогресс доставки до подтверждения транзакции.</summary>
    private static Task<int> PersistAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid id, ReportProgress progress, CancellationToken ct) => connection.ExecuteAsync(new CommandDefinition("""
            UPDATE run_manifests SET report_state = CAST(@State AS jsonb) WHERE run_id = @Id
            """, new { Id = id, State = JsonSerializer.Serialize(progress) }, transaction, cancellationToken: ct));

    /// <summary>Представляет заблокированную строку манифеста.</summary>
    private sealed record ManifestRow(string Payload, string? Finalization, string? ReportState, DateTime Now);
}

/// <summary>Согласованный снимок данных для ответа REST API.</summary>
sealed record ReportSnapshot(RunManifest? Manifest, RunFinalization? Finalization, ReportProgress Progress,
    string State, string[] Reasons, ResultBatch[] Batches)
{
    /// <summary>Не выдаёт результат без манифеста за подтверждённый запуск.</summary>
    public static ReportSnapshot Orphan(DateTime first) => DateTime.UtcNow >= first.AddMinutes(5)
        ? new(null, null, new(), "Incomplete", ["MissingManifest"], [])
        : new(null, null, new(), "Collecting", [], []);
}
