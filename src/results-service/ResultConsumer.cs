using Confluent.Kafka;
using Dapper;
using Google.Protobuf;
using LatencyStorm.Contracts.V1;
using Npgsql;

/// <summary>Сохраняет пакеты Kafka; подтверждает offset после записи.</summary>
sealed class ResultConsumer(NpgsqlDataSource db, IConfiguration configuration, ILogger<ResultConsumer> logger) : BackgroundService
{
    /// <summary>Последовательно читает пакеты, повторяя чтение после временных ошибок.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
            {
                BootstrapServers = configuration["KAFKA_BROKERS"],
                GroupId = "latencystorm-results-v1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SocketTimeoutMs = 5000
            }).Build();
            try
            {
                consumer.Subscribe("latencystorm.results.v1");
                while (!stoppingToken.IsCancellationRequested)
                {
                    var record = consumer.Consume(stoppingToken);
                    await SaveAsync(record.Message.Key, record.Message.Value, stoppingToken);
                    consumer.Commit(record);
                }
            }
            catch (Exception exception) when (exception is KafkaException or NpgsqlException or TimeoutException)
            {
                logger.LogWarning("Ingestion will retry: {ErrorType}", exception.GetType().Name);
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
            finally
            {
                consumer.Close();
            }
        }
    }

    /// <summary>Сохраняет только пакет ожидаемого назначения, не перезаписывая sequence.</summary>
    private async Task SaveAsync(string? key, string? payload, CancellationToken ct)
    {
        if (payload is null || System.Text.Encoding.UTF8.GetByteCount(payload) > 256 * 1024)
        {
            logger.LogWarning("Missing or oversized result batch");
            return;
        }

        ResultBatch batch;
        try
        {
            batch = JsonParser.Default.Parse<ResultBatch>(payload);
        }
        catch (Exception exception) when (exception is InvalidJsonException or InvalidProtocolBufferException)
        {
            logger.LogWarning("Malformed result batch");
            return;
        }

        if (batch.SchemaVersion != 1 || batch.Sequence == 0 || key != batch.AssignmentId ||
            !Guid.TryParseExact(batch.RunId, "D", out var run) ||
            !Guid.TryParseExact(batch.AssignmentId, "D", out var assignment) ||
            batch.Interval?.StartAt is null || batch.Interval.EndAt is null || batch.Counters is null ||
            batch.HttpLatency is null || batch.FailureLatency is null || batch.IsFinal != (batch.Summary is not null))
        {
            logger.LogWarning("Invalid result batch envelope");
            return;
        }

        await using var connection = await db.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO result_batches (run_id, assignment_id, sequence, payload)
            SELECT run_id, @Assignment, @Sequence, CAST(@Payload AS jsonb)
            FROM run_manifests WHERE run_id = @Run
              AND payload #>> '{assignments,0,assignmentId}' = @AssignmentId
              AND payload #>> '{assignments,0,agentId}' = @AgentId
            ON CONFLICT (run_id, assignment_id, sequence) DO NOTHING
            """, new
        {
            Run = run,
            Assignment = assignment,
            Sequence = (decimal)batch.Sequence,
            Payload = payload,
            batch.AssignmentId,
            batch.AgentId
        }, cancellationToken: ct));
    }
}
