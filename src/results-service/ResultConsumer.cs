using Confluent.Kafka;
using Npgsql;

/// <summary>Читает пакеты по порядку и подтверждает offset только после записи результата или карантина.</summary>
sealed class ResultConsumer(ReportStore store, IConfiguration configuration, ILogger<ResultConsumer> logger) : BackgroundService
{
    /// <summary>После временного сбоя возобновляет чтение с подтверждённого offset.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
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
                    await store.SaveAsync(record, stoppingToken);
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
}
