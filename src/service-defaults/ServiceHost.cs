using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Confluent.Kafka;
using Dapper;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace LatencyStorm.ServiceDefaults;

public static class ServiceHost
{
    public static async Task RunAsync(string[] args, string serviceName, Action<WebApplication>? configure = null)
    {
        // Container health checks use the same runtime and need no curl installation.
        if (args is ["--healthcheck"])
        {
            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
            using var response = await client.GetAsync("http://127.0.0.1:8080/ready");
            Environment.ExitCode = response.IsSuccessStatusCode ? 0 : 1;
            return;
        }

        var builder = WebApplication.CreateBuilder(args);
        builder.Logging.ClearProviders();
        builder.Logging.AddJsonConsole();
        builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(10));
        var database = builder.Configuration["DATABASE_URL"];
        var brokers = builder.Configuration["KAFKA_BROKERS"];
        if (string.IsNullOrWhiteSpace(database) || string.IsNullOrWhiteSpace(brokers))
        {
            throw new InvalidOperationException("DATABASE_URL and KAFKA_BROKERS are required.");
        }

        builder.Services.AddSingleton(NpgsqlDataSource.Create(database));
        builder.Services.AddSingleton(new DependencyState(serviceName));
        builder.Services.AddHostedService<DependencyMonitor>();
        var app = builder.Build();
        app.MapGet("/health", () => Results.Ok(new { status = "alive", service = serviceName }));
        app.MapGet("/ready", (DependencyState state) => state.Ready
            ? Results.Ok(new { status = "ready", service = serviceName })
            : Results.Json(new { status = "not_ready", service = serviceName }, statusCode: 503));
        app.MapGet("/metrics", (DependencyState state) => Results.Text(
            "# HELP latencystorm_service_ready Dependencies and migrations ready.\n" +
            "# TYPE latencystorm_service_ready gauge\n" +
            $"latencystorm_service_ready{{service=\"{serviceName}\"}} {(state.Ready ? 1 : 0).ToString(CultureInfo.InvariantCulture)}\n",
            "text/plain; version=0.0.4"));
        app.MapGet("/", () => Results.Ok(new { service = serviceName, stage = "infrastructure" }));
        configure?.Invoke(app);
        await app.RunAsync();
    }
}

internal sealed class DependencyState(string serviceName)
{
    private int ready;
    public string ServiceName { get; } = serviceName;
    public bool Ready => Volatile.Read(ref ready) == 1;
    public void Set(bool value) => Volatile.Write(ref ready, value ? 1 : 0);
}

internal sealed class DependencyMonitor(
    NpgsqlDataSource database,
    DependencyState state,
    IConfiguration configuration,
    ILogger<DependencyMonitor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var kafka = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = configuration["KAFKA_BROKERS"],
            ClientId = state.ServiceName,
            SocketTimeoutMs = 2000,
            LogConnectionClose = false
        }).SetLogHandler((_, _) => { }) // Do not print connection strings or native plaintext logs.
          .SetErrorHandler((_, _) => { }).Build();
        bool? previous = null;
        while (!stoppingToken.IsCancellationRequested)
        {
            var ready = false;
            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                timeout.CancelAfter(TimeSpan.FromSeconds(5));
                await using var connection = await database.OpenConnectionAsync(timeout.Token);
                await MigrationRunner.ApplyAsync(connection, timeout.Token);
                await connection.ExecuteScalarAsync<int>(new CommandDefinition(
                    "SELECT 1", cancellationToken: timeout.Token));
                // librdkafka metadata has a bounded native timeout; await it before disposing the client.
                var metadata = await Task.Run(() => kafka.GetMetadata(TimeSpan.FromSeconds(2)), stoppingToken);
                ready = metadata.Brokers.Count > 0;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception) when (exception is NpgsqlException or KafkaException or IOException or TimeoutException or OperationCanceledException)
            {
                logger.LogWarning("Dependency probe failed: {ErrorType}", exception.GetType().Name);
            }
            state.Set(ready);
            if (previous != ready)
            {
                logger.LogInformation("Readiness changed: {Ready}", ready);
                previous = ready;
            }
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        state.Set(false);
    }
}

internal static class MigrationRunner
{
    public static async Task ApplyAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var directory = Path.Combine(AppContext.BaseDirectory, "migrations");
        if (!Directory.Exists(directory))
        {
            throw new InvalidOperationException("Migration directory is missing.");
        }
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        // Transaction-scoped lock serializes concurrent starts of this service in its own database.
        await connection.ExecuteAsync(new CommandDefinition("""
            SELECT pg_advisory_xact_lock(71294021);
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()
            );
            """, transaction: transaction, cancellationToken: cancellationToken));
        foreach (var path in Directory.GetFiles(directory, "*.sql").Order(StringComparer.Ordinal))
        {
            var name = Path.GetFileName(path);
            var sql = await File.ReadAllTextAsync(path, cancellationToken);
            var checksum = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql.Replace("\r\n", "\n", StringComparison.Ordinal))));
            var applied = await connection.QuerySingleOrDefaultAsync<string>(new CommandDefinition(
                "SELECT checksum FROM schema_migrations WHERE name = @Name",
                new { Name = name }, transaction, cancellationToken: cancellationToken));
            if (applied is string stored)
            {
                if (stored != checksum)
                {
                    throw new InvalidOperationException($"Applied migration changed: {name}");
                }
                continue;
            }
            await connection.ExecuteAsync(new CommandDefinition(
                sql, transaction: transaction, cancellationToken: cancellationToken));
            await connection.ExecuteAsync(new CommandDefinition(
                "INSERT INTO schema_migrations(name, checksum) VALUES (@Name, @Checksum)",
                new { Name = name, Checksum = checksum }, transaction, cancellationToken: cancellationToken));
        }
        await transaction.CommitAsync(cancellationToken);
    }
}
