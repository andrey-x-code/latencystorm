// Explicit aliases keep database naming independent of Dapper global settings.
// JSONB stays a string here to preserve the existing HTTP response format.
/// <summary>
/// Результат чтения сценария из БД; JSON заголовков передаётся строкой в существующем формате API.
/// </summary>
sealed class ScenarioRow
{
    public const string SelectSql = """
        SELECT id, name, method, url, headers, body, target_rps AS "TargetRps",
            duration_seconds AS "DurationSeconds", request_timeout_ms AS "RequestTimeoutMs",
            concurrency_limit AS "ConcurrencyLimit", created_at AS "CreatedAt"
        FROM scenarios
        """;

    public Guid Id { get; set; }
    public string Name { get; set; } = null!;
    public string Method { get; set; } = null!;
    public string Url { get; set; } = null!;
    public string Headers { get; set; } = null!;
    public string? Body { get; set; }
    public int TargetRps { get; set; }
    public int DurationSeconds { get; set; }
    public int RequestTimeoutMs { get; set; }
    public int ConcurrencyLimit { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Краткие сведения о запуске; временные метки хранятся в UTC.
/// </summary>
class RunRow
{
    public const string SelectSql = """
        SELECT id, scenario_id AS "ScenarioId", state, created_at AS "CreatedAt", stopped_at AS "StoppedAt"
        FROM runs
        """;

    public Guid Id { get; set; }
    public Guid ScenarioId { get; set; }
    public string State { get; set; } = null!;
    public DateTime CreatedAt { get; set; }
    public DateTime? StoppedAt { get; set; }
}

/// <summary>
/// Подробные сведения о запуске с неизменяемым JSON-снимком сценария.
/// </summary>
sealed class RunDetailsRow : RunRow
{
    public string Snapshot { get; set; } = null!;
}
