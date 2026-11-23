/// <summary>
/// Параметры создания запуска на основе сохранённого сценария.
/// </summary>
record RunRequest(Guid ScenarioId);

/// <summary>
/// Параметры HTTP-сценария: RPS, длительность в секундах, таймаут в миллисекундах и предел одновременных запросов.
/// </summary>
record ScenarioRequest(
    string Name,
    string Method,
    string Url,
    Dictionary<string, string>? Headers,
    string? Body,
    int TargetRps,
    int DurationSeconds,
    int RequestTimeoutMs,
    int ConcurrencyLimit)
{
    /// <summary>
    /// Возвращает описание первой ошибки параметров или null, если параметры допустимы.
    /// </summary>
    public string? Validate()
    {
        if (string.IsNullOrWhiteSpace(Name) || Name.Length > 200)
        {
            return "name is required and must be <= 200 characters";
        }

        if (Headers is null)
        {
            return "headers is required";
        }

        if (Headers.Count > 100)
        {
            return "headers must contain at most 100 entries";
        }

        if (Headers.Any(header => string.IsNullOrEmpty(header.Key) || header.Key.Length > 256 ||
            header.Value is null || header.Value.Length > 8192 || header.Value.Contains('\r') || header.Value.Contains('\n')))
        {
            return "header names must be <= 256 and values <= 8192 characters";
        }

        if (Body is not null && System.Text.Encoding.UTF8.GetByteCount(Body) > 1_048_576)
        {
            return "body must be <= 1048576 bytes";
        }

        if (!Uri.TryCreate(Url, UriKind.Absolute, out var uri) ||
            uri.Scheme is not ("http" or "https") ||
            string.IsNullOrWhiteSpace(uri.Host) || uri.UserInfo.Length != 0 || uri.Fragment.Length != 0 ||
            System.Text.Encoding.UTF8.GetByteCount(Url) > 8192)
        {
            return "url must be an absolute HTTP or HTTPS URL";
        }

        if (Method is not ("GET" or "POST" or "PUT" or "PATCH" or "DELETE" or "HEAD"))
        {
            return "unsupported method";
        }

        if (TargetRps is < 1 or > 1_000_000)
        {
            return "targetRps must be 1..1000000";
        }

        if (DurationSeconds is < 1 or > 86400)
        {
            return "durationSeconds must be 1..86400";
        }

        if (RequestTimeoutMs is < 1 or > 300000)
        {
            return "requestTimeoutMs must be 1..300000";
        }

        if (ConcurrencyLimit is < 1 or > 100000)
        {
            return "concurrencyLimit must be 1..100000";
        }

        return null;
    }
}
