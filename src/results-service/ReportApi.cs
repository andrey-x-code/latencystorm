using System.Globalization;
using System.Text.Json.Serialization;
using LatencyStorm.Contracts.V1;

/// <summary>Возвращает измеренные показатели независимо от состояния запуска.</summary>
static class ReportApi
{
    /// <summary>Возвращает план, полноту, сводные наблюдения и перцентили накопленных гистограмм.</summary>
    public static async Task<IResult> ReportAsync(Guid id, ReportStore store, CancellationToken ct)
    {
        var report = await store.ReadAsync(id, 0, 0, ct);
        if (report is null)
        {
            return Results.Problem(statusCode: 404, title: "Run not found");
        }

        var manifest = report.Manifest;
        var totals = report.Progress.Totals;
        var observed = report.Progress.Assignments.Count != 0;
        var summaries = report.Progress.Assignments.Values.Select(item => item.Summary).OfType<AssignmentSummary>().ToArray();
        var allFinal = manifest is not null && summaries.Length == manifest.Assignments.Count;
        var generationSeconds = allFinal ? summaries[0].GenerationDurationUs / 1_000_000m : 0;
        var completionSeconds = allFinal
            ? (decimal)(summaries[0].FinishedAt.ToDateTime() - manifest!.StartAt.ToDateTime()).TotalSeconds : 0;
        var complete = report.State == "Complete";
        return Results.Ok(new ReportResponse(id, report.State, report.Reasons, report.Finalization?.State.ToString(),
            manifest?.Snapshot.TotalRps, manifest?.Assignments[0].PlannedRequests,
            observed ? CounterResponse.From(totals.Counters) : null, observed ? totals.HttpStatusCounts : null,
            complete ? Rate(totals.Counters.Started, generationSeconds) : null,
            complete && generationSeconds > 0 ? Rate(totals.Counters.Finished, completionSeconds) : null,
            Measurements.PercentilesOf(totals.HttpLatency), Measurements.PercentilesOf(totals.FailureLatency)));
    }

    /// <summary>Возвращает страницу полученных интервалов после указанного sequence.</summary>
    public static async Task<IResult> TimelineAsync(Guid id, string? after, int? limit, ReportStore store, CancellationToken ct)
    {
        var size = limit ?? 100;
        if (size is < 1 or > 1000 || !ulong.TryParse(after ?? "0", NumberStyles.None, CultureInfo.InvariantCulture, out var cursor))
        {
            return Results.Problem(statusCode: 400, title: "Invalid timeline cursor or limit");
        }

        var report = await store.ReadAsync(id, cursor, size + 1, ct);
        if (report is null)
        {
            return Results.Problem(statusCode: 404, title: "Run not found");
        }

        var points = report.Batches.Take(size).Select(batch =>
        {
            var seconds = (decimal)(batch.Interval.EndAt - batch.Interval.StartAt).ToTimeSpan().Ticks / TimeSpan.TicksPerSecond;
            return new TimelinePoint(batch.Sequence, batch.Interval.StartAt.ToDateTimeOffset(), batch.Interval.EndAt.ToDateTimeOffset(),
                batch.IsFinal, CounterResponse.From(batch.Counters), Rate(batch.Counters.Started, seconds),
                Rate(batch.Counters.Finished, seconds));
        }).ToArray();
        return Results.Ok(new TimelineResponse(id, report.State, report.Reasons, points,
            report.Batches.Length > size ? points[^1].Sequence : null));
    }

    /// <summary>Не вычисляет RPS при нулевой или отсутствующей длительности.</summary>
    private static decimal? Rate(ulong count, decimal seconds) => seconds > 0 ? count / seconds : null;
}

/// <summary>Сводка принятых измерений, плана и состояния их доставки.</summary>
sealed record ReportResponse(Guid RunId, string ReportState, string[] Reasons, string? RunState, uint? RequestedRps,
    [property: JsonNumberHandling(JsonNumberHandling.WriteAsString)] ulong? PlannedRequests,
    CounterResponse? Counters,
    [property: JsonNumberHandling(JsonNumberHandling.WriteAsString)] SortedDictionary<uint, ulong>? HttpStatusCounts,
    decimal? ObservedStartRps, decimal? CompletionRps, Percentiles HttpLatency, Percentiles FailureLatency);

/// <summary>Счётчики наблюдений с точным представлением uint64 в JSON.</summary>
[JsonNumberHandling(JsonNumberHandling.WriteAsString)]
sealed record CounterResponse(ulong Scheduled, ulong Started, ulong Skipped, ulong Finished, ulong HttpResponses,
    ulong HttpErrors, ulong TransportErrors, ulong Timeouts, ulong Cancelled, ulong ResponseBodyLimitExceeded)
{
    /// <summary>Отделяет представление REST от сгенерированных типов контракта.</summary>
    public static CounterResponse From(RequestCounters counters) => new(counters.Scheduled, counters.Started, counters.Skipped,
        counters.Finished, counters.HttpResponses, counters.HttpErrors, counters.TransportErrors, counters.Timeouts,
        counters.Cancelled, counters.ResponseBodyLimitExceeded);
}

/// <summary>Страница временного ряда без заполнения отсутствующих интервалов.</summary>
sealed record TimelineResponse(Guid RunId, string ReportState, string[] Reasons, TimelinePoint[] Points,
    [property: JsonNumberHandling(JsonNumberHandling.WriteAsString)] ulong? NextAfter);

/// <summary>Наблюдения за один принятый интервал и его фактические RPS.</summary>
sealed record TimelinePoint([property: JsonNumberHandling(JsonNumberHandling.WriteAsString)] ulong Sequence,
    DateTimeOffset StartAt, DateTimeOffset EndAt, bool IsFinal, CounterResponse Counters, decimal? ObservedStartRps, decimal? CompletionRps);
