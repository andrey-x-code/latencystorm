using Google.Protobuf.WellKnownTypes;
using LatencyStorm.Contracts.V1;

/// <summary>Проверяет дельты v1 и объединяет только совместимые измерения.</summary>
static class Measurements
{
    private static readonly int[] Bounds = [100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000,
        250000, 500000, 1000000, 2500000, 5000000, 10000000, 30000000, 60000000, 120000000, 300000000];

    /// <summary>Проверяет обязательные поля и инварианты пакета; нормализует порядок кодов HTTP.</summary>
    public static bool Validate(ResultBatch batch, string? key)
    {
        if (batch.SchemaVersion != 1 || batch.Sequence == 0 || key != batch.AssignmentId ||
            !IsId(batch.RunId) || !IsId(batch.AssignmentId) || string.IsNullOrEmpty(batch.AgentId) ||
            batch.Interval?.StartAt is null || batch.Interval.EndAt is null || batch.Counters is not { } counters ||
            batch.IsFinal != (batch.Summary is not null))
        {
            return false;
        }

        var interval = batch.Interval;
        if (interval.StartAt.CompareTo(interval.EndAt) > 0 ||
            (!batch.IsFinal && !interval.EndAt.Equals(interval.StartAt + Duration.FromTimeSpan(TimeSpan.FromSeconds(1)))) ||
            (interval.StartAt.Equals(interval.EndAt) && !counters.Equals(new RequestCounters())))
        {
            return false;
        }

        if (batch.Summary is { } summary && (summary.State is not (AssignmentState.Completed or AssignmentState.Stopped or AssignmentState.Failed) ||
            !System.Enum.IsDefined(summary.FailureReason) || (summary.State == AssignmentState.Failed) != (summary.FailureReason != FailureReason.Unspecified) ||
            summary.GenerationStoppedAt is not { } stopped || summary.FinishedAt is not { } finished || !finished.Equals(interval.EndAt) ||
            stopped.CompareTo(finished) > 0 || summary.ActualStartAt?.CompareTo(stopped) > 0))
        {
            return false;
        }

        var statuses = batch.HttpStatusCounts.OrderBy(item => item.StatusCode).ToArray();
        if (statuses.Any(item => item.StatusCode is < 100 or > 599 || item.Count == 0) ||
            statuses.Select(item => item.StatusCode).Distinct().Count() != statuses.Length ||
            (decimal)counters.Started + counters.Skipped != counters.Scheduled ||
            (decimal)counters.HttpResponses + counters.TransportErrors + counters.Timeouts + counters.Cancelled +
                counters.ResponseBodyLimitExceeded != counters.Finished ||
            statuses.Sum(item => (decimal)item.Count) != counters.HttpResponses ||
            statuses.Where(item => item.StatusCode >= 400).Sum(item => (decimal)item.Count) != counters.HttpErrors ||
            !HistogramMatches(batch.HttpLatency, counters.HttpResponses) ||
            !HistogramMatches(batch.FailureLatency, counters.Finished - counters.HttpResponses))
        {
            return false;
        }

        batch.HttpStatusCounts.Clear();
        batch.HttpStatusCounts.AddRange(statuses);
        return true;
    }

    /// <summary>Проверяет канонический UUID протокола.</summary>
    public static bool IsId(string value) => Guid.TryParseExact(value, "D", out var id) && id != Guid.Empty && id.ToString() == value;

    /// <summary>Проверяет layout и количество наблюдений гистограммы.</summary>
    private static bool HistogramMatches(LatencyHistogram? histogram, ulong samples) =>
        histogram is { LayoutId: "latency-us-v1", BucketCounts.Count: 20 } &&
        histogram.BucketCounts.Sum(count => (decimal)count) + histogram.OverflowCount == samples;

    /// <summary>Возвращает верхние границы nearest rank, не подставляя ноль вместо отсутствующего значения.</summary>
    public static Percentiles PercentilesOf(ulong[] buckets) => new(AtRank(buckets, 50), AtRank(buckets, 95), AtRank(buckets, 99));

    /// <summary>Находит первый бакет с достаточной накопленной суммой; последний индекс означает overflow.</summary>
    private static Percentile? AtRank(ulong[] buckets, int percent)
    {
        var total = buckets.Sum(count => (decimal)count);
        if (total == 0)
        {
            return null;
        }

        var rank = decimal.Ceiling(total * percent / 100);
        for (var index = 0; index < Bounds.Length; index++)
        {
            rank -= buckets[index];
            if (rank <= 0)
            {
                return new Percentile(Bounds[index], true, false, null);
            }
        }

        return new Percentile(null, true, true, Bounds[^1]);
    }
}

/// <summary>Верхняя оценка перцентиля либо нижняя граница при переполнении гистограммы.</summary>
sealed record Percentile(int? ValueUs, bool Approximate, bool Overflow, int? LowerBoundUs);

/// <summary>Перцентили одной области измерений.</summary>
sealed record Percentiles(Percentile? P50, Percentile? P95, Percentile? P99);

/// <summary>Суммы счётчиков, HTTP-кодов и бакетов; последний бакет содержит overflow.</summary>
sealed class MeasurementTotals
{
    public RequestCounters Counters { get; set; } = new();
    public SortedDictionary<uint, ulong> HttpStatusCounts { get; set; } = [];
    public ulong[] HttpLatency { get; set; } = new ulong[21];
    public ulong[] FailureLatency { get; set; } = new ulong[21];

    /// <summary>Создаёт новый агрегат, оставляя исходный неизменным при переполнении.</summary>
    public MeasurementTotals Add(MeasurementTotals other)
    {
        var left = Counters;
        var right = other.Counters;
        var statuses = new SortedDictionary<uint, ulong>(HttpStatusCounts);
        foreach (var (code, count) in other.HttpStatusCounts)
        {
            statuses[code] = checked(statuses.GetValueOrDefault(code) + count);
        }

        return new MeasurementTotals
        {
            Counters = new RequestCounters
            {
                Scheduled = checked(left.Scheduled + right.Scheduled),
                Started = checked(left.Started + right.Started),
                Skipped = checked(left.Skipped + right.Skipped),
                Finished = checked(left.Finished + right.Finished),
                HttpResponses = checked(left.HttpResponses + right.HttpResponses),
                HttpErrors = checked(left.HttpErrors + right.HttpErrors),
                TransportErrors = checked(left.TransportErrors + right.TransportErrors),
                Timeouts = checked(left.Timeouts + right.Timeouts),
                Cancelled = checked(left.Cancelled + right.Cancelled),
                ResponseBodyLimitExceeded = checked(left.ResponseBodyLimitExceeded + right.ResponseBodyLimitExceeded)
            },
            HttpStatusCounts = statuses,
            HttpLatency = HttpLatency.Zip(other.HttpLatency, (first, second) => checked(first + second)).ToArray(),
            FailureLatency = FailureLatency.Zip(other.FailureLatency, (first, second) => checked(first + second)).ToArray()
        };
    }

    /// <summary>Создаёт агрегат одной дельты после её валидации.</summary>
    public static MeasurementTotals FromBatch(ResultBatch batch) => new()
    {
        Counters = batch.Counters,
        HttpStatusCounts = new(batch.HttpStatusCounts.ToDictionary(item => item.StatusCode, item => item.Count)),
        HttpLatency = [.. batch.HttpLatency.BucketCounts, batch.HttpLatency.OverflowCount],
        FailureLatency = [.. batch.FailureLatency.BucketCounts, batch.FailureLatency.OverflowCount]
    };
}
