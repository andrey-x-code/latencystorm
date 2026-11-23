using LatencyStorm.Contracts.V1;

/// <summary>Сохраняет прогресс доставки и агрегаты по назначениям в одном снимке.</summary>
sealed class ReportProgress
{
    public Dictionary<string, AssignmentProgress> Assignments { get; set; } = [];
    public MeasurementTotals Totals { get; set; } = new();
    public bool InvalidBatch { get; set; }
    public bool MeasurementsLost { get; set; }

    /// <summary>Накапливает только новый пакет и проверяет инварианты законченной последовательности.</summary>
    public void Add(RunManifest manifest, ResultBatch batch, ExpectedAssignment expected)
    {
        var progress = Assignments.GetValueOrDefault(batch.AssignmentId) ?? new AssignmentProgress();
        var delta = MeasurementTotals.FromBatch(batch);
        var totals = Totals.Add(delta);
        var assignmentTotals = progress.Totals.Add(delta);
        var final = batch.IsFinal ? batch.Sequence : progress.FinalSequence;
        var highest = Math.Max(progress.HighestSequence, batch.Sequence);
        var count = checked(progress.Count + 1);
        var summary = batch.Summary ?? progress.Summary;
        if (assignmentTotals.Counters.Scheduled > expected.PlannedRequests ||
            (batch.IsFinal && progress.FinalSequence is not null) || highest > final ||
            (batch.Sequence == 1 && !batch.Interval.StartAt.Equals(manifest.StartAt) && !IsPreStartStop(batch)) ||
            (summary is not null && count == final &&
                (assignmentTotals.Counters.Started != assignmentTotals.Counters.Finished ||
                 (summary.State == AssignmentState.Completed && assignmentTotals.Counters.Scheduled != expected.PlannedRequests))))
        {
            throw new InvalidDataException("Invalid sequence or assignment totals");
        }

        progress.Totals = assignmentTotals;
        progress.HighestSequence = highest;
        progress.Count = count;
        progress.FinalSequence = final;
        progress.Summary = summary;
        Assignments[batch.AssignmentId] = progress;
        Totals = totals;
        MeasurementsLost |= batch.MeasurementsLost;
    }

    /// <summary>Распознаёт пустой финальный пакет остановленного до старта назначения.</summary>
    private static bool IsPreStartStop(ResultBatch batch) => batch.Summary is
    { State: AssignmentState.Stopped or AssignmentState.Failed, ActualStartAt: null, GenerationDurationUs: 0 } &&
        batch.Interval.StartAt.Equals(batch.Interval.EndAt) && batch.Interval.EndAt.Equals(batch.Summary.GenerationStoppedAt);

    /// <summary>Различает необратимые ошибки и ещё не доставленные пакеты.</summary>
    public (string State, string[] Reasons) Status(RunManifest manifest, DateTime now)
    {
        List<string> reasons = [];
        if (InvalidBatch)
        {
            reasons.Add("InvalidBatch");
        }

        if (MeasurementsLost)
        {
            reasons.Add("MeasurementsLost");
        }

        var missingFinal = manifest.Assignments.Any(item => Assignments.GetValueOrDefault(item.AssignmentId)?.FinalSequence is null);
        var missingBatch = Assignments.Values.Any(item => item.Count < (item.FinalSequence ?? item.HighestSequence));
        if ((missingBatch || missingFinal) && now >= manifest.ResultDeadline.ToDateTime())
        {
            if (missingBatch)
            {
                reasons.Add("MissingBatch");
            }

            if (missingFinal)
            {
                reasons.Add("MissingFinalBatch");
            }

            reasons.Add("DeliveryDeadlineExceeded");
        }

        return (reasons.Count != 0 ? "Incomplete" : missingBatch || missingFinal ? "Collecting" : "Complete", [.. reasons]);
    }
}

/// <summary>Отслеживает размер уникальной последовательности, её финал и агрегаты назначения.</summary>
sealed class AssignmentProgress
{
    public ulong Count { get; set; }
    public ulong HighestSequence { get; set; }
    public ulong? FinalSequence { get; set; }
    public AssignmentSummary? Summary { get; set; }
    public MeasurementTotals Totals { get; set; } = new();
}
