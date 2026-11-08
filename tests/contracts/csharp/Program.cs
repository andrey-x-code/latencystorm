using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using LatencyStorm.Contracts.V1;

if (args.Length != 2)
{
    throw new ArgumentException("Usage: ContractCheck <input.json> <output.json>");
}

var batch = JsonParser.Default.Parse<ResultBatch>(File.ReadAllText(args[0]));
Require(batch.SchemaVersion == 1 && batch.Sequence == 1 && batch.IsFinal, "Batch envelope");
Require(batch.RunId == "11111111-1111-4111-8111-111111111111", "Run ID");
Require(batch.AssignmentId == "22222222-2222-4222-8222-222222222222", "Assignment ID");
Require(batch.AgentId == "agent-01" && !batch.MeasurementsLost, "Agent and loss marker");
Require(batch.Interval.StartAt == Timestamp.FromDateTime(DateTime.Parse("2026-11-08T00:00:00Z").ToUniversalTime()), "Interval start");
Require(batch.Interval.EndAt.Seconds - batch.Interval.StartAt.Seconds == 1, "Interval duration");
var counters = batch.Counters;
Require(counters.Scheduled == 5 && counters.Started == 4 && counters.Skipped == 1 && counters.Finished == 4, "Request counts");
Require(counters.HttpResponses == 2 && counters.HttpErrors == 1 && counters.TransportErrors == 1 && counters.Timeouts == 1, "Outcomes");
Require(counters.Cancelled == 0 && counters.ResponseBodyLimitExceeded == 0, "Remaining outcomes");
Require(batch.HttpStatusCounts.Count == 2 && batch.HttpStatusCounts[0].StatusCode == 200 && batch.HttpStatusCounts[0].Count == 1 && batch.HttpStatusCounts[1].StatusCode == 500 && batch.HttpStatusCounts[1].Count == 1, "HTTP status distribution");
Require(batch.HttpLatency.LayoutId == "latency-us-v1" && batch.HttpLatency.BucketCounts.Count == 20, "Histogram layout");
Require(batch.HttpLatency.BucketCounts[2] == 1 && batch.HttpLatency.BucketCounts[3] == 1 && batch.HttpLatency.OverflowCount == 0, "HTTP histogram");
Require(batch.FailureLatency.LayoutId == "latency-us-v1" && batch.FailureLatency.BucketCounts.Count == 20 && batch.FailureLatency.BucketCounts[11] == 2 && batch.FailureLatency.OverflowCount == 0, "Failure histogram");
Require(batch.Summary.State == AssignmentState.Completed && batch.Summary.FailureReason == FailureReason.Unspecified, "Assignment outcome");
Require(batch.Summary.ActualStartAt == batch.Interval.StartAt && batch.Summary.GenerationStoppedAt == batch.Interval.EndAt && batch.Summary.FinishedAt == batch.Interval.EndAt && batch.Summary.GenerationDurationUs == 1_000_000, "Final summary");
Require(batch.Equals(ResultBatch.Parser.ParseFrom(batch.ToByteArray())), "Protobuf binary round trip");
File.WriteAllText(args[1], JsonFormatter.Default.Format(batch));

// ProtoJSON must preserve integers larger than JavaScript's exact integer range.
var large = new ResultBatch { Sequence = 9_007_199_254_740_993UL };
var largeJson = JsonFormatter.Default.Format(large);
Require(largeJson.Contains("\"9007199254740993\"", StringComparison.Ordinal), "uint64 is a JSON string");
Require(JsonParser.Default.Parse<ResultBatch>(largeJson).Sequence == large.Sequence, "uint64 precision");

var start = new CoordinatorMessage
{
    SessionId = "44444444-4444-4444-8444-444444444444",
    CommandId = "55555555-5555-4555-8555-555555555555",
    Start = new StartAssignment
    {
        RunId = batch.RunId,
        AssignmentId = batch.AssignmentId,
        StartAt = batch.Interval.StartAt,
        LeaseExpiresAt = Timestamp.FromDateTime(batch.Interval.StartAt.ToDateTime().AddSeconds(5)),
        LeaseSequence = 1
    }
};
Require(start.PayloadCase == CoordinatorMessage.PayloadOneofCase.Start, "Start oneof");
Require(CoordinatorMessage.Parser.ParseFrom(start.ToByteArray()).Equals(start), "Control binary round trip");
var startPath = Path.Combine(Path.GetDirectoryName(args[1])!, "start-command.bin");
File.WriteAllBytes(startPath, start.ToByteArray());

var manifest = new RunManifest
{
    SchemaVersion = 1,
    RunId = batch.RunId,
    Snapshot = new ScenarioSnapshot
    {
        ScenarioId = "33333333-3333-4333-8333-333333333333",
        Revision = 1,
        Request = new HttpRequestSpec { Method = "POST", Url = "http://demo-target:8080/", Body = ByteString.CopyFromUtf8("hello") },
        TotalRps = 5,
        DurationMs = 1_000,
        RequestTimeoutMs = 500,
        DrainTimeoutMs = 1_000,
        MaxConcurrencyPerAgent = 4,
        MaxResponseBodyBytes = 1_048_576
    },
    StartAt = batch.Interval.StartAt,
    ResultDeadline = Timestamp.FromDateTime(batch.Interval.StartAt.ToDateTime().AddSeconds(32))
};
manifest.Assignments.Add(new ExpectedAssignment { AssignmentId = batch.AssignmentId, AgentId = batch.AgentId, AssignedRps = 5, PlannedRequests = 5 });
Require(JsonParser.Default.Parse<RunManifest>(JsonFormatter.Default.Format(manifest)).Equals(manifest), "Snapshot and manifest round trip");
File.WriteAllText(Path.Combine(Path.GetDirectoryName(args[1])!, "manifest.json"), JsonFormatter.Default.Format(manifest));
Console.WriteLine("PASS: C# sample, binary, ProtoJSON, uint64, control and manifest checks");

static void Require(bool condition, string message)
{
    if (!condition)
    {
        throw new InvalidOperationException(message);
    }
}
