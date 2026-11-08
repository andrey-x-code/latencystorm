package compat

import (
	"os"
	"path/filepath"
	"testing"

	contractsv1 "github.com/andrey-x-code/latencystorm/contracts/go/gen/latencystorm/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func root(t *testing.T) string {
	t.Helper()
	value := os.Getenv("CONTRACT_ROOT")
	if value == "" {
		t.Fatal("CONTRACT_ROOT must point to the repository root")
	}
	return value
}

func read(t *testing.T, relative string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root(t), relative))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func decodeBatch(t *testing.T, data []byte) *contractsv1.ResultBatch {
	t.Helper()
	value := &contractsv1.ResultBatch{}
	if err := protojson.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestCSharpGoJSONCompatibility(t *testing.T) {
	sample := decodeBatch(t, read(t, "contracts/events/examples/result-batch.v1.json"))
	csharp := decodeBatch(t, read(t, ".artifacts/csharp.json"))
	if !proto.Equal(sample, csharp) {
		t.Fatal("C# output changed sample values")
	}
	if sample.SchemaVersion != 1 || sample.Sequence != 1 || !sample.IsFinal || sample.MeasurementsLost {
		t.Fatal("invalid envelope")
	}
	if sample.RunId != "11111111-1111-4111-8111-111111111111" || sample.AssignmentId != "22222222-2222-4222-8222-222222222222" || sample.AgentId != "agent-01" {
		t.Fatal("invalid identities")
	}
	c := sample.Counters
	if c.Scheduled != 5 || c.Started != 4 || c.Skipped != 1 || c.Finished != 4 || c.HttpResponses != 2 || c.HttpErrors != 1 || c.TransportErrors != 1 || c.Timeouts != 1 || c.Cancelled != 0 || c.ResponseBodyLimitExceeded != 0 {
		t.Fatal("counter values changed")
	}
	if sample.Interval.EndAt.Seconds-sample.Interval.StartAt.Seconds != 1 {
		t.Fatal("interval changed")
	}
	if len(sample.HttpStatusCounts) != 2 || sample.HttpStatusCounts[0].StatusCode != 200 || sample.HttpStatusCounts[1].StatusCode != 500 {
		t.Fatal("status values changed")
	}
	if sample.HttpLatency.LayoutId != "latency-us-v1" || len(sample.HttpLatency.BucketCounts) != 20 || sample.HttpLatency.BucketCounts[2] != 1 || sample.HttpLatency.BucketCounts[3] != 1 || sample.HttpLatency.OverflowCount != 0 {
		t.Fatal("HTTP histogram changed")
	}
	if sample.FailureLatency.LayoutId != "latency-us-v1" || len(sample.FailureLatency.BucketCounts) != 20 || sample.FailureLatency.BucketCounts[11] != 2 || sample.FailureLatency.OverflowCount != 0 {
		t.Fatal("failure histogram changed")
	}
	if sample.Summary.State != contractsv1.AssignmentState_ASSIGNMENT_STATE_COMPLETED || sample.Summary.GenerationDurationUs != 1_000_000 {
		t.Fatal("summary changed")
	}
	wire, err := proto.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	binaryRoundTrip := &contractsv1.ResultBatch{}
	if err := proto.Unmarshal(wire, binaryRoundTrip); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(sample, binaryRoundTrip) {
		t.Fatal("binary round trip changed sample")
	}
	data, err := protojson.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root(t), ".artifacts/go.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestControlAndManifestFromCSharp(t *testing.T) {
	command := &contractsv1.CoordinatorMessage{}
	if err := proto.Unmarshal(read(t, ".artifacts/start-command.bin"), command); err != nil {
		t.Fatal(err)
	}
	start := command.GetStart()
	if start == nil || command.SessionId != "44444444-4444-4444-8444-444444444444" || command.CommandId != "55555555-5555-4555-8555-555555555555" || start.LeaseSequence != 1 || start.StartAt == nil || start.LeaseExpiresAt == nil {
		t.Fatal("C# control oneof did not decode")
	}
	if start.RunId != "11111111-1111-4111-8111-111111111111" || start.AssignmentId != "22222222-2222-4222-8222-222222222222" {
		t.Fatal("assignment identity changed")
	}
	manifest := &contractsv1.RunManifest{}
	if err := protojson.Unmarshal(read(t, ".artifacts/manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 1 || manifest.Snapshot.Revision != 1 || string(manifest.Snapshot.Request.Body) != "hello" || len(manifest.Assignments) != 1 || manifest.Assignments[0].PlannedRequests != 5 || manifest.Snapshot.TotalRps != 5 {
		t.Fatal("manifest fields changed")
	}
}

func TestUint64JSONPrecisionAndStrictParsing(t *testing.T) {
	value := &contractsv1.ResultBatch{Sequence: 9_007_199_254_740_993}
	data, err := protojson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if decodeBatch(t, data).Sequence != value.Sequence {
		t.Fatal("uint64 lost precision")
	}
	invalid := []string{
		`{"schemaVersion":1,"unknownField":true}`,
		`{"sequence":"18446744073709551616"}`,
		`{"summary":{"state":"ASSIGNMENT_STATE_NOT_REAL"}}`,
	}
	for _, data := range invalid {
		if err := protojson.Unmarshal([]byte(data), &contractsv1.ResultBatch{}); err == nil {
			t.Fatalf("accepted invalid payload: %s", data)
		}
	}
}
