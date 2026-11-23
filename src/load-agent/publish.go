package main

import (
	"context"
	"errors"
	"slices"
	"time"

	contracts "github.com/andrey-x-code/latencystorm/contracts/go/gen/latencystorm/v1"
	"github.com/andrey-x-code/latencystorm/src/load-agent/internal/engine"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errLeaseExpired = errors.New("lease expired")

// launchAssignment владеет движком и очередью Kafka до ограниченного завершения доставки.
func launchAssignment(runCtx, streamCtx context.Context, start time.Time, assignment remoteAssignment, agentID string,
	writer *kafka.Writer, started chan<- *contracts.AssignmentStarted, finished chan<- *contracts.AssignmentFinished) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		prepare := assignment.prepare
		generation := engine.New()
		result := make(chan engine.Result, 1)
		go func() {
			timer := time.NewTimer(time.Until(start))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-runCtx.Done():
			}
			value, _ := generation.RunScheduled(runCtx, assignment.config, start, func(actual time.Time) {
				started <- &contracts.AssignmentStarted{RunId: prepare.RunId, AssignmentId: prepare.AssignmentId,
					ActualStartAt: timestamppb.New(actual)}
			})
			result <- value
		}()

		delivery, cancel := context.WithCancel(streamCtx)
		defer cancel()
		queue := make(chan *contracts.ResultBatch, 63)
		published := make(chan bool, 1)
		var lost bool
		var sequence uint64
		var dropped bool
		go func() {
			var deliveryLost, finalPublished bool
			for batch := range queue {
				batch.MeasurementsLost = batch.MeasurementsLost || deliveryLost
				ok := publishBatch(delivery, writer, batch)
				deliveryLost = deliveryLost || !ok
				finalPublished = batch.IsFinal && ok
			}
			lost = deliveryLost
			published <- finalPublished
		}()
		for interval := range generation.Intervals() {
			sequence++
			batch := resultBatch(prepare, agentID, sequence, interval)
			batch.MeasurementsLost = dropped
			if len(queue) >= cap(queue)-1 {
				dropped = true
				continue
			}
			queue <- batch
		}
		value := <-result
		state := contracts.AssignmentState_ASSIGNMENT_STATE_COMPLETED
		var reason contracts.FailureReason
		if value.Stopped {
			state = contracts.AssignmentState_ASSIGNMENT_STATE_STOPPED
			if errors.Is(context.Cause(runCtx), errLeaseExpired) {
				state = contracts.AssignmentState_ASSIGNMENT_STATE_FAILED
				reason = contracts.FailureReason_FAILURE_REASON_LEASE_EXPIRED
			}
		}
		deadline := time.AfterFunc(30*time.Second, cancel)
		defer deadline.Stop()
		sequence++
		final := resultBatch(prepare, agentID, sequence, value.Final)
		final.IsFinal = true
		final.MeasurementsLost = dropped || value.MeasurementsLost
		final.Summary = &contracts.AssignmentSummary{State: state, FailureReason: reason,
			GenerationStoppedAt: timestamppb.New(value.GenerationStoppedAt), FinishedAt: timestamppb.New(value.FinishedAt),
			GenerationDurationUs: value.GenerationDurationUs}
		if value.ActualStartAt != nil {
			final.Summary.ActualStartAt = timestamppb.New(*value.ActualStartAt)
		}
		queue <- final
		close(queue)
		finalPublished := <-published
		finished <- &contracts.AssignmentFinished{RunId: prepare.RunId, AssignmentId: prepare.AssignmentId,
			State: state, FailureReason: reason, GenerationStoppedAt: final.Summary.GenerationStoppedAt,
			FinishedAt: final.Summary.FinishedAt, FinalSequence: sequence,
			MeasurementsLost: final.MeasurementsLost || lost, FinalBatchPublished: finalPublished}
	}()
	return done
}

// publishBatch повторяет неизменяемый пакет с теми же ключом, sequence и временем.
func publishBatch(ctx context.Context, writer *kafka.Writer, batch *contracts.ResultBatch) bool {
	if ctx.Err() != nil {
		return false
	}
	payload, err := protojson.Marshal(batch)
	if err != nil || len(payload) > 256<<10 {
		return false
	}
	message := kafka.Message{Key: []byte(batch.AssignmentId), Value: payload, Time: batch.Interval.EndAt.AsTime(),
		Headers: []kafka.Header{{Key: "content-type", Value: []byte("application/json")}}}
	for attempt := range 3 {
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := writer.WriteMessages(call, message)
		cancel()
		if err == nil {
			return true
		}
		if attempt < 2 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(time.Duration(100<<attempt) * time.Millisecond):
			}
		}
	}
	return false
}

// resultBatch переводит дельту без потери точности uint64 и единиц гистограммы.
func resultBatch(prepare *contracts.PrepareAssignment, agentID string, sequence uint64, interval engine.Interval) *contracts.ResultBatch {
	histogram := func(value engine.Histogram) *contracts.LatencyHistogram {
		return &contracts.LatencyHistogram{LayoutId: value.LayoutID, BucketCounts: value.BucketCounts[:], OverflowCount: value.OverflowCount}
	}
	counters := interval.Counters
	batch := &contracts.ResultBatch{
		SchemaVersion: 1, RunId: prepare.RunId, AssignmentId: prepare.AssignmentId, AgentId: agentID, Sequence: sequence,
		Interval: &contracts.MeasurementInterval{StartAt: timestamppb.New(interval.StartAt), EndAt: timestamppb.New(interval.EndAt)},
		Counters: &contracts.RequestCounters{Scheduled: counters.Scheduled, Started: counters.Started, Skipped: counters.Skipped,
			Finished: counters.Finished, HttpResponses: counters.HTTPResponses, HttpErrors: counters.HTTPErrors,
			TransportErrors: counters.TransportErrors, Timeouts: counters.Timeouts, Cancelled: counters.Cancelled,
			ResponseBodyLimitExceeded: counters.ResponseBodyLimitExceeded},
		HttpLatency: histogram(interval.HTTPLatency), FailureLatency: histogram(interval.FailureLatency),
	}
	for code, count := range interval.HTTPStatusCounts {
		batch.HttpStatusCounts = append(batch.HttpStatusCounts, &contracts.HttpStatusCount{StatusCode: uint32(code), Count: count})
	}
	slices.SortFunc(batch.HttpStatusCounts, func(left, right *contracts.HttpStatusCount) int {
		return int(left.StatusCode) - int(right.StatusCode)
	})
	return batch
}
