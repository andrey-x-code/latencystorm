package engine

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

// testConfig возвращает небольшой валидный сценарий для проверки наблюдаемого поведения.
func testConfig() Config {
	return Config{Method: "GET", URL: "http://localhost/", RPS: 10, DurationMs: 2000, RequestTimeoutMs: 1000, MaxConcurrency: 20, MaxResponseBodyBytes: 1024, DrainTimeoutMs: 500}
}

// TestScheduleIndependentOfResponses проверяет монотонную сетку независимо от скорости ответов.
func TestScheduleIndependentOfResponses(t *testing.T) {
	for _, delay := range []time.Duration{0, 250 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config := testConfig()
				start := time.Now()
				starts := make(chan time.Duration, 20)
				generator := New()
				result, err := generator.run(t.Context(), config, func(ctx context.Context, _ *http.Client, _ Config) outcome {
					starts <- time.Since(start)
					time.Sleep(delay)
					return outcome{status: 200, elapsed: delay}
				})
				if err != nil {
					t.Fatal(err)
				}
				close(starts)
				index := 0
				for at := range starts {
					if at != time.Duration(index)*100*time.Millisecond {
						t.Fatalf("slot %d at %v", index, at)
					}
					index++
				}
				if index != 20 || result.Totals.Started != 20 || result.Totals.Finished != 20 || result.Totals.Skipped != 0 {
					t.Fatalf("result: %+v", result)
				}
				assertIntervals(t, generator, result)
			})
		})
	}
}

// TestDelayedScheduler проверяет пропуски после задержки пробуждения без догоняющего всплеска.
func TestDelayedScheduler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := testConfig()
		generator := New()
		reads := 0
		clock := func() time.Time {
			reads++
			if reads == 2 {
				time.Sleep(350 * time.Millisecond)
			}
			return time.Now()
		}
		starts := make(chan time.Time, 20)
		result, err := generator.runWithClock(t.Context(), config, func(context.Context, *http.Client, Config) outcome {
			starts <- time.Now()
			return outcome{status: 200}
		}, clock)
		if err != nil {
			t.Fatal(err)
		}
		close(starts)
		if result.Totals.Scheduled != 20 || result.Totals.Skipped != 3 || result.Totals.Started != 17 {
			t.Fatalf("result: %+v", result)
		}
		var previous time.Time
		for at := range starts {
			if !previous.IsZero() && at.Sub(previous) < 50*time.Millisecond {
				t.Fatal("catch-up burst")
			}
			previous = at
		}
		assertIntervals(t, generator, result)
	})
}

// TestConcurrencyAndDrain проверяет пропуски и отмену запросов по ограниченному drain.
func TestConcurrencyAndDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := testConfig()
		config.MaxConcurrency = 1
		generator := New()
		result, err := generator.run(t.Context(), config, func(ctx context.Context, _ *http.Client, _ Config) outcome {
			<-ctx.Done()
			return outcome{kind: "cancelled", elapsed: 2500 * time.Millisecond}
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Totals.Started != 1 || result.Totals.Skipped != 19 || result.Totals.Cancelled != 1 || result.GenerationDurationUs != 2_000_000 {
			t.Fatalf("result: %+v", result)
		}
		if result.FinishedAt.Sub(result.GenerationStoppedAt) != 500*time.Millisecond {
			t.Fatal("drain duration changed")
		}
		assertIntervals(t, generator, result)
	})
}

// TestManualStop проверяет остановку до начала и во время генерации.
func TestManualStop(t *testing.T) {
	for _, before := range []bool{true, false} {
		name := "during generation"
		if before {
			name = "before start"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if before {
					cancel()
				} else {
					go func() { time.Sleep(350 * time.Millisecond); cancel() }()
				}
				generator := New()
				result, err := generator.run(ctx, testConfig(), func(context.Context, *http.Client, Config) outcome { return outcome{status: 200} })
				if err != nil {
					t.Fatal(err)
				}
				expected := uint64(4)
				if before {
					expected = 0
				}
				if !result.Stopped || result.Totals.Started != expected || result.Totals.Finished != expected {
					t.Fatalf("result: %+v", result)
				}
				if before && (result.ActualStartAt != nil || !result.Final.StartAt.Equal(result.Final.EndAt)) {
					t.Fatal("pre-start stop must have a zero interval")
				}
				assertIntervals(t, generator, result)
			})
		})
	}
}

// TestSlowConsumer проверяет ограниченную очередь без блокировки планировщика.
func TestSlowConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := testConfig()
		config.DurationMs = 20000
		config.RPS = 1
		generator := New()
		result, err := generator.run(t.Context(), config, func(context.Context, *http.Client, Config) outcome { return outcome{status: 200} })
		if err != nil {
			t.Fatal(err)
		}
		if !result.MeasurementsLost || result.Totals.Started != 20 || len(generator.intervals) != 16 {
			t.Fatalf("result: %+v", result)
		}
		if _, err := generator.Run(t.Context(), config); err == nil {
			t.Fatal("second execution accepted")
		}
	})
}

// assertIntervals проверяет непрерывность интервалов и инварианты дельт после drain.
func assertIntervals(t *testing.T, generator *Engine, result Result) {
	t.Helper()
	intervals := []Interval{}
	for interval := range generator.Intervals() {
		intervals = append(intervals, interval)
	}
	intervals = append(intervals, result.Final)
	var scheduled, started, finished uint64
	for index, interval := range intervals {
		if index > 0 && !intervals[index-1].EndAt.Equal(interval.StartAt) {
			t.Fatal("interval gap")
		}
		c := interval.Counters
		if c.Scheduled != c.Started+c.Skipped || c.Finished != c.HTTPResponses+c.TransportErrors+c.Timeouts+c.Cancelled+c.ResponseBodyLimitExceeded {
			t.Fatalf("invalid counters: %+v", c)
		}
		var httpSamples, failureSamples uint64
		for _, count := range interval.HTTPLatency.BucketCounts {
			httpSamples += count
		}
		for _, count := range interval.FailureLatency.BucketCounts {
			failureSamples += count
		}
		if httpSamples+interval.HTTPLatency.OverflowCount != c.HTTPResponses || failureSamples+interval.FailureLatency.OverflowCount != c.Finished-c.HTTPResponses {
			t.Fatal("histogram mismatch")
		}
		scheduled += c.Scheduled
		started += c.Started
		finished += c.Finished
	}
	if scheduled != result.Totals.Scheduled || started != result.Totals.Started || finished != started {
		t.Fatal("interval totals mismatch")
	}
}
