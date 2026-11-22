package engine

import (
	"testing"
	"time"
)

// TestHistogram проверяет включительные границы, округление вверх и overflow layout latency-us-v1.
func TestHistogram(t *testing.T) {
	cases := []struct {
		name    string
		elapsed time.Duration
		bucket  int
	}{
		{"zero", 0, 0}, {"inclusive", 100 * time.Microsecond, 0}, {"ceil", 100*time.Microsecond + 1, 1},
		{"maximum", 300 * time.Second, 19}, {"overflow", 300*time.Second + 1, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			histogram := Histogram{LayoutID: "latency-us-v1"}
			histogram.record(tc.elapsed)
			if tc.bucket < 0 {
				if histogram.OverflowCount != 1 {
					t.Fatal("missing overflow")
				}
				return
			}
			if histogram.BucketCounts[tc.bucket] != 1 || histogram.OverflowCount != 0 {
				t.Fatalf("histogram: %+v", histogram)
			}
		})
	}
}
