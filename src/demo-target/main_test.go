package main

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestTargetStatusAndErrorProbability(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		rate   float64
		want   int
	}{
		{"configured status", 418, 0, 418}, {"forced error", 201, 1, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := &atomic.Uint64{}
			response := httptest.NewRecorder()
			target(settings{status: tc.status, errorRate: tc.rate}, requests)(response, httptest.NewRequest("GET", "/", nil))
			if response.Code != tc.want || requests.Load() != 1 {
				t.Fatalf("status=%d count=%d", response.Code, requests.Load())
			}
		})
	}
}

func TestTargetDelayAndCancellation(t *testing.T) {
	response := httptest.NewRecorder()
	start := time.Now()
	target(settings{status: 200, delay: 20 * time.Millisecond}, &atomic.Uint64{})(response, httptest.NewRequest("GET", "/", nil))
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("response returned before configured delay")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response = httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		target(settings{status: 200, delay: time.Minute}, &atomic.Uint64{})(response, httptest.NewRequest("GET", "/", nil).WithContext(ctx))
	}()
	select {
	case <-done:
		if response.Body.Len() != 0 {
			t.Fatal("cancelled request wrote a body")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled handler did not stop")
	}
}

func TestSettingsValidation(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"DEMO_DELAY_MS", "-1"}, {"DEMO_STATUS_CODE", "600"}, {"DEMO_ERROR_RATE", "NaN"}, {"DEMO_ERROR_RATE", "1.1"},
	} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv("DEMO_DELAY_MS", "0")
			t.Setenv("DEMO_STATUS_CODE", "200")
			t.Setenv("DEMO_ERROR_RATE", "0")
			t.Setenv(tc.name, tc.value)
			if _, err := readSettings(); err == nil {
				t.Fatal("invalid setting accepted")
			}
		})
	}
}
