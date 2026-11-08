package main

import (
	"encoding/json"
	"fmt"
	common "github.com/andrey-x-code/latencystorm/src/go-common"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type settings struct {
	delay     time.Duration
	status    int
	errorRate float64
}

func readSettings() (settings, error) {
	readInt := func(name string, fallback, min, max int) (int, error) {
		raw := os.Getenv(name)
		if raw == "" {
			return fallback, nil
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value < min || value > max {
			return 0, fmt.Errorf("invalid %s", name)
		}
		return value, nil
	}
	delay, err := readInt("DEMO_DELAY_MS", 0, 0, 300000)
	if err != nil {
		return settings{}, err
	}
	status, err := readInt("DEMO_STATUS_CODE", 200, 200, 599)
	if err != nil {
		return settings{}, err
	}
	rate := 0.0
	if raw := os.Getenv("DEMO_ERROR_RATE"); raw != "" {
		rate, err = strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) || rate < 0 || rate > 1 {
			return settings{}, fmt.Errorf("invalid DEMO_ERROR_RATE")
		}
	}
	return settings{time.Duration(delay) * time.Millisecond, status, rate}, nil
}
func target(config settings, requests *atomic.Uint64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if config.delay > 0 {
			timer := time.NewTimer(config.delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		status := config.status
		if rand.Float64() < config.errorRate {
			status = 500
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "delayMs": config.delay.Milliseconds()})
	}
}
func main() {
	if common.Healthcheck() {
		return
	}
	log := common.Logger("demo-target")
	config, err := readSettings()
	if err != nil {
		log.Error("configuration rejected", "error", err)
		os.Exit(1)
	}
	state := &common.Readiness{}
	requests := &atomic.Uint64{}
	mux := http.NewServeMux()
	common.Endpoints(mux, "demo-target", state, requests)
	mux.HandleFunc("/", target(config, requests))
	log.Info("service starting", "delayMs", config.delay.Milliseconds(), "statusCode", config.status, "errorRate", config.errorRate)
	if err := common.Run(mux, state, log, nil); err != nil {
		log.Error("service failed", "error", err)
		os.Exit(1)
	}
}
