package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

type Readiness struct{ ready atomic.Bool }

func (r *Readiness) Set(value bool) { r.ready.Store(value) }
func (r *Readiness) Ready() bool    { return r.ready.Load() }
func Logger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", service)
}
func Healthcheck() bool {
	if len(os.Args) != 2 || os.Args[1] != "--healthcheck" {
		return false
	}
	client := http.Client{Timeout: 3 * time.Second}
	response, err := client.Get("http://127.0.0.1:8080/ready")
	if err != nil {
		os.Exit(1)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		os.Exit(1)
	}
	return true
}
func Endpoints(mux *http.ServeMux, service string, state *Readiness, requests *atomic.Uint64) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "alive", "service": service})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !state.Ready() {
			w.WriteHeader(503)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": state.Ready(), "service": service})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		ready := 0
		if state.Ready() {
			ready = 1
		}
		fmt.Fprintf(w, "# HELP latencystorm_service_ready Dependency readiness.\n# TYPE latencystorm_service_ready gauge\nlatencystorm_service_ready{service=%q} %d\n", service, ready)
		if requests != nil {
			fmt.Fprintf(w, "# HELP latencystorm_demo_requests_total Target requests.\n# TYPE latencystorm_demo_requests_total counter\nlatencystorm_demo_requests_total %d\n", requests.Load())
		}
	})
}

// Run owns the server and optional dependency monitor; both stop before returning.
func Run(mux *http.ServeMux, state *Readiness, log *slog.Logger, monitor func(context.Context)) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 310 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10, BaseContext: nil}
	monitorDone := make(chan struct{})
	if monitor != nil {
		go func() { defer close(monitorDone); monitor(ctx) }()
	} else {
		state.Set(true)
		close(monitorDone)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe() }()
	var result error
	select {
	case <-ctx.Done():
	case err := <-serverDone:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
		stop()
	}
	state.Set(false)
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		result = err
	}
	<-monitorDone
	log.Info("service stopped")
	return result
}
