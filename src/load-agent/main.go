package main

import (
	"context"
	"fmt"
	common "github.com/andrey-x-code/latencystorm/src/go-common"
	"github.com/segmentio/kafka-go"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func probe(ctx context.Context, client *http.Client, control, broker string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, control+"/ready", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("control API not ready")
	}
	connection, err := (&kafka.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	_, err = connection.Brokers()
	return err
}
func main() {
	if common.Healthcheck() {
		return
	}
	log := common.Logger("load-agent")
	control := strings.TrimRight(os.Getenv("CONTROL_API_URL"), "/")
	broker := os.Getenv("KAFKA_BROKERS")
	agentID := os.Getenv("AGENT_ID")
	parsed, err := url.Parse(control)
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || broker == "" || agentID == "" {
		log.Error("CONTROL_API_URL, KAFKA_BROKERS and AGENT_ID are required")
		os.Exit(1)
	}
	state := &common.Readiness{}
	mux := http.NewServeMux()
	common.Endpoints(mux, "load-agent", state, nil)
	monitor := func(ctx context.Context) {
		client := &http.Client{Timeout: 2 * time.Second}
		defer client.CloseIdleConnections()
		var previous bool
		for {
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := probe(probeCtx, client, control, broker)
			cancel()
			ready := err == nil
			state.Set(ready)
			if previous != ready {
				log.Info("readiness changed", "ready", ready, "agentId", agentID)
				previous = ready
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
	log.Info("service starting", "agentId", agentID, "stage", "infrastructure")
	if err := common.Run(mux, state, log, monitor); err != nil {
		log.Error("service failed", "error", err)
		os.Exit(1)
	}
}
