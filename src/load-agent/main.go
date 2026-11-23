package main

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"time"

	contracts "github.com/andrey-x-code/latencystorm/contracts/go/gen/latencystorm/v1"
	common "github.com/andrey-x-code/latencystorm/src/go-common"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// main запускает локальный режим или поддерживает сессию агента в сети Compose.
func main() {
	if len(os.Args) > 1 && os.Args[1] == "--local" {
		os.Exit(localMain(os.Args[2:]))
	}
	if common.Healthcheck() {
		return
	}
	log := common.Logger("load-agent")
	address, err := url.Parse(os.Getenv("CONTROL_API_URL"))
	broker := os.Getenv("KAFKA_BROKERS")
	agentID := os.Getenv("AGENT_ID")
	if err != nil || address.Scheme != "http" || address.Host == "" || broker == "" ||
		!regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`).MatchString(agentID) {
		log.Error("valid CONTROL_API_URL, KAFKA_BROKERS and AGENT_ID are required")
		os.Exit(1)
	}
	connection, err := grpc.NewClient("passthrough:///"+address.Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("could not configure coordinator client")
		os.Exit(1)
	}
	defer connection.Close()
	writer := &kafka.Writer{
		Addr: kafka.TCP(broker), Topic: "latencystorm.results.v1", Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, BatchSize: 1, MaxAttempts: 1,
		ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, AllowAutoTopicCreation: true,
	}
	defer writer.Close()
	state := &common.Readiness{}
	mux := http.NewServeMux()
	common.Endpoints(mux, "load-agent", state, nil)
	sessionID := uuid.NewString()
	monitor := func(ctx context.Context) {
		client := contracts.NewAgentControlClient(connection)
		for ctx.Err() == nil {
			err := connectAgent(ctx, client, agentID, sessionID, broker, writer, state)
			if ctx.Err() == nil && err != nil {
				log.Warn("coordinator session disconnected", "agentId", agentID, "sessionId", sessionID)
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	if err := common.Run(mux, state, log, monitor); err != nil {
		log.Error("service failed", "error", err)
		os.Exit(1)
	}
}
