package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	contracts "github.com/andrey-x-code/latencystorm/contracts/go/gen/latencystorm/v1"
	common "github.com/andrey-x-code/latencystorm/src/go-common"
	"github.com/andrey-x-code/latencystorm/src/load-agent/internal/engine"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// remoteAssignment принадлежит циклу команд; рабочая горутина получает неизменяемый снимок.
type remoteAssignment struct {
	prepare *contracts.PrepareAssignment
	command string
	config  engine.Config
	phase   contracts.AssignmentState
	lease   uint64
	expires time.Time
	cancel  context.CancelCauseFunc
}

// connectAgent принимает команды в одной горутине и сериализует исходящие сообщения.
func connectAgent(parent context.Context, client contracts.AgentControlClient, agentID, sessionID, broker string,
	writer *kafka.Writer, readiness *common.Readiness) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer readiness.Set(false)
	stream, err := client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("open control stream: %w", err)
	}
	send := func(message *contracts.AgentMessage) error {
		message.AgentId, message.SessionId = agentID, sessionID
		return stream.Send(message)
	}
	watchdog := time.AfterFunc(8*time.Second, cancel)
	defer watchdog.Stop()
	if err := send(&contracts.AgentMessage{Payload: &contracts.AgentMessage_Register{Register: &contracts.RegisterAgent{
		ProtocolVersion: 1, AgentVersion: "stage-05",
	}}}); err != nil {
		return fmt.Errorf("register agent: %w", err)
	}
	incoming := make(chan *contracts.CoordinatorMessage)
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		defer close(incoming)
		for {
			command, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case incoming <- command:
			case <-ctx.Done():
				return
			}
		}
	}()
	var active *remoteAssignment
	var lastFinished string
	var workerDone chan struct{}
	started := make(chan *contracts.AssignmentStarted, 1)
	finished := make(chan *contracts.AssignmentFinished, 1)
	defer func() {
		cancel()
		if active != nil && active.cancel != nil {
			active.cancel(nil)
		}
		if workerDone != nil {
			<-workerDone
		}
		<-receiverDone
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	leaseTimer := time.NewTimer(time.Hour)
	defer leaseTimer.Stop()
	leaseTimer.Stop()
	registered := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-leaseTimer.C:
			if active != nil && active.cancel != nil {
				active.cancel(errLeaseExpired)
			}
		case <-ticker.C:
			if !registered {
				continue
			}
			beat := &contracts.Heartbeat{SentAt: timestamppb.Now()}
			if active != nil {
				beat.AssignmentId, beat.State = active.prepare.AssignmentId, active.phase
			}
			if err := send(&contracts.AgentMessage{Payload: &contracts.AgentMessage_Heartbeat{Heartbeat: beat}}); err != nil {
				return fmt.Errorf("send heartbeat: %w", err)
			}
		case event := <-started:
			if active.phase != contracts.AssignmentState_ASSIGNMENT_STATE_STOPPING {
				active.phase = contracts.AssignmentState_ASSIGNMENT_STATE_RUNNING
			}
			if err := send(&contracts.AgentMessage{Payload: &contracts.AgentMessage_Started{Started: event}}); err != nil {
				return fmt.Errorf("send assignment start: %w", err)
			}
		case event := <-finished:
			<-workerDone
			workerDone = nil
			select {
			case startEvent := <-started:
				if err := send(&contracts.AgentMessage{Payload: &contracts.AgentMessage_Started{Started: startEvent}}); err != nil {
					return fmt.Errorf("send assignment start: %w", err)
				}
			default:
			}
			leaseTimer.Stop()
			active.cancel(nil)
			lastFinished, active = event.AssignmentId, nil
			if err := send(&contracts.AgentMessage{Payload: &contracts.AgentMessage_Finished{Finished: event}}); err != nil {
				return fmt.Errorf("send assignment finish: %w", err)
			}
		case command, open := <-incoming:
			if !open || command.SessionId != sessionID {
				return fmt.Errorf("control stream closed or invalid session")
			}
			if !registered {
				if command.GetRegistered().GetProtocolVersion() != 1 {
					return fmt.Errorf("invalid registration acknowledgement")
				}
				watchdog.Stop()
				registered = true
				readiness.Set(true)
				continue
			}
			if prepare := command.GetPrepare(); prepare != nil {
				if prepare.AssignmentId == lastFinished || (active != nil && active.command != command.CommandId) {
					continue
				}
				if active == nil {
					config, err := prepareConfig(prepare)
					if err == nil {
						probe, stop := context.WithTimeout(ctx, 2*time.Second)
						conn, dialErr := (&kafka.Dialer{Timeout: 2 * time.Second}).DialContext(probe, "tcp", broker)
						stop()
						if dialErr == nil {
							conn.Close()
						}
						err = dialErr
					}
					if err != nil {
						if err := reject(send, command.CommandId, prepare.RunId, prepare.AssignmentId); err != nil {
							return err
						}
						continue
					}
					active = &remoteAssignment{prepare: prepare, command: command.CommandId, config: config,
						phase: contracts.AssignmentState_ASSIGNMENT_STATE_READY}
				}
				if err := send(&contracts.AgentMessage{Payload: &contracts.AgentMessage_Ready{Ready: &contracts.AssignmentReady{
					RunId: active.prepare.RunId, AssignmentId: active.prepare.AssignmentId, PrepareCommandId: active.command,
				}}}); err != nil {
					return fmt.Errorf("send assignment ready: %w", err)
				}
				continue
			}
			if active == nil {
				continue
			}
			matches := func(runID, assignmentID string) bool {
				return active.prepare.RunId == runID && active.prepare.AssignmentId == assignmentID
			}
			now := time.Now()
			if start := command.GetStart(); start != nil && matches(start.RunId, start.AssignmentId) && active.cancel == nil {
				if start.StartAt == nil || start.LeaseExpiresAt == nil || start.LeaseSequence != 1 ||
					!now.Before(start.StartAt.AsTime()) || !start.StartAt.AsTime().Before(start.LeaseExpiresAt.AsTime()) {
					if err := reject(send, command.CommandId, start.RunId, start.AssignmentId); err != nil {
						return err
					}
					continue
				}
				active.lease = 1
				active.expires = now.Add(min(start.LeaseExpiresAt.AsTime().Sub(now), 5*time.Second))
				leaseTimer.Reset(time.Until(active.expires))
				active.phase = contracts.AssignmentState_ASSIGNMENT_STATE_SCHEDULED
				runCtx, stop := context.WithCancelCause(ctx)
				active.cancel = stop
				workerDone = launchAssignment(runCtx, ctx, now.Add(start.StartAt.AsTime().Sub(now)), *active, agentID, writer, started, finished)
			} else if renew := command.GetRenewLease(); renew != nil && matches(renew.RunId, renew.AssignmentId) &&
				active.lease > 0 && now.Before(active.expires) && renew.LeaseSequence > active.lease &&
				renew.ExpiresAt != nil && now.Before(renew.ExpiresAt.AsTime()) && active.phase != contracts.AssignmentState_ASSIGNMENT_STATE_STOPPING {
				active.lease = renew.LeaseSequence
				active.expires = now.Add(min(renew.ExpiresAt.AsTime().Sub(now), 5*time.Second))
				leaseTimer.Reset(time.Until(active.expires))
			} else if stop := command.GetStop(); stop != nil && matches(stop.RunId, stop.AssignmentId) {
				active.phase = contracts.AssignmentState_ASSIGNMENT_STATE_STOPPING
				if active.cancel == nil {
					runCtx, cancelRun := context.WithCancelCause(ctx)
					active.cancel = cancelRun
					cancelRun(nil)
					workerDone = launchAssignment(runCtx, ctx, now, *active, agentID, writer, started, finished)
				} else {
					active.cancel(nil)
				}
			}
		}
	}
}

// reject сообщает о непригодной команде без вывода содержимого сценария.
func reject(send func(*contracts.AgentMessage) error, command, run, assignment string) error {
	return send(&contracts.AgentMessage{Payload: &contracts.AgentMessage_Rejected{Rejected: &contracts.CommandRejected{
		CommandId: command, RunId: run, AssignmentId: assignment,
		Reason: contracts.CommandRejectionReason_COMMAND_REJECTION_REASON_INVALID_ARGUMENT,
	}}})
}

// prepareConfig проверяет версию снимка, адрес, заголовки и лимиты движка.
func prepareConfig(prepare *contracts.PrepareAssignment) (engine.Config, error) {
	snapshot := prepare.GetSnapshot()
	request := snapshot.GetRequest()
	_, runErr := uuid.Parse(prepare.GetRunId())
	_, assignmentErr := uuid.Parse(prepare.GetAssignmentId())
	if runErr != nil || assignmentErr != nil || request == nil || snapshot.Revision == 0 ||
		prepare.AssignedRps != snapshot.TotalRps || prepare.PrepareDeadline == nil ||
		!time.Now().Before(prepare.PrepareDeadline.AsTime()) || len(request.Url) > 8192 {
		return engine.Config{}, fmt.Errorf("invalid assignment snapshot")
	}
	config := engine.Config{Method: request.Method, URL: request.Url, Body: string(request.Body),
		Headers: make(map[string]string), RPS: int(prepare.AssignedRps), DurationMs: int(snapshot.DurationMs),
		RequestTimeoutMs: int(snapshot.RequestTimeoutMs), MaxConcurrency: int(snapshot.MaxConcurrencyPerAgent),
		MaxResponseBodyBytes: int64(snapshot.MaxResponseBodyBytes), DrainTimeoutMs: int(snapshot.DrainTimeoutMs)}
	bytes := 0
	for _, header := range request.Headers {
		name := strings.ToLower(header.GetName())
		switch name {
		case "host", "proxy-connection", "proxy-authenticate", "proxy-authorization", "keep-alive", "te":
			return engine.Config{}, fmt.Errorf("forbidden header")
		}
		if _, exists := config.Headers[name]; exists || len(header.GetValues()) != 1 {
			return engine.Config{}, fmt.Errorf("invalid header values")
		}
		config.Headers[name] = header.Values[0]
		bytes += len(name) + len(header.Values[0])
	}
	if bytes > 64<<10 {
		return engine.Config{}, fmt.Errorf("headers exceed 64 KiB")
	}
	return config, config.Validate()
}
