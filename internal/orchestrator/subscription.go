package orchestrator

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/dapr/durabletask-go/workflow"
)

// SubscriptionResult is the three-state return for subscription processing.
type SubscriptionResult string

const (
	SubscriptionDrop    SubscriptionResult = "DROP"
	SubscriptionSuccess SubscriptionResult = "SUCCESS"
	SubscriptionRetry   SubscriptionResult = "RETRY"
)

// AttemptEvent is the pub/sub message from workers.
type AttemptEvent struct {
	AttemptKey string `json:"attemptKey"`
	GoalID     string `json:"goalID"`
	Status     string `json:"status"`
	At         string `json:"at"`
}

// SubscriptionHandler processes attempt-events and raises signals to workflows.
type SubscriptionHandler struct {
	client *workflow.Client
	logger *slog.Logger
}

func NewSubscriptionHandler(client *workflow.Client, logger *slog.Logger) *SubscriptionHandler {
	return &SubscriptionHandler{client: client, logger: logger}
}

// Handle processes a single pub/sub message.
func (h *SubscriptionHandler) Handle(ctx context.Context, topic string, data []byte) SubscriptionResult {
	var event AttemptEvent
	if err := json.Unmarshal(data, &event); err != nil {
		h.logger.Warn("failed to parse attempt-event", "topic", topic, "error", err)
		return SubscriptionDrop
	}

	if event.GoalID == "" || event.AttemptKey == "" {
		h.logger.Warn("missing fields in attempt-event", "topic", topic)
		return SubscriptionDrop
	}

	// RaiseEvent to the workflow instance (goalID is the instance ID)
	signal := Signal{
		Kind: SignalAttemptDone,
		Key:  event.AttemptKey,
	}

	err := h.client.RaiseEvent(ctx, event.GoalID, signalEventName, workflow.WithEventPayload(signal))
	if err == nil {
		h.logger.Debug("attempt-event delivered", "goalID", event.GoalID, "attemptKey", event.AttemptKey, "status", event.Status)
		return SubscriptionSuccess
	}

	// Check for "no such instance exists" error
	errMsg := err.Error()
	if strings.Contains(errMsg, "no such instance exists") || strings.Contains(errMsg, "instance not found") {
		return SubscriptionDrop
	}

	h.logger.Warn("RaiseEvent failed", "goalID", event.GoalID, "error", err)
	return SubscriptionRetry
}

// BulkHandle processes multiple messages.
func (h *SubscriptionHandler) BulkHandle(ctx context.Context, topic string, messages [][]byte) []SubscriptionResult {
	results := make([]SubscriptionResult, len(messages))
	for i, data := range messages {
		results[i] = h.Handle(ctx, topic, data)
	}
	return results
}
