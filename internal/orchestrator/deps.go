package orchestrator

import (
	"github.com/dapr/durabletask-go/workflow"
	daprclient "github.com/dapr/go-sdk/client"

	"github.com/BenLocal/my-bot/internal/a2a"
	"github.com/BenLocal/my-bot/internal/store"
)

// Package-level dependencies for activities. These are set by the Runtime
// during initialization and must be set before any workflow runs.
var (
	depStore          store.Store
	depA2A            *a2a.Client
	depToken          string
	depWorkflowClient *workflow.Client
	depDaprClient     daprclient.Client
)

// SetDeps sets the package-level dependencies for activities.
// Called by the Runtime main during initialization.
func SetDeps(s store.Store, client *a2a.Client, token string) {
	depStore = s
	depA2A = client
	depToken = token
}

// SetWorkflowClient sets the Dapr workflow client for ScheduleWorkflow activity.
func SetWorkflowClient(c *workflow.Client) {
	depWorkflowClient = c
}

// SetDaprClient sets the Dapr client used to invoke the Scheduler actor.
func SetDaprClient(c daprclient.Client) {
	depDaprClient = c
}
