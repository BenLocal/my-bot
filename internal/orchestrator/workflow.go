package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dapr/durabletask-go/workflow"

	"github.com/BenLocal/my-bot/internal/core"
)

// WorkflowState is the persisted state passed between rounds via ContinueAsNew.
type WorkflowState struct {
	GoalID        string
	Spec          core.GoalSpec
	Round         int
	Progress      string
	Budget        core.Budget
	FailStreak    int
	NoProgress    int
	PrevSummary   string
	PrevArtifacts int
	CurrArtifacts int
	LastResults   []core.StepResult
	Decision      *core.Decision
	ScheduledAt   *time.Time

	// ValidationFailCount tracks consecutive brain decision validation failures.
	// Per spec §5.3, the brain gets 2 retries before counting as FailStreak.
	ValidationFailCount int

	// PendingSignals is the inbox of signals received but not yet consumed.
	// Using a slice (not map) for deterministic workflow replay.
	PendingSignals []*Signal
}

// Signal kinds for the inbox pattern.
const (
	SignalAttemptDone = "attempt-done"
	SignalSlotGranted = "slot-granted"
	SignalControl     = "control"
	SignalUserInput   = "user-input"
)

// Signal is the envelope for all external events.
type Signal struct {
	Kind string
	Key  string
	Data json.RawMessage
}

// ControlSignal carries pause/resume/cancel/append/ask_user_reply.
type ControlSignal struct {
	Action  string
	Note    string
	ReplyTo string
}

// GoalWorkflow is the Dapr durable workflow for Goal/Loop/Task execution.
func GoalWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var state WorkflowState
	if err := ctx.GetInput(&state); err != nil {
		return nil, fmt.Errorf("workflow input: %w", err)
	}

	for {
		// Drain any pending signals from the inbox.
		drainSignals(ctx, &state)

		// Check for control signals.
		ctrl := tryReadControl(ctx, &state)
		if ctrl != nil {
			switch ctrl.Action {
			case "cancel":
				finalizeGoal(ctx, &state, core.StopCancelled)
				return nil, nil
			case "append":
				state.Spec.Objective += "\n[notes]\n" + ctrl.Note
				continue
			case "ask_user_reply":
				state.Spec.Objective += "\n[user reply]\n" + ctrl.Note
				continue
			case "pause":
				if !waitForResume(ctx, &state) {
					finalizeGoal(ctx, &state, core.StopCancelled)
					return nil, nil
				}
			case "resume":
				// continue
			}
		}

		state.Round++

		roundSummary, stop, reason, err := executeRound(ctx, &state)
		if err != nil {
			return nil, err
		}

		if stop {
			finalizeGoal(ctx, &state, reason)
			return nil, nil
		}

		state.LastResults = roundSummary.Steps
		state.PrevSummary = roundSummary.Summary
		state.PrevArtifacts = state.CurrArtifacts

		ctx.ContinueAsNew(state, workflow.WithKeepUnprocessedEvents())
	}
}

// executeRound runs one round of the goal.
func executeRound(ctx *workflow.WorkflowContext, state *WorkflowState) (core.RoundSummary, bool, core.StopReason, error) {
	// Check stop conditions.
	stop, reason := checkStop(state)
	if stop {
		return core.RoundSummary{}, true, reason, nil
	}

	var steps []core.StepResult

	if state.Spec.Strategy == "brain" {
		brainResult, err := runBrainStep(ctx, state)
		if err != nil {
			return core.RoundSummary{}, false, "", err
		}
		if brainResult.Status != core.StepSucceeded {
			state.FailStreak++
			return core.RoundSummary{
				Round:   state.Round,
				Status:  core.RoundFailed,
				Summary: "brain step failed",
				Steps:   []core.StepResult{brainResult},
			}, false, "", nil
		}

		state.FailStreak = 0

		if brainResult.Decision == nil {
			return core.RoundSummary{}, false, "", fmt.Errorf("no decision in brain result")
		}

		decision := brainResult.Decision
		state.Decision = decision

		// Validate the decision per spec §5.3.
		// Build validation context from current state.
		existingArtifacts := make([]string, 0)
		for _, r := range state.LastResults {
			for _, a := range r.Artifacts {
				existingArtifacts = append(existingArtifacts, a.Path)
			}
		}
		vctx := core.ValidationContext{
			MemberRoles:       state.Spec.Members,
			ExistingArtifacts: existingArtifacts,
			MaxStepsPerRound:  state.Spec.Limits.MaxStepsPerRound,
			MaxTokens:         state.Spec.Limits.MaxTokens,
		}
		violations := core.ValidateDecision(*decision, vctx)
		if len(violations) > 0 {
			state.ValidationFailCount++
			if state.ValidationFailCount >= 2 {
				state.FailStreak++
				state.ValidationFailCount = 0
			}
			// Return violations to the brain for retry.
			violationMsgs := make([]string, len(violations))
			for i, v := range violations {
				violationMsgs[i] = v.String()
			}
			return core.RoundSummary{
				Round:   state.Round,
				Status:  core.RoundFailed,
				Summary: fmt.Sprintf("decision validation failed: %s", strings.Join(violationMsgs, "; ")),
				Steps:   []core.StepResult{brainResult},
			}, false, "", nil
		}
		state.ValidationFailCount = 0

		if decision.Action == "finish" && decision.Done {
			if state.Spec.Check != nil {
				checkResult, err := runCheck(ctx, state)
				if err != nil {
					return core.RoundSummary{}, false, "", err
				}
				steps = append(steps, brainResult)
				steps = append(steps, checkResult)
				if checkResult.Status != core.StepSucceeded {
					return core.RoundSummary{
						Round: state.Round, Status: core.RoundFailed,
						Summary: "check failed", Steps: steps,
					}, false, "", nil
				}
			}
			steps = append(steps, brainResult)
			return core.RoundSummary{
				Round: state.Round, Status: core.RoundSucceeded,
				Summary: decision.Reason, Steps: steps,
			}, true, core.StopFinished, nil
		}

		if decision.Action == "ask_user" {
			// Wait for user input with a specific key.
			askKey := core.AskRequestID(state.GoalID, state.Round)
			sig := awaitSignal(ctx, state, SignalUserInput, askKey, 30*time.Minute)
			if sig == nil {
				state.Spec.Version = "waiting"
				steps = append(steps, brainResult)
				return core.RoundSummary{
					Round: state.Round, Status: core.RoundFailed,
					Summary: "ask_user timed out", Steps: steps,
				}, false, "", nil
			}
			steps = append(steps, brainResult)
			return core.RoundSummary{
				Round: state.Round, Status: core.RoundSucceeded,
				Summary: "ask_user answered", Steps: steps,
			}, false, "", nil
		}

		// dispatch: run member steps.
		memberResults := dispatchMembers(ctx, state, decision)
		steps = append(steps, brainResult)
		steps = append(steps, memberResults...)

		state.CurrArtifacts = 0
		for _, s := range memberResults {
			state.CurrArtifacts += len(s.Artifacts)
		}
	} else {
		strategy := core.NewStrategy(state.Spec.Strategy)
		if strategy == nil {
			return core.RoundSummary{}, false, "", fmt.Errorf("unknown strategy: %s", state.Spec.Strategy)
		}
		st := strategy.Init(state.Spec)
		plans := strategy.Next(st, state.Round)

		for _, plan := range plans {
			result, err := runMemberStep(ctx, state, plan)
			if err != nil {
				return core.RoundSummary{}, false, "", err
			}
			steps = append(steps, result)
		}
		strategy.Absorb(st, steps)
	}

	if core.CheckNoProgress(state.PrevSummary, state.PrevArtifacts, getSummary(steps), state.CurrArtifacts) {
		state.NoProgress++
	} else {
		state.NoProgress = 0
	}

	stop, reason = checkStop(state)

	return core.RoundSummary{
		Round:   state.Round,
		Status:  core.AggregateRound(steps),
		Summary: getSummary(steps),
		Steps:   steps,
	}, stop, reason, nil
}

// checkStop checks whether the goal should stop.
func checkStop(state *WorkflowState) (bool, core.StopReason) {
	summary := state.Progress
	if state.Decision != nil {
		summary = state.Decision.Reason
	}
	return core.CheckStopConditions(
		state.Spec, state.Round,
		state.FailStreak, state.NoProgress,
		state.PrevSummary, state.PrevArtifacts,
		summary, state.CurrArtifacts,
		state.Budget.TokensUsed,
	)
}

// drainSignals non-blockingly reads all pending signals from the event channel
// and stores them in the workflow state's inbox.
func drainSignals(ctx *workflow.WorkflowContext, state *WorkflowState) {
	for {
		task := ctx.WaitForExternalEvent("signal", 0)
		var raw json.RawMessage
		if err := task.Await(&raw); err != nil {
			return // no more signals
		}
		var sig Signal
		if err := json.Unmarshal(raw, &sig); err != nil {
			continue
		}
		sigCopy := sig // copy for safety
		state.PendingSignals = append(state.PendingSignals, &sigCopy)
	}
}

// awaitSignal waits for a signal with the given kind and key.
// First checks the inbox; if not found, waits for the event.
// Returns nil if the deadline is exceeded.
func awaitSignal(ctx *workflow.WorkflowContext, state *WorkflowState, kind string, key string, timeout time.Duration) *Signal {
	deadline := ctx.CurrentTimeUTC().Add(timeout)

	for {
		// Linear scan of inbox for a matching signal.
		for i, sig := range state.PendingSignals {
			if sig.Kind == kind && sig.Key == key {
				// Remove from inbox and return.
				s := state.PendingSignals[i]
				state.PendingSignals = append(state.PendingSignals[:i], state.PendingSignals[i+1:]...)
				return s
			}
		}

		// Check if deadline exceeded.
		remaining := deadline.Sub(ctx.CurrentTimeUTC())
		if remaining <= 0 {
			return nil
		}

		// Wait for next signal.
		task := ctx.WaitForExternalEvent("signal", remaining)
		var raw json.RawMessage
		if err := task.Await(&raw); err != nil {
			return nil
		}
		var sig Signal
		if err := json.Unmarshal(raw, &sig); err != nil {
			continue
		}
		sigCopy := sig

		if sig.Kind == kind && sig.Key == key {
			return &sigCopy
		}

		// Not the one we want; store in inbox and keep looking.
		state.PendingSignals = append(state.PendingSignals, &sigCopy)
	}
}

// tryReadControl attempts to read a control signal from the inbox.
func tryReadControl(ctx *workflow.WorkflowContext, state *WorkflowState) *ControlSignal {
	for i, sig := range state.PendingSignals {
		if sig.Kind == SignalControl {
			s := state.PendingSignals[i]
			state.PendingSignals = append(state.PendingSignals[:i], state.PendingSignals[i+1:]...)
			var ctrl ControlSignal
			_ = json.Unmarshal(s.Data, &ctrl)
			return &ctrl
		}
	}
	return nil
}

// waitForResume waits for a resume signal.
func waitForResume(ctx *workflow.WorkflowContext, state *WorkflowState) bool {
	for {
		// First check inbox.
		for i, sig := range state.PendingSignals {
			if sig.Kind == SignalControl {
				s := state.PendingSignals[i]
				state.PendingSignals = append(state.PendingSignals[:i], state.PendingSignals[i+1:]...)
				var ctrl ControlSignal
				_ = json.Unmarshal(s.Data, &ctrl)
				if ctrl.Action == "resume" {
					return true
				}
				if ctrl.Action == "cancel" {
					return false
				}
			}
		}

		// Wait for next signal (no timeout).
		task := ctx.WaitForExternalEvent("signal", 0)
		var raw json.RawMessage
		if err := task.Await(&raw); err != nil {
			return false
		}
		var sig Signal
		if err := json.Unmarshal(raw, &sig); err != nil {
			continue
		}
		sigCopy := sig
		state.PendingSignals = append(state.PendingSignals, &sigCopy)
	}
}

func getSummary(steps []core.StepResult) string {
	for _, s := range steps {
		if s.Summary != "" {
			return s.Summary
		}
	}
	return ""
}

func finalizeGoal(ctx *workflow.WorkflowContext, state *WorkflowState, reason core.StopReason) {
	status := mapStopReasonToGoalStatus(reason)
	task := ctx.CallActivity(FinishGoal, workflow.WithActivityInput(FinishGoalReq{
		GoalID: state.GoalID,
		Status: status,
		Reason: string(reason),
	}))
	_ = task.Await(nil)
}

// mapStopReasonToGoalStatus maps a stop reason to the appropriate goal status.
func mapStopReasonToGoalStatus(reason core.StopReason) core.GoalStatus {
	switch reason {
	case core.StopFinished:
		return core.GoalSucceeded
	case core.StopCancelled:
		return core.GoalCancelled
	default:
		return core.GoalFailed
	}
}

// acquireThenSubmit acquires a slot, waits if needed, submits the attempt,
// polls until terminal, and releases the slot. Returns the step result.
func acquireThenSubmit(
	ctx *workflow.WorkflowContext,
	state *WorkflowState,
	stepID string,
	role string,
	executor string,
	account string,
	prompt string,
	inputs []string,
	affinityWorker string,
) core.StepResult {
	attemptKey := core.AttemptKey(state.GoalID, state.Round, stepID, 1)

	// Acquire a slot from the scheduler actor.
	acquireTask := ctx.CallActivity(AcquireSlot, workflow.WithActivityInput(AcquireSlotReq{
		WaiterInstanceID: state.GoalID,
		AttemptKey:       attemptKey,
		Executor:         executor,
		Account:          account,
		PreferWorker:     affinityWorker,
	}))
	var slotResp AcquireSlotResp
	if err := acquireTask.Await(&slotResp); err != nil {
		return core.StepResult{StepID: stepID, Role: role, Status: core.StepFailed}
	}

	var lease *LeaseInfo
	if slotResp.Granted && slotResp.Lease != nil {
		lease = slotResp.Lease
	} else {
		// Wait for slot-granted signal.
		sig := awaitSignal(ctx, state, SignalSlotGranted, attemptKey, 5*time.Minute)
		if sig == nil {
			return core.StepResult{StepID: stepID, Role: role, Status: core.StepFailed}
		}
		var data struct {
			Lease *LeaseInfo `json:"lease"`
		}
		if err := json.Unmarshal(sig.Data, &data); err != nil || data.Lease == nil {
			return core.StepResult{StepID: stepID, Role: role, Status: core.StepFailed}
		}
		lease = data.Lease
	}

	// Submit the attempt with the lease.
	sessionPolicy := lease.SessionPolicy
	if sessionPolicy == "" {
		sessionPolicy = "fresh"
	}

	req := SubmitAttemptReq{
		GoalID:        state.GoalID,
		AttemptKey:    attemptKey,
		Round:         state.Round,
		Role:          role,
		WorkerID:      lease.WorkerID,
		Executor:      lease.Executor,
		Account:       lease.Account,
		Prompt:        prompt,
		SessionPolicy: sessionPolicy,
		Inputs:        inputs,
	}

	submitTask := ctx.CallActivity(SubmitAttempt, workflow.WithActivityInput(req))
	var submitResp SubmitAttemptResp
	if err := submitTask.Await(&submitResp); err != nil {
		// Release slot on submit failure.
		releaseTask := ctx.CallActivity(ReleaseSlot, workflow.WithActivityInput(ReleaseSlotReq{
			AttemptKey: attemptKey,
		}))
		_ = releaseTask.Await(nil)
		return core.StepResult{StepID: stepID, Role: role, Status: core.StepFailed}
	}

	// Poll until terminal status.
	pollReq := PollAttemptReq{
		GoalID:       state.GoalID,
		AttemptKey:   attemptKey,
		RemoteTaskID: submitResp.RemoteTaskID,
		WorkerID:     lease.WorkerID,
		Executor:     lease.Executor,
		Round:        state.Round,
	}

	var pollResp PollAttemptResp
	for {
		pollTask := ctx.CallActivity(PollAttempt, workflow.WithActivityInput(pollReq))
		if err := pollTask.Await(&pollResp); err != nil {
			break
		}

		if pollResp.Status == core.AttemptCompleted {
			// Release slot.
			releaseTask := ctx.CallActivity(ReleaseSlot, workflow.WithActivityInput(ReleaseSlotReq{
				AttemptKey: attemptKey,
			}))
			_ = releaseTask.Await(nil)
			return core.StepResult{
				StepID:    stepID,
				Role:      role,
				Status:    core.StepSucceeded,
				Summary:   pollResp.Summary,
				Artifacts: pollResp.Artifacts,
				Usage:     pollResp.Usage,
			}
		}
		if pollResp.Status == core.AttemptFailed || pollResp.Status == core.AttemptRejected {
			break
		}
		ctx.CreateTimer(5 * time.Second)
	}

	// Terminal failure — release slot.
	releaseTask := ctx.CallActivity(ReleaseSlot, workflow.WithActivityInput(ReleaseSlotReq{
		AttemptKey: attemptKey,
	}))
	_ = releaseTask.Await(nil)

	return core.StepResult{StepID: stepID, Role: role, Status: core.StepFailed}
}

// runBrainStep executes the brain step.
func runBrainStep(ctx *workflow.WorkflowContext, state *WorkflowState) (core.StepResult, error) {
	prompt := buildBrainPrompt(state)

	result := acquireThenSubmit(
		ctx, state,
		"brain", state.Spec.Brain,
		getExecutor(state.Spec.Brain), getAccount(state.Spec.Brain),
		prompt, nil,
		state.Spec.Brain, // affinity to same brain
	)

	// Parse decision from summary for brain steps.
	// For M0, the brain's response is a JSON decision in the summary.
	if result.Status == core.StepSucceeded {
		var decision core.Decision
		if err := json.Unmarshal([]byte(result.Summary), &decision); err == nil {
			result.Decision = &decision
		} else {
			// Not JSON — brain returned plain text; treat as finish.
			result.Decision = &core.Decision{
				Action: "finish",
				Done:   true,
				Reason: result.Summary,
			}
		}
	}

	return result, nil
}

// dispatchMembers dispatches member steps in parallel.
func dispatchMembers(ctx *workflow.WorkflowContext, state *WorkflowState, decision *core.Decision) []core.StepResult {
	results := make([]core.StepResult, 0, len(decision.Steps))
	for i, step := range decision.Steps {
		stepID := fmt.Sprintf("member%d", i)
		prompt := buildMemberPrompt(state, step)

		result := acquireThenSubmit(
			ctx, state,
			stepID, step.Role,
			getExecutor(step.Role), getAccount(step.Role),
			prompt, step.Inputs,
			"", // no affinity for member steps
		)
		results = append(results, result)
	}
	return results
}

// runMemberStep executes a single member step for deterministic strategies.
func runMemberStep(ctx *workflow.WorkflowContext, state *WorkflowState, plan core.StepPlan) (core.StepResult, error) {
	prompt := buildMemberPrompt(state, core.DispatchStep{
		Role:        plan.Role,
		Instruction: plan.Instruction,
		Inputs:      plan.Inputs,
	})

	return acquireThenSubmit(
		ctx, state,
		"step", plan.Role,
		getExecutor(plan.Role), getAccount(plan.Role),
		prompt, plan.Inputs,
		"",
	), nil
}

// runCheck executes the goal-level check.
func runCheck(ctx *workflow.WorkflowContext, state *WorkflowState) (core.StepResult, error) {
	check := state.Spec.Check
	if check == nil {
		return core.StepResult{StepID: "check", Role: "_check", Status: core.StepSucceeded}, nil
	}

	req := RunCheckReq{
		GoalID:    state.GoalID,
		Command:   check.Command,
		Workspace: check.Workspace,
	}

	task := ctx.CallActivity(RunCheck, workflow.WithActivityInput(req))
	var result core.StepResult
	if err := task.Await(&result); err != nil {
		return core.StepResult{StepID: "check", Role: "_check", Status: core.StepFailed}, err
	}
	return result, nil
}

// getExecutor returns the executor for a role (simplified - would read from roles.yaml).
func getExecutor(role string) string {
	return "pi"
}

// getAccount returns the account for a role.
func getAccount(role string) string {
	return "default"
}

// buildBrainPrompt assembles the brain prompt (spec §7.1).
func buildBrainPrompt(state *WorkflowState) string {
	var parts []string
	parts = append(parts, "[role instructions]")
	parts = append(parts, "[team]")
	for _, m := range state.Spec.Members {
		parts = append(parts, m)
	}
	parts = append(parts, "[objective]")
	parts = append(parts, state.Spec.Objective)
	if state.Spec.Acceptance != "" {
		parts = append(parts, "[acceptance]")
		parts = append(parts, state.Spec.Acceptance)
	}
	parts = append(parts, "[progress]")
	parts = append(parts, state.Progress)
	parts = append(parts, "[limits]")
	parts = append(parts, fmt.Sprintf("maxRounds=%d", state.Spec.Limits.MaxRounds))
	return core.JoinInstructions(parts)
}

// buildMemberPrompt assembles a member prompt (spec §7.1).
func buildMemberPrompt(state *WorkflowState, step core.DispatchStep) string {
	var parts []string
	parts = append(parts, "[role instructions]")
	parts = append(parts, "[instruction]")
	parts = append(parts, step.Instruction)
	if len(step.Inputs) > 0 {
		parts = append(parts, "[inputs]")
		for _, input := range step.Inputs {
			parts = append(parts, input)
		}
	}
	parts = append(parts, "[output contract]")
	parts = append(parts, `{"summary": "...", "done": false, "artifacts": []}`)
	return core.JoinInstructions(parts)
}
