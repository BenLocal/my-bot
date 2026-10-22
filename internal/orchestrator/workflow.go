package orchestrator

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dapr/durabletask-go/workflow"

	"github.com/BenLocal/my-bot/internal/core"
)

// WorkflowState is the GoalState of spec §5.2: the input carried between
// rounds via ContinueAsNew. Round is the number of completed rounds.
type WorkflowState struct {
	GoalID       string
	Spec         core.GoalSpec
	Round        int
	Progress     string
	Budget       core.Budget
	FailStreak   int
	NoProgress   int
	LastResults  []core.StepResult // 上一轮成员结果，供大脑下一轮输入
	Artifacts    []string          // 本 Goal 已登记产物引用 r<round>/<stepID>/<name>
	Paused       bool
	PendingNotes []string          // 追加指令 / ask_user 回答，下一轮生效
	LastWorker   map[string]string // role → workerID，会话亲和（§5.1）
	EventSeq     int               // 已分配的事件序号；事件 ID = <goalID>:<seq>

	// PendingSignals is the inbox of signals received but not yet consumed.
	// A slice (not a map) keeps replay deterministic.
	PendingSignals []*Signal
}

// run holds the per-execution (runtime-only) state of one workflow round.
type run struct {
	ctx       *workflow.WorkflowContext
	st        *WorkflowState
	events    []core.Event // 已分配序号、尚未落库的事件
	cancelled bool
	resumed   bool
}

func (r *run) now() time.Time { return r.ctx.CurrentTimeUTC() }

// GoalWorkflow is the Dapr durable workflow for Goal/Task execution: one
// execution = one round, then ContinueAsNew (spec §5.2).
func GoalWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var st WorkflowState
	if err := ctx.GetInput(&st); err != nil {
		return nil, fmt.Errorf("workflow input: %w", err)
	}
	r := &run{ctx: ctx, st: &st}

	// 第 1 步：读空 inbox 并处理控制事件。上一代遗留的 attempt-done / slot-granted
	// 都属于已结束的 Step（轮次之间不持有 lease），直接丢弃。
	r.collectSignals()
	r.applyControls()
	r.st.PendingSignals = nil
	if r.cancelled {
		return nil, r.finish(core.GoalCancelled, core.StopCancelled)
	}
	if st.Paused {
		r.emit("goal.paused", nil)
		r.project(core.GoalPaused, "")
		for st.Paused && !r.cancelled {
			r.wait(-1, nil)
		}
		if r.cancelled {
			return nil, r.finish(core.GoalCancelled, core.StopCancelled)
		}
		r.emit("goal.resumed", nil)
	}

	// 第 2 步：判停。
	if stop, reason := core.CheckStopConditions(st.Spec, r.now(), st.Round, st.FailStreak, st.NoProgress, st.Budget.TokensUsed); stop {
		return nil, r.finish(core.GoalFailed, reason)
	}

	st.Round++
	r.emit("round.started", map[string]any{"round": st.Round})
	r.project(core.GoalRunning, "")

	var out roundOutcome
	if st.Spec.Strategy == "brain" {
		out = r.brainRound()
	} else {
		out = r.deterministicRound()
	}
	if r.cancelled {
		return nil, r.finish(core.GoalCancelled, core.StopCancelled)
	}

	// 第 8 步：汇总本轮。
	if out.status == core.RoundSucceeded {
		st.FailStreak = 0
	} else if out.countFailure {
		st.FailStreak++
	}
	if out.trackProgress {
		newArtifacts := 0
		for _, s := range out.steps {
			newArtifacts += len(s.Artifacts)
		}
		if core.CheckNoProgress(st.Progress, 0, out.progress, newArtifacts) {
			st.NoProgress++
		} else {
			st.NoProgress = 0
		}
	}
	if out.progress != "" {
		st.Progress = truncate(out.progress, 4096)
	}
	if out.members != nil {
		st.LastResults = out.members
	}
	if out.consumedNotes {
		st.PendingNotes = nil
	}

	summary := core.RoundSummary{Round: st.Round, Status: out.status, Summary: out.progress, Steps: out.steps}
	_ = r.call(ActSaveRoundSummary, SaveRoundSummaryReq{GoalID: st.GoalID, Summary: summary}, nil)
	if out.status == core.RoundSucceeded {
		r.emit("round.accepted", map[string]any{"round": st.Round, "summary": out.progress})
	} else {
		r.emit("round.rejected", map[string]any{"round": st.Round, "summary": out.progress})
	}

	switch {
	case out.finished:
		return nil, r.finish(core.GoalSucceeded, core.StopFinished)
	case st.Spec.Kind == core.KindTask:
		// Task = 只有 1 轮、无完成判定的 Goal（spec §4.1）。
		if out.status == core.RoundSucceeded {
			return nil, r.finish(core.GoalSucceeded, core.StopFinished)
		}
		return nil, r.finish(core.GoalFailed, core.StopError)
	}

	r.project(core.GoalRunning, "")
	ctx.ContinueAsNew(st, workflow.WithKeepUnprocessedEvents())
	return nil, nil
}

// roundOutcome is what a round strategy hands back to the round loop.
type roundOutcome struct {
	status        core.RoundStatus
	steps         []core.StepResult
	members       []core.StepResult // nil = keep LastResults unchanged
	progress      string
	finished      bool
	countFailure  bool // failed round counts towards FailStreak
	trackProgress bool // participates in NoProgress
	consumedNotes bool
}

func (r *run) deterministicRound() roundOutcome {
	st := r.st
	strategy := core.NewStrategy(st.Spec.Strategy)
	if strategy == nil {
		return roundOutcome{status: core.RoundFailed, progress: "unknown strategy " + st.Spec.Strategy, countFailure: true}
	}
	sst := strategy.Init(st.Spec)
	for _, res := range st.LastResults {
		sst.Artifacts = append(sst.Artifacts, res.Artifacts...)
	}
	plans := strategy.Next(sst, st.Round)
	steps := make([]*stepRun, 0, len(plans))
	for _, p := range plans {
		ds := core.DispatchStep{ID: p.ID, Role: p.Role, Instruction: p.Instruction, Inputs: p.Inputs}
		steps = append(steps, r.newStep(ds, false, buildMemberPrompt(st, ds)))
	}
	if r.runSteps(steps) {
		return roundOutcome{}
	}
	results := stepResults(steps)
	status := core.AggregateRound(results)
	return roundOutcome{
		status:        status,
		steps:         results,
		members:       results,
		progress:      firstSummary(results),
		finished:      core.DeterministicDone(st.Spec, st.Round, results),
		countFailure:  true,
		trackProgress: true,
		consumedNotes: true,
	}
}

// brainRound: brain step → ParseDecision + validation (2 tries) → finish /
// ask_user / dispatch members (spec §5.2 第 3 步, §5.3).
func (r *run) brainRound() roundOutcome {
	st := r.st
	var all []core.StepResult
	feedback := ""
	var decision core.Decision
	valid := false
	for try := 1; try <= 2 && !valid; try++ {
		stepID := "brain"
		if try > 1 {
			stepID = fmt.Sprintf("brain-%d", try)
		}
		bs := r.newStep(core.DispatchStep{ID: stepID, Role: st.Spec.Brain}, true, buildBrainPrompt(st, feedback))
		if r.runSteps([]*stepRun{bs}) {
			return roundOutcome{}
		}
		res := bs.Result
		if res.Status != core.StepSucceeded {
			all = append(all, res)
			return roundOutcome{status: core.RoundFailed, steps: all, progress: "brain step failed: " + res.Reason, countFailure: true}
		}
		d, err := core.ParseDecision(res.Output)
		if err != nil {
			feedback = "your previous output could not be parsed: " + err.Error()
		} else if vs := core.ValidateDecision(d, r.validationContext()); len(vs) > 0 {
			msgs := make([]string, len(vs))
			for i, v := range vs {
				msgs[i] = v.String()
			}
			feedback = "your previous decision was rejected:\n" + strings.Join(msgs, "\n")
		} else {
			decision = d
			valid = true
		}
		if valid {
			res.Decision = &decision
			res.Summary = decision.Summary
		} else {
			res.Status = core.StepFailed
			res.Reason = "invalid_decision"
			res.Summary = feedback
			r.emit("decision.rejected", map[string]any{"stepID": stepID, "feedback": feedback})
		}
		all = append(all, res)
	}
	if !valid {
		// 连续 2 次解析 / 校验失败计入 FailStreak（spec §5.3）。
		return roundOutcome{status: core.RoundRejected, steps: all, progress: st.Progress, countFailure: true}
	}

	switch decision.Action {
	case "finish":
		if st.Spec.Check != nil {
			check := r.runCheck()
			all = append(all, check)
			if check.Status != core.StepSucceeded {
				return roundOutcome{status: core.RoundFailed, steps: all, members: []core.StepResult{check}, progress: decision.Summary, countFailure: true, consumedNotes: true}
			}
		}
		return roundOutcome{status: core.RoundSucceeded, steps: all, progress: decision.Summary, finished: true, consumedNotes: true}

	case "ask_user":
		return r.askUser(decision, all)
	}

	members := make([]*stepRun, 0, len(decision.Steps))
	for _, ds := range decision.Steps {
		members = append(members, r.newStep(ds, false, buildMemberPrompt(st, ds)))
	}
	if r.runSteps(members) {
		return roundOutcome{}
	}
	results := stepResults(members)
	all = append(all, results...)
	return roundOutcome{
		status:        core.AggregateRound(results),
		steps:         all,
		members:       results,
		progress:      decision.Summary,
		countFailure:  true,
		trackProgress: true,
		consumedNotes: true,
	}
}

// askUser waits for the user's answer (spec §5.3): the goal is waiting, no
// slot is held; timeout → paused(ask_timeout).
func (r *run) askUser(d core.Decision, all []core.StepResult) roundOutcome {
	st := r.st
	reqID := core.AskRequestID(st.GoalID, st.Round, 1)
	timeout := parseDuration(st.Spec.Limits.AskTimeout, defaultAskTimeout)
	ask := &core.AskState{RequestID: reqID, Question: d.Question, ExpiresAt: r.now().Add(timeout)}
	r.emit("goal.waiting", map[string]any{"reason": "ask_user", "requestID": reqID, "question": d.Question})
	r.projectAsk(ask)

	deadline := ask.ExpiresAt
	r.resumed = false
	for !r.cancelled && !r.resumed {
		remaining := deadline.Sub(r.now())
		sig, timedOut := r.wait(remaining, func(s *Signal) bool { return s.Kind == SignalUserInput && s.Key == reqID })
		if sig != nil {
			var in UserInput
			_ = jsonUnmarshal(sig.Data, &in)
			st.PendingNotes = append(st.PendingNotes, "[answer to: "+d.Question+"] "+in.Note)
			r.emit("goal.resumed", map[string]any{"requestID": reqID})
			return roundOutcome{status: core.RoundSucceeded, steps: all, progress: d.Summary}
		}
		if timedOut {
			st.Paused = true
			r.emit("goal.paused", map[string]any{"reason": "ask_timeout", "requestID": reqID})
			return roundOutcome{status: core.RoundSucceeded, steps: all, progress: d.Summary}
		}
	}
	if r.cancelled {
		return roundOutcome{}
	}
	// resume 不带回答：直接进入下一轮。
	r.emit("goal.resumed", map[string]any{"requestID": reqID})
	return roundOutcome{status: core.RoundSucceeded, steps: all, progress: d.Summary}
}

func (r *run) runCheck() core.StepResult {
	var res core.StepResult
	req := RunCheckReq{GoalID: r.st.GoalID, Command: r.st.Spec.Check.Command, Workspace: r.st.Spec.Check.Workspace}
	if err := r.call(ActRunCheck, req, &res); err != nil {
		return core.StepResult{StepID: "check", Role: "_check", Status: core.StepFailed, Reason: err.Error()}
	}
	return res
}

func (r *run) validationContext() core.ValidationContext {
	ws := make([]string, 0, len(r.st.Spec.Workspaces))
	for name := range r.st.Spec.Workspaces {
		ws = append(ws, name)
	}
	sort.Strings(ws)
	mounts := make([]string, 0, len(r.st.Spec.Inputs))
	for name := range r.st.Spec.Inputs {
		mounts = append(mounts, name)
	}
	sort.Strings(mounts)
	return core.ValidationContext{
		MemberRoles:       r.st.Spec.Members,
		ExistingArtifacts: r.st.Artifacts,
		InputMounts:       mounts,
		Workspaces:        ws,
		MaxStepsPerRound:  r.st.Spec.Limits.MaxStepsPerRound,
		MaxTokens:         r.st.Spec.Limits.MaxTokens,
	}
}

// finish releases everything this goal holds and writes the terminal
// projection (spec §5.2 第 9 步 / cancel).
func (r *run) finish(status core.GoalStatus, reason core.StopReason) error {
	r.emit("goal.finished", map[string]any{"status": status, "reason": reason})
	return r.call(ActFinishGoal, FinishGoalReq{
		GoalID: r.st.GoalID,
		State:  r.goalState(status, string(reason)),
		Events: r.takeEvents(),
	}, nil)
}

// emit allocates a deterministic event seq; the event is persisted by the
// next project / finish activity.
func (r *run) emit(typ string, data any) {
	r.st.EventSeq++
	r.events = append(r.events, core.Event{Seq: r.st.EventSeq, Type: typ, At: r.now(), Data: data})
}

func (r *run) takeEvents() []core.Event {
	ev := r.events
	r.events = nil
	return ev
}

func (r *run) goalState(status core.GoalStatus, reason string) core.GoalState {
	gs := core.GoalState{
		ID:        r.st.GoalID,
		Kind:      r.st.Spec.Kind,
		Status:    status,
		Round:     r.st.Round,
		Progress:  core.Progress{Summary: r.st.Progress},
		Budget:    r.st.Budget,
		Reason:    reason,
		UpdatedAt: r.now(),
	}
	gs.Budget.TokensLimit = r.st.Spec.Limits.MaxTokens
	return gs
}

// project writes pending events and the goal projection (spec §6 投影只由 Activity 写).
func (r *run) project(status core.GoalStatus, reason string) {
	r.projectState(r.goalState(status, reason))
}

func (r *run) projectAsk(ask *core.AskState) {
	gs := r.goalState(core.GoalWaiting, "ask_user")
	gs.Ask = ask
	r.projectState(gs)
}

func (r *run) projectState(gs core.GoalState) {
	_ = r.call(ActProject, ProjectReq{GoalID: r.st.GoalID, State: gs, Events: r.takeEvents(), EventSeq: r.st.EventSeq}, nil)
}

// call runs an activity with its per-type retry policy (spec §5.2 表) and
// decodes the result into out (may be nil).
func (r *run) call(name string, in, out any) error {
	opts := []workflow.CallActivityOption{workflow.WithActivityInput(in)}
	if p := activityRetryPolicy(name); p != nil {
		opts = append(opts, workflow.WithActivityRetryPolicy(p))
	}
	return r.ctx.CallActivity(name, opts...).Await(out)
}

func stepResults(steps []*stepRun) []core.StepResult {
	out := make([]core.StepResult, 0, len(steps))
	for _, s := range steps {
		out = append(out, s.Result)
	}
	return out
}

func firstSummary(steps []core.StepResult) string {
	for _, s := range steps {
		if s.Summary != "" {
			return s.Summary
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func jsonUnmarshal(data json.RawMessage, v any) error {
	if len(data) == 0 {
		return fmt.Errorf("empty payload")
	}
	return json.Unmarshal(data, v)
}
