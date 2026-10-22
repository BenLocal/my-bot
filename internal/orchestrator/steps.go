package orchestrator

import (
	"fmt"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

// Step phases inside one round (runtime-only, rebuilt on replay).
const (
	phasePending   = "pending"
	phaseAcquiring = "acquiring"
	phaseRunning   = "running"
	phaseBackoff   = "backoff"
	phaseBlocked   = "blocked" // auth_required / unknown：等用户 resume（spec §5.2 第 7 步）
	phaseDone      = "done"
)

const (
	maxStepAttempts   = 3                // 可重试失败的 Step 在本轮内最多尝试 3 次
	pollInterval      = 60 * time.Second // 等 attempt-done 的超时，超时则 Poll 一次
	orphanPollEvery   = 15 * time.Second // Worker 不可达时的 Poll 间隔
	reacquireAfter    = 10 * time.Minute // 10 分钟没拿到 slot-granted 就重新 Acquire（防丢事件）
	defaultOrphan     = 5 * time.Minute
	defaultAskTimeout = 24 * time.Hour
	defaultParallel   = 2
)

// stepRun is the per-step state machine driven by runSteps.
type stepRun struct {
	Spec   core.DispatchStep
	Role   core.Role
	Brain  bool
	Prompt string

	Attempt       int
	Key           string
	Phase         string
	Lease         *LeaseInfo
	SubmittedAt   time.Time
	NextPoll      time.Time
	Deadline      time.Time
	RetryAt       time.Time
	AcquireAt     time.Time
	PollFailSince time.Time
	BlockReason   string

	Result core.StepResult
}

func (r *run) newStep(spec core.DispatchStep, brain bool, prompt string) *stepRun {
	role, ok := r.st.Spec.Roles[spec.Role]
	s := &stepRun{Spec: spec, Role: role, Brain: brain, Prompt: prompt, Attempt: 1, Phase: phasePending}
	s.Result = core.StepResult{StepID: spec.ID, Role: spec.Role}
	if !ok {
		s.Phase = phaseDone
		s.Result.Status = core.StepFailed
		s.Result.Reason = "unknown_role"
	}
	return s
}

// runSteps drives steps until all are done (fan-out ≤ maxParallel, steps that
// share a workspace are serialized). Returns true when the goal was cancelled.
func (r *run) runSteps(steps []*stepRun) bool {
	maxParallel := r.st.Spec.MaxParallel
	if maxParallel <= 0 {
		maxParallel = defaultParallel
	}
	for {
		if r.cancelled {
			r.cancelSteps(steps)
			return true
		}
		now := r.now()
		for _, s := range steps {
			switch s.Phase {
			case phaseRunning:
				if !s.Deadline.IsZero() && !now.Before(s.Deadline) {
					r.timeoutStep(s)
				} else if !now.Before(s.NextPoll) {
					r.pollStep(s)
				}
			case phaseAcquiring:
				if !now.Before(s.AcquireAt.Add(reacquireAfter)) {
					r.acquire(s)
				}
			case phaseBackoff:
				if !now.Before(s.RetryAt) {
					s.Phase = phasePending
				}
			}
		}
		for _, s := range steps {
			if s.Phase == phasePending && countPhases(steps, phaseAcquiring, phaseRunning) < maxParallel && !workspaceBusy(steps, s) {
				r.startStep(s)
			}
		}

		if countPhases(steps, phasePending, phaseAcquiring, phaseRunning, phaseBackoff) == 0 {
			if countPhases(steps, phaseBlocked) == 0 {
				return false
			}
			if r.waitUnblock(steps) {
				r.cancelSteps(steps)
				return true
			}
			continue
		}

		next := nextWake(steps)
		if next.IsZero() {
			// 只剩因并行上限 / 同 workspace 而未启动的 Step：等其他 Step 的信号。
			next = r.now().Add(pollInterval)
		}
		sig, _ := r.wait(next.Sub(r.now()), func(sig *Signal) bool { return stepFor(steps, sig) != nil })
		if sig == nil {
			continue
		}
		s := stepFor(steps, sig)
		switch sig.Kind {
		case SignalSlotGranted:
			var data struct {
				Lease *LeaseInfo `json:"lease"`
			}
			if err := jsonUnmarshal(sig.Data, &data); err == nil && data.Lease != nil {
				r.submit(s, data.Lease)
			}
		case SignalAttemptDone:
			r.pollStep(s)
		}
	}
}

func countPhases(steps []*stepRun, phases ...string) int {
	n := 0
	for _, s := range steps {
		for _, p := range phases {
			if s.Phase == p {
				n++
			}
		}
	}
	return n
}

// workspaceBusy enforces spec §5.3 rule 5: steps sharing a workspace never run in parallel.
func workspaceBusy(steps []*stepRun, s *stepRun) bool {
	if s.Spec.Workspace == "" {
		return false
	}
	for _, o := range steps {
		if o != s && o.Spec.Workspace == s.Spec.Workspace && (o.Phase == phaseAcquiring || o.Phase == phaseRunning) {
			return true
		}
	}
	return false
}

func nextWake(steps []*stepRun) time.Time {
	var next time.Time
	consider := func(t time.Time) {
		if !t.IsZero() && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}
	for _, s := range steps {
		switch s.Phase {
		case phaseRunning:
			consider(s.NextPoll)
			consider(s.Deadline)
		case phaseAcquiring:
			consider(s.AcquireAt.Add(reacquireAfter))
		case phaseBackoff:
			consider(s.RetryAt)
		}
	}
	return next
}

func stepFor(steps []*stepRun, sig *Signal) *stepRun {
	for _, s := range steps {
		if s.Key != sig.Key {
			continue
		}
		if (sig.Kind == SignalSlotGranted && s.Phase == phaseAcquiring) || (sig.Kind == SignalAttemptDone && s.Phase == phaseRunning) {
			return s
		}
	}
	return nil
}

func (r *run) startStep(s *stepRun) {
	s.Key = core.AttemptKey(r.st.GoalID, r.st.Round, s.Spec.ID, s.Attempt)
	s.Lease = nil
	s.PollFailSince = time.Time{}
	r.acquire(s)
}

func (r *run) acquire(s *stepRun) {
	req := AcquireSlotReq{
		WaiterInstanceID: r.st.GoalID,
		AttemptKey:       s.Key,
		Executor:         s.Role.Executor,
		Account:          s.Role.Account,
		PreferWorker:     r.st.LastWorker[s.Spec.Role],
		AffinityWait:     r.st.Spec.Limits.AffinityWait,
	}
	var resp AcquireSlotResp
	if err := r.call(ActAcquireSlot, req, &resp); err != nil {
		// Actor 暂不可用：留在 acquiring，1 分钟后重试。
		s.Phase = phaseAcquiring
		s.AcquireAt = r.now().Add(time.Minute - reacquireAfter)
		return
	}
	if resp.Granted && resp.Lease != nil {
		r.submit(s, resp.Lease)
		return
	}
	if s.Phase != phaseAcquiring {
		r.emit("slot.waiting", map[string]any{"stepID": s.Spec.ID, "role": s.Spec.Role, "attemptKey": s.Key})
		r.project(core.GoalRunning, "")
	}
	s.Phase = phaseAcquiring
	s.AcquireAt = r.now()
}

func (r *run) submit(s *stepRun, lease *LeaseInfo) {
	s.Lease = lease
	req := SubmitAttemptReq{
		GoalID:        r.st.GoalID,
		AttemptKey:    s.Key,
		Round:         r.st.Round,
		StepID:        s.Spec.ID,
		Role:          s.Spec.Role,
		WorkerID:      lease.WorkerID,
		Executor:      lease.Executor,
		Account:       lease.Account,
		Prompt:        attemptMarker(s.Key) + s.Prompt,
		SessionPolicy: lease.SessionPolicy,
		Workspace:     s.Spec.Workspace,
		Inputs:        artifactURIs(r.st.GoalID, s.Spec.Inputs),
	}
	var resp SubmitAttemptResp
	if err := r.call(ActSubmitAttempt, req, &resp); err != nil {
		r.release(s)
		r.afterFailure(s, core.AttemptFailed, "submit_failed", true)
		return
	}
	now := r.now()
	s.Phase = phaseRunning
	s.SubmittedAt = now
	s.NextPoll = now.Add(pollInterval)
	s.Deadline = now.Add(s.Role.TimeoutDuration())
	if r.st.LastWorker == nil {
		r.st.LastWorker = map[string]string{}
	}
	r.st.LastWorker[s.Spec.Role] = lease.WorkerID
	r.emit("attempt.submitted", map[string]any{
		"stepID": s.Spec.ID, "role": s.Spec.Role, "attemptKey": s.Key,
		"workerID": lease.WorkerID, "sessionPolicy": lease.SessionPolicy,
	})
	r.project(core.GoalRunning, "")
}

func (r *run) pollStep(s *stepRun) {
	var resp PollAttemptResp
	err := r.call(ActPollAttempt, PollAttemptReq{
		GoalID:     r.st.GoalID,
		AttemptKey: s.Key,
		WorkerID:   s.Lease.WorkerID,
		Executor:   s.Lease.Executor,
		Round:      r.st.Round,
	}, &resp)
	now := r.now()
	if err != nil || !resp.Reachable {
		if s.PollFailSince.IsZero() {
			s.PollFailSince = now
		}
		orphanAfter := parseDuration(r.st.Spec.Limits.OrphanAfter, defaultOrphan)
		if err == nil && !resp.WorkerAlive && now.Sub(s.PollFailSince) >= orphanAfter {
			// spec §5.2 第 6 步：Worker 不可达且注册已过期超过 orphanAfter。
			r.release(s)
			r.afterFailure(s, core.AttemptInterrupted, "worker_lost", true)
			return
		}
		s.NextPoll = now.Add(orphanPollEvery)
		return
	}
	s.PollFailSince = time.Time{}
	if !isTerminalAttempt(resp.Status) {
		s.NextPoll = now.Add(pollInterval)
		return
	}
	r.finishAttempt(s, resp)
}

func (r *run) finishAttempt(s *stepRun, resp PollAttemptResp) {
	r.release(s)
	r.st.Budget.TokensUsed += resp.Usage.InputTokens + resp.Usage.OutputTokens
	s.Result.Usage.InputTokens += resp.Usage.InputTokens
	s.Result.Usage.OutputTokens += resp.Usage.OutputTokens

	switch resp.Status {
	case core.AttemptCompleted:
		r.collect(s, resp)
	case core.AttemptCancelled:
		r.done(s, core.StepCancelled, "cancelled")
	case core.AttemptRejected:
		r.done(s, core.StepFailed, firstNonEmpty(resp.Reason, "rejected"))
	case core.AttemptUnknown:
		r.block(s, "unknown_outcome")
	case core.AttemptInterrupted:
		r.afterFailure(s, resp.Status, firstNonEmpty(resp.Reason, "interrupted"), true)
	default:
		r.afterFailure(s, resp.Status, resp.Reason, resp.Retryable)
	}
}

// afterFailure applies the step-level retry rules (spec §5.1 限流 / 认证失效, §5.2 第 7 步).
func (r *run) afterFailure(s *stepRun, status core.AttemptStatus, reason string, retryable bool) {
	r.emit("attempt.finished", map[string]any{"stepID": s.Spec.ID, "attemptKey": s.Key, "status": status, "reason": reason})
	account := s.Role.Executor + "/" + s.Role.Account
	switch reason {
	case "auth_required":
		_ = r.call(ActAccountState, AccountStateReq{Account: account, NeedsAuth: true}, nil)
		r.emit("account.needs_auth", map[string]any{"account": account})
		r.block(s, reason)
		return
	case "rate_limited":
		_ = r.call(ActAccountState, AccountStateReq{Account: account, CooldownUntil: r.now().Add(time.Minute)}, nil)
		r.emit("account.cooldown", map[string]any{"account": account})
		retryable = true
	}
	if retryable && s.Attempt < maxStepAttempts {
		s.Phase = phaseBackoff
		s.RetryAt = r.now().Add(time.Duration(5<<uint(s.Attempt-1)) * time.Second)
		s.Attempt++
		return
	}
	r.done(s, core.StepFailed, reason)
}

func (r *run) collect(s *stepRun, resp PollAttemptResp) {
	var oc core.OutputContract
	if !s.Brain {
		var err error
		if oc, err = core.ParseOutputContract(resp.Text); err != nil {
			// spec §7.4 第 1 条：没有合法 output contract 则本 Step 不通过。
			s.Result.Output = resp.Text
			r.done(s, core.StepFailed, "bad_output_contract")
			return
		}
	}
	if len(resp.Artifacts) > 0 {
		if err := r.call(ActCollectStep, CollectStepReq{GoalID: r.st.GoalID, Round: r.st.Round, StepID: s.Spec.ID, AttemptKey: s.Key, Artifacts: resp.Artifacts}, nil); err != nil {
			r.done(s, core.StepFailed, "collect_failed")
			return
		}
	}
	s.Result.Output = resp.Text
	s.Result.Summary = oc.Summary
	s.Result.Done = oc.Done
	s.Result.Artifacts = resp.Artifacts
	for _, a := range resp.Artifacts {
		r.st.Artifacts = append(r.st.Artifacts, a.Path)
	}
	r.done(s, core.StepSucceeded, "")
}

func (r *run) done(s *stepRun, status core.StepStatus, reason string) {
	s.Phase = phaseDone
	s.Result.Status = status
	s.Result.Reason = reason
	s.Result.Attempt = s.Attempt
	if s.Lease != nil {
		s.Result.WorkerID = s.Lease.WorkerID
	}
	if status == core.StepSucceeded {
		r.emit("attempt.finished", map[string]any{"stepID": s.Spec.ID, "attemptKey": s.Key, "status": core.AttemptCompleted, "artifacts": len(s.Result.Artifacts)})
	}
	r.project(core.GoalRunning, "")
}

func (r *run) block(s *stepRun, reason string) {
	s.Phase = phaseBlocked
	s.BlockReason = reason
}

// waitUnblock puts the goal into waiting until the user resumes (true = cancelled).
func (r *run) waitUnblock(steps []*stepRun) bool {
	reason := ""
	for _, s := range steps {
		if s.Phase == phaseBlocked {
			reason = s.BlockReason
			break
		}
	}
	r.emit("goal.waiting", map[string]any{"reason": reason})
	r.project(core.GoalWaiting, reason)
	r.resumed = false
	for !r.resumed && !r.cancelled {
		r.wait(-1, nil)
	}
	if r.cancelled {
		return true
	}
	r.emit("goal.resumed", nil)
	r.project(core.GoalRunning, "")
	for _, s := range steps {
		if s.Phase == phaseBlocked {
			s.Phase = phasePending
			s.Attempt++
		}
	}
	return false
}

func (r *run) timeoutStep(s *stepRun) {
	_ = r.call(ActCancelAttempt, CancelAttemptReq{AttemptKey: s.Key, WorkerID: s.Lease.WorkerID, Executor: s.Lease.Executor}, nil)
	r.release(s)
	r.emit("attempt.finished", map[string]any{"stepID": s.Spec.ID, "attemptKey": s.Key, "status": core.AttemptFailed, "reason": "timeout"})
	r.done(s, core.StepFailed, "timeout")
}

// cancelSteps cancels running attempts and releases every lease / queued
// request of this goal (spec §5.2 cancel).
func (r *run) cancelSteps(steps []*stepRun) {
	for _, s := range steps {
		if s.Phase == phaseRunning && s.Lease != nil {
			_ = r.call(ActCancelAttempt, CancelAttemptReq{AttemptKey: s.Key, WorkerID: s.Lease.WorkerID, Executor: s.Lease.Executor}, nil)
		}
		if s.Phase != phaseDone {
			s.Phase = phaseDone
			s.Result.Status = core.StepCancelled
		}
	}
	_ = r.call(ActReleaseWaiter, ReleaseWaiterReq{InstanceID: r.st.GoalID}, nil)
}

func (r *run) release(s *stepRun) {
	_ = r.call(ActReleaseSlot, ReleaseSlotReq{AttemptKey: s.Key}, nil)
}

func isTerminalAttempt(st core.AttemptStatus) bool {
	switch st {
	case core.AttemptCompleted, core.AttemptFailed, core.AttemptRejected, core.AttemptCancelled, core.AttemptInterrupted, core.AttemptUnknown:
		return true
	}
	return false
}

// attemptMarker is the first prompt line used by Recover (spec §7.2).
func attemptMarker(key string) string {
	return fmt.Sprintf("[mybot attempt %s]\n", key)
}

// artifactURIs maps r<round>/<stepID>/<name> refs to mybot-artifact URIs (spec §7.1).
func artifactURIs(goalID string, refs []string) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, "mybot-artifact://"+goalID+"/"+ref)
	}
	return out
}

func parseDuration(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	return def
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
