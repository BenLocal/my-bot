package orchestrator

import (
	"fmt"
	"strings"

	"github.com/BenLocal/my-bot/internal/core"
)

// buildBrainPrompt assembles the brain prompt (spec §7.1 大脑 Step). M0 leaves
// [memory index] / [recalled] empty. feedback carries the previous decision's
// parse / validation errors within the same round (spec §5.3).
func buildBrainPrompt(st *WorkflowState, feedback string) string {
	var b strings.Builder
	section := func(name, body string) {
		fmt.Fprintf(&b, "[%s]\n%s\n\n", name, strings.TrimSpace(body))
	}

	brain := st.Spec.Roles[st.Spec.Brain]
	section("role instructions", brain.InstructionsText+"\n\nOutput your decision as the last JSON code block, matching this schema:\n"+core.DecisionSchemaJSON())

	var team strings.Builder
	for _, m := range st.Spec.Members {
		role := st.Spec.Roles[m]
		fmt.Fprintf(&team, "- %s (executor: %s)\n", m, role.Executor)
	}
	section("team", team.String())

	section("objective", st.Spec.Objective)
	if st.Spec.Acceptance != "" {
		section("acceptance", st.Spec.Acceptance)
	}
	section("progress", st.Progress)

	var last strings.Builder
	for _, r := range st.LastResults {
		fmt.Fprintf(&last, "- %s (%s): %s — %s\n", r.StepID, r.Role, r.Status, r.Summary)
		for _, a := range r.Artifacts {
			fmt.Fprintf(&last, "  artifact: %s (sha256 %s)\n", a.Path, a.Sha256)
		}
	}
	section("last results", last.String())

	if len(st.PendingNotes) > 0 {
		section("notes", strings.Join(st.PendingNotes, "\n"))
	}
	if feedback != "" {
		section("feedback", feedback)
	}

	maxParallel := st.Spec.MaxParallel
	if maxParallel <= 0 {
		maxParallel = defaultParallel
	}
	maxSteps := st.Spec.Limits.MaxStepsPerRound
	if maxSteps <= 0 {
		maxSteps = core.DefaultMaxStepsPerRound
	}
	remaining := "unlimited"
	if st.Spec.Limits.MaxRounds > 0 {
		remaining = fmt.Sprint(st.Spec.Limits.MaxRounds - st.Round + 1)
	}
	section("limits", fmt.Sprintf("round=%d remainingRounds=%s tokensUsed=%d maxTokens=%d maxParallel=%d maxStepsPerRound=%d",
		st.Round, remaining, st.Budget.TokensUsed, st.Spec.Limits.MaxTokens, maxParallel, maxSteps))
	return b.String()
}

// buildMemberPrompt assembles a member step prompt (spec §7.1 成员 Step).
func buildMemberPrompt(st *WorkflowState, step core.DispatchStep) string {
	var b strings.Builder
	section := func(name, body string) {
		fmt.Fprintf(&b, "[%s]\n%s\n\n", name, strings.TrimSpace(body))
	}
	section("role instructions", st.Spec.Roles[step.Role].InstructionsText)
	section("instruction", step.Instruction)
	if len(step.Inputs) > 0 {
		var in strings.Builder
		for _, ref := range step.Inputs {
			fmt.Fprintf(&in, "- /data/artifacts/%s/%s\n", st.GoalID, ref)
		}
		section("inputs", in.String())
	}
	if len(st.PendingNotes) > 0 {
		section("notes", strings.Join(st.PendingNotes, "\n"))
	}
	section("output contract", `Finish with a JSON code block: {"summary": "...", "done": false, "artifacts": ["<file name>", ...]}`)
	return b.String()
}
