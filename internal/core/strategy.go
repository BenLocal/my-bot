package core

import (
	"fmt"
	"sort"
	"strings"
)

// Strategy defines the interface for deterministic (non-brain) strategies.
type Strategy interface {
	Init(spec GoalSpec) StrategyState
	Next(st StrategyState, round int) []StepPlan
	Absorb(st StrategyState, results []StepResult) StrategyState
	ShouldContinue(st StrategyState) bool
	Finalize(st StrategyState) RoundSummary
}

// StrategyState holds per-goal strategy state. Strategies are rebuilt every
// round (the workflow ContinueAsNew-s per round), so all state is derived from
// the round number and the previous round's artifacts.
type StrategyState struct {
	Round     int
	StepCount int
	Artifacts []ArtifactRef // 上一轮产物，pipeline 传给下一个角色
}

// StepPlan describes a step to be executed in the current round.
type StepPlan struct {
	ID          string
	Role        string
	Instruction string
	Inputs      []string
}

// NewStrategy creates a Strategy based on the strategy name.
func NewStrategy(name string) Strategy {
	switch name {
	case "single":
		return &SingleStrategy{}
	case "pipeline":
		return &PipelineStrategy{}
	case "round_robin":
		return &RoundRobinStrategy{}
	default:
		return nil
	}
}

// goalInstruction is the member instruction for deterministic strategies: the
// Loop instruction when set, otherwise the Goal objective (spec §7.1).
func goalInstruction(spec GoalSpec) string {
	if spec.Instruction != "" {
		return spec.Instruction
	}
	return spec.Objective
}

func artifactInputs(arts []ArtifactRef) []string {
	inputs := make([]string, 0, len(arts))
	for _, a := range arts {
		inputs = append(inputs, a.Path)
	}
	return inputs
}

// -------- Single --------

type SingleStrategy struct {
	spec GoalSpec
}

func (s *SingleStrategy) Init(spec GoalSpec) StrategyState {
	s.spec = spec
	return StrategyState{}
}

func (s *SingleStrategy) Next(st StrategyState, round int) []StepPlan {
	// single strategy: one step per round targeting the first member (or the brain role)
	role := s.spec.Brain
	if len(s.spec.Members) > 0 {
		role = s.spec.Members[0]
	}
	return []StepPlan{{ID: "main", Role: role, Instruction: goalInstruction(s.spec), Inputs: artifactInputs(st.Artifacts)}}
}

func (s *SingleStrategy) Absorb(st StrategyState, results []StepResult) StrategyState {
	st.StepCount++
	return st
}

func (s *SingleStrategy) ShouldContinue(st StrategyState) bool {
	return st.StepCount == 0
}

func (s *SingleStrategy) Finalize(st StrategyState) RoundSummary {
	return RoundSummary{Round: st.Round, Status: RoundSucceeded, Summary: "single strategy complete"}
}

// -------- Pipeline --------

// PipelineStrategy runs members[round-1] in round `round`, passing the
// previous round's artifacts as inputs.
type PipelineStrategy struct {
	spec GoalSpec
}

func (s *PipelineStrategy) Init(spec GoalSpec) StrategyState {
	s.spec = spec
	return StrategyState{}
}

func (s *PipelineStrategy) Next(st StrategyState, round int) []StepPlan {
	idx := round - 1
	if idx < 0 || idx >= len(s.spec.Members) {
		return []StepPlan{}
	}
	return []StepPlan{{
		ID:          fmt.Sprintf("stage%d", idx+1),
		Role:        s.spec.Members[idx],
		Instruction: goalInstruction(s.spec),
		Inputs:      artifactInputs(st.Artifacts),
	}}
}

func (s *PipelineStrategy) Absorb(st StrategyState, results []StepResult) StrategyState {
	st.StepCount++
	st.Round++
	for _, r := range results {
		st.Artifacts = append(st.Artifacts, r.Artifacts...)
	}
	return st
}

func (s *PipelineStrategy) ShouldContinue(st StrategyState) bool {
	return st.Round < len(s.spec.Members)
}

func (s *PipelineStrategy) Finalize(st StrategyState) RoundSummary {
	return RoundSummary{Round: st.Round, Status: RoundSucceeded, Summary: "pipeline strategy complete"}
}

// -------- Round Robin --------

// RoundRobinStrategy sorts members and picks members[(round-1)%n] (spec §5.0).
type RoundRobinStrategy struct {
	spec GoalSpec
}

func (s *RoundRobinStrategy) Init(spec GoalSpec) StrategyState {
	s.spec = spec
	s.spec.Members = SortMembers(append([]string(nil), spec.Members...))
	return StrategyState{}
}

func (s *RoundRobinStrategy) Next(st StrategyState, round int) []StepPlan {
	if len(s.spec.Members) == 0 || round < 1 {
		return []StepPlan{}
	}
	role := s.spec.Members[(round-1)%len(s.spec.Members)]
	return []StepPlan{{ID: "main", Role: role, Instruction: goalInstruction(s.spec), Inputs: artifactInputs(st.Artifacts)}}
}

func (s *RoundRobinStrategy) Absorb(st StrategyState, results []StepResult) StrategyState {
	st.StepCount++
	return st
}

func (s *RoundRobinStrategy) ShouldContinue(st StrategyState) bool {
	return true
}

func (s *RoundRobinStrategy) Finalize(st StrategyState) RoundSummary {
	return RoundSummary{Round: st.Round, Status: RoundSucceeded, Summary: "round_robin strategy complete"}
}

// DeterministicDone reports whether a deterministic-strategy goal is complete
// after `round`: the round must have succeeded, and either a member reported
// done=true or (pipeline) the last stage has run (spec §7.4).
func DeterministicDone(spec GoalSpec, round int, steps []StepResult) bool {
	if AggregateRound(steps) != RoundSucceeded {
		return false
	}
	if spec.Strategy == "pipeline" && round >= len(spec.Members) {
		return true
	}
	for _, s := range steps {
		if s.Done {
			return true
		}
	}
	return false
}

// SortMembers sorts member names for deterministic ordering.
func SortMembers(members []string) []string {
	sort.Strings(members)
	return members
}

// JoinInstructions joins instruction parts with newlines.
func JoinInstructions(parts []string) string {
	return strings.Join(parts, "\n")
}
