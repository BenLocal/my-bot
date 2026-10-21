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

// StrategyState holds per-goal strategy state.
type StrategyState struct {
	Round     int
	StepCount int
	Artifacts []ArtifactRef
}

// StepPlan describes a step to be executed in the current round.
type StepPlan struct {
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

// -------- Single --------

type SingleStrategy struct {
	spec  GoalSpec
	state StrategyState
}

func (s *SingleStrategy) Init(spec GoalSpec) StrategyState {
	s.spec = spec
	s.state = StrategyState{Round: 0, StepCount: 0}
	return s.state
}

func (s *SingleStrategy) Next(st StrategyState, round int) []StepPlan {
	// single strategy: one step per round targeting the brain or first member
	role := s.spec.Brain
	if role == "" && len(s.spec.Members) > 0 {
		role = s.spec.Members[0]
	}
	return []StepPlan{
		{
			Role:        role,
			Instruction: "Execute the goal objective",
		},
	}
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

type PipelineStrategy struct {
	spec  GoalSpec
	state StrategyState
	index int
}

func (s *PipelineStrategy) Init(spec GoalSpec) StrategyState {
	s.spec = spec
	s.state = StrategyState{Round: 0, StepCount: 0}
	s.index = 0
	return s.state
}

func (s *PipelineStrategy) Next(st StrategyState, round int) []StepPlan {
	if s.index >= len(s.spec.Members) {
		return []StepPlan{}
	}
	role := s.spec.Members[s.index]
	instruction := fmt.Sprintf("Pipeline step %d: %s", s.index, "Execute this step")
	// Pass previous artifacts as inputs
	inputs := make([]string, 0)
	for _, a := range st.Artifacts {
		inputs = append(inputs, "mybot-artifact://"+a.Path)
	}
	return []StepPlan{
		{
			Role:        role,
			Instruction: instruction,
			Inputs:      inputs,
		},
	}
}

func (s *PipelineStrategy) Absorb(st StrategyState, results []StepResult) StrategyState {
	st.StepCount++
	for _, r := range results {
		st.Artifacts = append(st.Artifacts, r.Artifacts...)
	}
	s.index++
	return st
}

func (s *PipelineStrategy) ShouldContinue(st StrategyState) bool {
	return s.index < len(s.spec.Members)
}

func (s *PipelineStrategy) Finalize(st StrategyState) RoundSummary {
	return RoundSummary{Round: st.Round, Status: RoundSucceeded, Summary: "pipeline strategy complete"}
}

// -------- Round Robin --------

type RoundRobinStrategy struct {
	spec   GoalSpec
	state  StrategyState
	cursor int
}

func (s *RoundRobinStrategy) Init(spec GoalSpec) StrategyState {
	s.spec = spec
	members := SortMembers(spec.Members)
	s.spec.Members = members
	s.state = StrategyState{Round: 0, StepCount: 0}
	s.cursor = 0
	return s.state
}

func (s *RoundRobinStrategy) Next(st StrategyState, round int) []StepPlan {
	if len(s.spec.Members) == 0 {
		return []StepPlan{}
	}
	idx := s.cursor % len(s.spec.Members)
	role := s.spec.Members[idx]
	s.cursor++
	return []StepPlan{
		{
			Role:        role,
			Instruction: fmt.Sprintf("Round %d: Execute for %s", round, role),
		},
	}
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

// SortMembers sorts member names for deterministic ordering.
func SortMembers(members []string) []string {
	sort.Strings(members)
	return members
}

// JoinInstructions joins instruction parts with newlines.
func JoinInstructions(parts []string) string {
	return strings.Join(parts, "\n")
}
