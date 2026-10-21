package core

import "fmt"

// transitionDef defines a legal transition.
type transitionDef struct {
	From  string // from status (as string for generic matching)
	To    string
	Event string
}

var goalTransitionDefs = []transitionDef{
	{"queued", "running", "start"},
	{"running", "succeeded", "finish"},
	{"running", "failed", "fail"},
	{"running", "paused", "pause"},
	{"paused", "running", "resume"},
	{"running", "waiting", "waiting"},
	{"waiting", "running", "resume"},
	{"running", "cancelled", "cancel"},
	{"paused", "cancelled", "cancel"},
	{"waiting", "cancelled", "cancel"},
	{"running", "completed", "complete"},
}

// GoalTransition validates and returns the target status for a Goal state transition.
func GoalTransition(from GoalStatus, event string) (GoalStatus, error) {
	for _, t := range goalTransitionDefs {
		if string(from) == t.From && event == t.Event {
			return GoalStatus(t.To), nil
		}
	}
	return "", fmt.Errorf("illegal goal transition: %s + event %q", from, event)
}

var stepTransitionDefs = []transitionDef{
	{"pending", "running", "start"},
	{"running", "succeeded", "succeed"},
	{"running", "failed", "fail"},
	{"running", "cancelled", "cancel"},
	{"pending", "cancelled", "cancel"},
}

// StepTransition validates and returns the target status for a Step state transition.
func StepTransition(from StepStatus, event string) (StepStatus, error) {
	for _, t := range stepTransitionDefs {
		if string(from) == t.From && event == t.Event {
			return StepStatus(t.To), nil
		}
	}
	return "", fmt.Errorf("illegal step transition: %s + event %q", from, event)
}

var attemptTransitionDefs = []transitionDef{
	{"submitting", "submitted", "submitted"},
	{"submitted", "working", "working"},
	{"working", "completed", "completed"},
	{"working", "failed", "failed"},
	{"submitting", "rejected", "rejected"},
	{"submitted", "rejected", "rejected"},
	{"working", "cancelled", "cancelled"},
	{"submitted", "cancelled", "cancelled"},
	{"working", "interrupted", "interrupted"},
	{"submitting", "interrupted", "interrupted"},
	{"submitted", "interrupted", "interrupted"},
	{"submitting", "unknown", "unknown"},
}

// AttemptTransition validates and returns the target status for an Attempt state transition.
func AttemptTransition(from AttemptStatus, event string) (AttemptStatus, error) {
	for _, t := range attemptTransitionDefs {
		if string(from) == t.From && event == t.Event {
			return AttemptStatus(t.To), nil
		}
	}
	return "", fmt.Errorf("illegal attempt transition: %s + event %q", from, event)
}
