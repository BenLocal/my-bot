package orchestrator

import (
	"encoding/json"
	"time"
)

// Signal kinds for the inbox pattern (spec §5.2「等待规则与 inbox」).
const (
	SignalAttemptDone = "attempt-done"
	SignalSlotGranted = "slot-granted"
	SignalControl     = "control"
	SignalUserInput   = "user-input"
)

// signalEventName is the single external event name used for every signal.
const signalEventName = "signal"

// Signal is the envelope for all external events.
type Signal struct {
	Kind string          `json:"kind"`
	Key  string          `json:"key,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// ControlSignal carries pause/resume/cancel/append.
type ControlSignal struct {
	Action string `json:"action"`
	Note   string `json:"note,omitempty"`
}

// UserInput is the payload of a user-input signal (answer to ask_user).
type UserInput struct {
	Note string `json:"note"`
}

// collectSignals non-blockingly moves every already-delivered signal into the
// inbox: WaitForExternalEvent(name, 0) returns immediately with an error when
// nothing is buffered (P0-2).
func (r *run) collectSignals() {
	for {
		var sig Signal
		if err := r.ctx.WaitForExternalEvent(signalEventName, 0).Await(&sig); err != nil {
			return
		}
		r.st.PendingSignals = append(r.st.PendingSignals, &sig)
	}
}

// applyControls consumes every control signal in the inbox, keeping the
// relative order of the others.
func (r *run) applyControls() {
	kept := r.st.PendingSignals[:0]
	for _, sig := range r.st.PendingSignals {
		if sig.Kind == SignalControl {
			r.applyControl(sig)
			continue
		}
		kept = append(kept, sig)
	}
	r.st.PendingSignals = kept
}

func (r *run) applyControl(sig *Signal) {
	var c ControlSignal
	if err := json.Unmarshal(sig.Data, &c); err != nil {
		return
	}
	switch c.Action {
	case "cancel":
		r.cancelled = true
	case "pause":
		r.st.Paused = true
	case "resume":
		// resume 只在 paused / waiting 时有意义，其余情况无副作用（spec §5.2）。
		r.st.Paused = false
		r.resumed = true
	case "append":
		if c.Note != "" {
			r.st.PendingNotes = append(r.st.PendingNotes, c.Note)
		}
	}
}

// wait is the only blocking primitive: every wait point (slot, attempt,
// ask_user, paused, waiting) goes through it. It returns
//   - (sig, false) when a signal accepted by match arrives (removed from the inbox);
//   - (nil, false) right after a control signal was applied, so the caller can
//     re-check r.cancelled / r.st.Paused / r.resumed;
//   - (nil, true) when the timeout elapsed. timeout < 0 waits indefinitely.
//
// Signals that are neither controls nor matched stay in the inbox.
func (r *run) wait(timeout time.Duration, match func(*Signal) bool) (*Signal, bool) {
	var deadline time.Time
	if timeout >= 0 {
		deadline = r.now().Add(timeout)
	}
	for {
		applied := false
		kept := r.st.PendingSignals[:0]
		var found *Signal
		for _, sig := range r.st.PendingSignals {
			switch {
			case sig.Kind == SignalControl:
				r.applyControl(sig)
				applied = true
			case found == nil && match != nil && match(sig):
				found = sig
			default:
				kept = append(kept, sig)
			}
		}
		r.st.PendingSignals = kept
		if found != nil {
			return found, false
		}
		if applied {
			return nil, false
		}

		wait := time.Duration(-1)
		if timeout >= 0 {
			wait = deadline.Sub(r.now())
			if wait <= 0 {
				return nil, true
			}
		}
		var sig Signal
		if err := r.ctx.WaitForExternalEvent(signalEventName, wait).Await(&sig); err != nil {
			return nil, true
		}
		r.st.PendingSignals = append(r.st.PendingSignals, &sig)
	}
}
