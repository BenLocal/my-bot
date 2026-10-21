package fake

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/executor"
)

// drain 在 timeout 内收集通道事件；通道正常关闭返回 true，超时返回 false。
func drain(ch <-chan executor.Event, timeout time.Duration) ([]executor.Event, bool) {
	var evs []executor.Event
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return evs, true
			}
			evs = append(evs, ev)
		case <-deadline:
			return evs, false
		}
	}
}

func TestFakeScriptable(t *testing.T) {
	ctx := context.Background()

	t.Run("delay and completed", func(t *testing.T) {
		f := NewWithScript(Script{Delay: Duration(50 * time.Millisecond)})
		h, err := f.Start(ctx, executor.StartReq{AttemptKey: "a1"})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		evs, closed := drain(f.Events(h), 2*time.Second)
		if !closed {
			t.Fatal("channel did not close")
		}
		if len(evs) != 1 || evs[0].Type != executor.EventTerminal {
			t.Fatalf("expected 1 terminal event, got %d: %+v", len(evs), evs)
		}
		if evs[0].Phase != executor.PhaseCompleted {
			t.Fatalf("expected completed, got %s", evs[0].Phase)
		}
		if time.Since(start) < 40*time.Millisecond {
			t.Fatalf("delay not honored: elapsed %v", time.Since(start))
		}
	})

	t.Run("result text", func(t *testing.T) {
		result := `{"summary":"ok","done":true,"artifacts":["report.md"]}`
		f := NewWithScript(Script{Result: result})
		h, _ := f.Start(ctx, executor.StartReq{AttemptKey: "a2"})
		evs, _ := drain(f.Events(h), time.Second)
		if len(evs) != 1 {
			t.Fatalf("expected 1 event, got %d", len(evs))
		}
		if evs[0].Text != result {
			t.Fatalf("result text mismatch: %q", evs[0].Text)
		}
	})

	t.Run("fail and retryable", func(t *testing.T) {
		f := NewWithScript(Script{Fail: true, Retryable: true, Reason: "rate_limited"})
		h, _ := f.Start(ctx, executor.StartReq{AttemptKey: "a3"})
		evs, _ := drain(f.Events(h), time.Second)
		if len(evs) != 1 || evs[0].Type != executor.EventTerminal {
			t.Fatalf("expected 1 terminal event, got %d: %+v", len(evs), evs)
		}
		if evs[0].Phase != executor.PhaseFailed || !evs[0].Retryable || evs[0].Reason != "rate_limited" {
			t.Fatalf("unexpected terminal: %+v", evs[0])
		}
		st, _ := f.Status(ctx, h)
		if st.Phase != executor.PhaseFailed || !st.Retryable {
			t.Fatalf("unexpected status: %+v", st)
		}
	})

	t.Run("hang emits no terminal", func(t *testing.T) {
		f := NewWithScript(Script{Hang: true, Delay: Duration(10 * time.Millisecond)})
		h, _ := f.Start(ctx, executor.StartReq{AttemptKey: "a4"})
		evs, closed := drain(f.Events(h), 50*time.Millisecond)
		if closed {
			t.Fatal("channel should not close while hanging")
		}
		for _, ev := range evs {
			if ev.Type == executor.EventTerminal {
				t.Fatal("hang must not emit terminal event")
			}
		}
		st, _ := f.Status(ctx, h)
		if st.Phase != executor.PhaseWorking {
			t.Fatalf("hanging attempt should stay working, got %s", st.Phase)
		}
		if err := f.Cancel(ctx, h); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("repeats callbacks", func(t *testing.T) {
		f := NewWithScript(Script{Repeats: 3, Result: "working"})
		h, _ := f.Start(ctx, executor.StartReq{AttemptKey: "a5"})
		evs, _ := drain(f.Events(h), time.Second)
		var progress int
		var terminal *executor.Event
		for i := range evs {
			switch evs[i].Type {
			case executor.EventProgress:
				progress++
			case executor.EventTerminal:
				terminal = &evs[i]
			}
		}
		if progress != 3 {
			t.Fatalf("expected 3 progress events, got %d", progress)
		}
		if terminal == nil || terminal.Phase != executor.PhaseCompleted {
			t.Fatalf("expected terminal completed, got %+v", terminal)
		}
	})

	t.Run("artifacts", func(t *testing.T) {
		root := t.TempDir()
		content := "hello artifact"
		f := NewWithScript(Script{
			Artifacts: []ArtifactSpec{{Name: "report.md", Content: content}},
		}, WithArtifactRoot(root))
		h, _ := f.Start(ctx, executor.StartReq{AttemptKey: "a6"})
		evs, _ := drain(f.Events(h), time.Second)
		if len(evs) != 2 || evs[0].Type != executor.EventArtifact || evs[1].Type != executor.EventTerminal {
			t.Fatalf("expected artifact then terminal, got %d events: %+v", len(evs), evs)
		}
		art := evs[0].Artifacts[0]
		if art.Name != "report.md" || art.Sha256 != core.HashSHA256([]byte(content)) || art.Size != int64(len(content)) {
			t.Fatalf("unexpected artifact: %+v", art)
		}
		if _, err := os.Stat(art.Path); err != nil {
			t.Fatalf("artifact file missing: %v", err)
		}
		if got, _ := os.ReadFile(art.Path); string(got) != content {
			t.Fatalf("artifact content mismatch")
		}
	})

	t.Run("cancel stops run", func(t *testing.T) {
		f := NewWithScript(Script{Delay: Duration(5 * time.Second)})
		h, _ := f.Start(ctx, executor.StartReq{AttemptKey: "a7"})
		if err := f.Cancel(ctx, h); err != nil {
			t.Fatal(err)
		}
		_, closed := drain(f.Events(h), time.Second)
		if !closed {
			t.Fatal("cancel should close event channel")
		}
	})
}

func TestRecoverFourBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("completed", func(t *testing.T) {
		f := New()
		f.SetRecover("r1", executor.PhaseCompleted, "")
		st, err := f.Recover(ctx, executor.AttemptRecord{AttemptKey: "r1"})
		if err != nil {
			t.Fatal(err)
		}
		if st.Phase != executor.PhaseCompleted {
			t.Fatalf("expected completed, got %s", st.Phase)
		}
	})

	t.Run("resume working", func(t *testing.T) {
		f := New()
		f.SetRecover("r2", executor.PhaseWorking, "")
		st, _ := f.Recover(ctx, executor.AttemptRecord{AttemptKey: "r2"})
		if st.Phase != executor.PhaseWorking {
			t.Fatalf("expected working, got %s", st.Phase)
		}
	})

	t.Run("interrupted tool calls", func(t *testing.T) {
		f := New()
		f.SetRecover("r3", executor.PhaseInterrupted, "tool_call_uncertain")
		st, _ := f.Recover(ctx, executor.AttemptRecord{AttemptKey: "r3"})
		if st.Phase != executor.PhaseInterrupted || st.Reason != "tool_call_uncertain" {
			t.Fatalf("unexpected: %+v", st)
		}
	})

	t.Run("interrupted not found", func(t *testing.T) {
		f := New()
		f.SetRecover("r4", executor.PhaseInterrupted, "session_not_found")
		st, _ := f.Recover(ctx, executor.AttemptRecord{AttemptKey: "r4"})
		if st.Phase != executor.PhaseInterrupted || st.Reason != "session_not_found" {
			t.Fatalf("unexpected: %+v", st)
		}
	})

	t.Run("default interrupted", func(t *testing.T) {
		f := New()
		st, _ := f.Recover(ctx, executor.AttemptRecord{AttemptKey: "unknown"})
		if st.Phase != executor.PhaseInterrupted {
			t.Fatalf("default should be interrupted, got %s", st.Phase)
		}
	})
}

func TestParseScript(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"delay":  "20ms",
		"result": `{"summary":"ok"}`,
	})
	prompt := "[mybot attempt g1/r1/s1/a1]\n[instruction] do it\n\n```fake-script\n" + string(body) + "\n```\n"
	s, err := ParseScript(prompt)
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(s.Delay) != 20*time.Millisecond {
		t.Fatalf("delay mismatch: %v", time.Duration(s.Delay))
	}
	if s.Result != `{"summary":"ok"}` {
		t.Fatalf("result mismatch: %q", s.Result)
	}
}

func TestParseScriptPureJSON(t *testing.T) {
	raw := `{"fail":true,"retryable":true,"reason":"timeout"}`
	s, err := ParseScript(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Fail || !s.Retryable || s.Reason != "timeout" {
		t.Fatalf("unexpected: %+v", s)
	}
}

func TestLoadScriptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fake.json")
	body := map[string]any{"delay": "30ms", "repeats": 2}
	b, _ := json.Marshal(body)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := LoadScriptFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(s.Delay) != 30*time.Millisecond || s.Repeats != 2 {
		t.Fatalf("unexpected: %+v", s)
	}
}
