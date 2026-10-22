package a2a

import (
	"encoding/json"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/BenLocal/my-bot/internal/core"
)

func TestMetadataRoleStepIDRoundTrip(t *testing.T) {
	m := EncodeMetadata(nil, Metadata{AttemptKey: "k", Role: "dev", StepID: "s1:dev:1"})
	if m[MetaRole] != "dev" || m[MetaStepID] != "s1:dev:1" {
		t.Fatalf("encode: %+v", m)
	}
	got, err := DecodeMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	if got.Role != "dev" || got.StepID != "s1:dev:1" {
		t.Fatalf("decode: %+v", got)
	}
}

func TestDecodeTaskResultFromMetadata(t *testing.T) {
	task := &a2a.Task{
		ID:     "t1",
		Status: a2a.TaskStatus{State: a2a.TaskStateFailed, Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("boom"))},
		Metadata: EncodeResultMeta(nil, core.AttemptInterrupted, "crashed", true,
			core.Usage{InputTokens: 7, OutputTokens: 8}),
		Artifacts: []*a2a.Artifact{
			NewArtifact("g1", 3, "s2", "a.md", "/data/artifacts/g1/r3/s2/a.md", "abc", 12),
			{ID: "x", Parts: []*a2a.Part{a2a.NewTextPart("not mybot")}},
		},
	}
	check := func(t *testing.T, task *a2a.Task) {
		t.Helper()
		r := DecodeTaskResult(task)
		if r.Status != core.AttemptInterrupted || r.Reason != "crashed" || !r.Retryable || r.Text != "boom" {
			t.Fatalf("unexpected: %+v", r)
		}
		if r.Usage != (core.Usage{InputTokens: 7, OutputTokens: 8}) {
			t.Fatalf("usage: %+v", r.Usage)
		}
		want := core.ArtifactRef{Path: "r3/s2/a.md", Sha256: "abc", Size: 12, Round: 3}
		if len(r.Artifacts) != 1 || r.Artifacts[0] != want {
			t.Fatalf("artifacts: %+v", r.Artifacts)
		}
	}
	t.Run("in process", func(t *testing.T) { check(t, task) })
	t.Run("after JSON", func(t *testing.T) {
		b, err := json.Marshal(task)
		if err != nil {
			t.Fatal(err)
		}
		var decoded a2a.Task
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatal(err)
		}
		check(t, &decoded)
	})
}

func TestDecodeTaskResultFallbackState(t *testing.T) {
	cases := []struct {
		state  a2a.TaskState
		want   core.AttemptStatus
		reason string
	}{
		{a2a.TaskStateCompleted, core.AttemptCompleted, ""},
		{a2a.TaskStateFailed, core.AttemptFailed, ""},
		{a2a.TaskStateCanceled, core.AttemptCancelled, ""},
		{a2a.TaskStateRejected, core.AttemptRejected, ""},
		{a2a.TaskStateWorking, core.AttemptWorking, ""},
		{a2a.TaskStateSubmitted, core.AttemptSubmitted, ""},
		{a2a.TaskStateAuthRequired, core.AttemptFailed, "auth_required"},
		{a2a.TaskStateInputRequired, core.AttemptUnknown, ""},
	}
	for _, c := range cases {
		r := DecodeTaskResult(&a2a.Task{Status: a2a.TaskStatus{State: c.state}})
		if r.Status != c.want || r.Reason != c.reason {
			t.Errorf("%s: got %s/%q, want %s/%q", c.state, r.Status, r.Reason, c.want, c.reason)
		}
	}
	if r := DecodeTaskResult(nil); r.Status != core.AttemptUnknown {
		t.Errorf("nil task: %s", r.Status)
	}
}
