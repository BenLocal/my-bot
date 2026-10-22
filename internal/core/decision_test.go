package core

import (
	"testing"
)

func TestParseDecisionPureJSON(t *testing.T) {
	raw := `{"action":"finish","done":true,"summary":"completed"}`
	d, err := ParseDecision(raw)
	if err != nil {
		t.Fatalf("ParseDecision: %v", err)
	}
	if d.Action != "finish" || !d.Done {
		t.Errorf("expected finish with done=true, got %+v", d)
	}
}

func TestParseDecisionFenced(t *testing.T) {
	raw := "Here is my decision:\n```json\n{\"action\":\"dispatch\",\"steps\":[{\"role\":\"coder\",\"instruction\":\"write code\"}]}\n```"
	d, err := ParseDecision(raw)
	if err != nil {
		t.Fatalf("ParseDecision: %v", err)
	}
	if d.Action != "dispatch" {
		t.Errorf("expected dispatch, got %s", d.Action)
	}
	if len(d.Steps) != 1 {
		t.Errorf("expected 1 step, got %d", len(d.Steps))
	}
}

func TestParseDecisionMultipleBlocks(t *testing.T) {
	raw := "First attempt:\n```json\n{\"action\":\"ask_user\",\"question\":\"which?\"}\n```\nSecond:\n```json\n{\"action\":\"dispatch\",\"steps\":[{\"role\":\"coder\",\"instruction\":\"do it\"}]}\n```"
	d, err := ParseDecision(raw)
	if err != nil {
		t.Fatalf("ParseDecision: %v", err)
	}
	// Should take the LAST block
	if d.Action != "dispatch" {
		t.Errorf("expected last block (dispatch), got %s", d.Action)
	}
}

func TestParseDecisionGarbage(t *testing.T) {
	raw := "I don't know what to do"
	_, err := ParseDecision(raw)
	if err == nil {
		t.Error("expected error for garbage input")
	}
}

func TestValidateDecisionDispatch(t *testing.T) {
	d := Decision{
		Action: "dispatch",
		Steps: []DispatchStep{
			{ID: "impl", Role: "coder", Instruction: "write code"},
			{ID: "review", Role: "reviewer", Instruction: "review"},
		},
	}
	ctx := ValidationContext{
		MemberRoles:      []string{"coder", "reviewer", "designer"},
		MaxStepsPerRound: 4,
	}
	violations := ValidateDecision(d, ctx)
	if len(violations) > 0 {
		t.Errorf("expected no violations, got: %v", violations)
	}
}

func TestValidateDecisionNonMember(t *testing.T) {
	d := Decision{
		Action: "dispatch",
		Steps: []DispatchStep{
			{ID: "x", Role: "nonexistent", Instruction: "do something"},
		},
	}
	ctx := ValidationContext{
		MemberRoles: []string{"coder", "reviewer"},
	}
	violations := ValidateDecision(d, ctx)
	if len(violations) == 0 {
		t.Error("expected violation for non-member role")
	}
}

func TestValidateDecisionTooManySteps(t *testing.T) {
	d := Decision{
		Action: "dispatch",
		Steps: []DispatchStep{
			{ID: "s1", Role: "coder", Instruction: "1"},
			{ID: "s2", Role: "coder", Instruction: "2"},
			{ID: "s3", Role: "coder", Instruction: "3"},
			{ID: "s4", Role: "coder", Instruction: "4"},
			{ID: "s5", Role: "coder", Instruction: "5"},
		},
	}
	ctx := ValidationContext{
		MemberRoles:      []string{"coder"},
		MaxStepsPerRound: 4,
	}
	violations := ValidateDecision(d, ctx)
	if len(violations) == 0 {
		t.Error("expected violation for too many steps")
	}
}

func TestValidateDecisionFinishDone(t *testing.T) {
	d := Decision{Action: "finish", Done: false}
	ctx := ValidationContext{}
	violations := ValidateDecision(d, ctx)
	if len(violations) == 0 {
		t.Error("expected violation for finish without done=true")
	}
}

func TestValidateDecisionInvalidAction(t *testing.T) {
	d := Decision{Action: "invalid"}
	ctx := ValidationContext{}
	violations := ValidateDecision(d, ctx)
	if len(violations) == 0 {
		t.Error("expected violation for invalid action")
	}
}

func TestValidateDecisionMemoryTopic(t *testing.T) {
	d := Decision{
		Action: "finish",
		Done:   true,
		Memories: []MemoryEntry{
			{Topic: "Invalid Topic!", Text: "test", Kind: "fact"},
		},
	}
	ctx := ValidationContext{}
	violations := ValidateDecision(d, ctx)
	if len(violations) == 0 {
		t.Error("expected violation for invalid topic")
	}
}

func TestValidateDecisionMemoryCredential(t *testing.T) {
	d := Decision{
		Action: "finish",
		Done:   true,
		Memories: []MemoryEntry{
			{Topic: "secrets", Text: "sk-abcdefghijklmnopqrstuvwxyz1234567890", Kind: "fact"},
		},
	}
	ctx := ValidationContext{}
	violations := ValidateDecision(d, ctx)
	if len(violations) == 0 {
		t.Error("expected violation for credential in memory")
	}
}

func TestValidateDecisionMemoryTooLong(t *testing.T) {
	longText := ""
	for i := 0; i < 350; i++ {
		longText += "a"
	}
	d := Decision{
		Action: "finish",
		Done:   true,
		Memories: []MemoryEntry{
			{Topic: "test", Text: longText, Kind: "fact"},
		},
	}
	ctx := ValidationContext{}
	violations := ValidateDecision(d, ctx)
	if len(violations) == 0 {
		t.Error("expected violation for too long memory text")
	}
}

func TestDecisionSchema(t *testing.T) {
	schema := DecisionSchema()
	if schema == nil {
		t.Fatal("DecisionSchema returned nil")
	}
	if schema["type"] != "object" {
		t.Errorf("expected type=object, got %v", schema["type"])
	}
	props := schema["properties"].(map[string]interface{})
	if props["action"] == nil {
		t.Error("expected action property")
	}
}

func TestValidateDecisionStepRules(t *testing.T) {
	ctx := ValidationContext{
		MemberRoles:       []string{"coder", "reviewer"},
		ExistingArtifacts: []string{"r1/design/design.md"},
		Workspaces:        []string{"repo"},
	}
	tests := []struct {
		name string
		d    Decision
		rule string
	}{
		{"bad id", Decision{Action: "dispatch", Steps: []DispatchStep{{ID: "Bad_ID", Role: "coder", Instruction: "x"}}}, "R2"},
		{"dup id", Decision{Action: "dispatch", Steps: []DispatchStep{{ID: "a", Role: "coder", Instruction: "x"}, {ID: "a", Role: "reviewer", Instruction: "y"}}}, "R2"},
		{"missing artifact", Decision{Action: "dispatch", Steps: []DispatchStep{{ID: "a", Role: "coder", Instruction: "x", Inputs: []string{"r1/design/nope.md"}}}}, "R3"},
		{"bad workspace", Decision{Action: "dispatch", Steps: []DispatchStep{{ID: "a", Role: "coder", Instruction: "x", Workspace: "../etc"}}}, "R3"},
		{"self brain", Decision{Action: "dispatch", Steps: []DispatchStep{{ID: "a", Role: "planner", Instruction: "x"}}}, "R1"},
		{"case sensitive role", Decision{Action: "dispatch", Steps: []DispatchStep{{ID: "a", Role: "Coder", Instruction: "x"}}}, "R1"},
		{"ask without question", Decision{Action: "ask_user"}, "R0"},
		{"empty dispatch", Decision{Action: "dispatch"}, "R2"},
	}
	for _, tt := range tests {
		vs := ValidateDecision(tt.d, ctx)
		found := false
		for _, v := range vs {
			if v.Rule == tt.rule {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: expected rule %s violation, got %v", tt.name, tt.rule, vs)
		}
	}

	ok := Decision{Action: "dispatch", Steps: []DispatchStep{
		{ID: "impl", Role: "coder", Instruction: "x", Inputs: []string{"r1/design/design.md"}, Workspace: "repo"},
		{ID: "tests", Role: "reviewer", Instruction: "y", Workspace: "repo-tests"},
	}}
	if vs := ValidateDecision(ok, ctx); len(vs) != 0 {
		t.Errorf("expected valid decision, got %v", vs)
	}
}

func TestParseOutputContract(t *testing.T) {
	oc, err := ParseOutputContract("work done\n```json\n{\"summary\":\"wrote design\",\"done\":true,\"artifacts\":[\"design.md\"]}\n```")
	if err != nil {
		t.Fatalf("ParseOutputContract: %v", err)
	}
	if oc.Summary != "wrote design" || !oc.Done || len(oc.Artifacts) != 1 {
		t.Errorf("unexpected contract %+v", oc)
	}
	if _, err := ParseOutputContract("plain text"); err == nil {
		t.Error("expected error for missing contract")
	}
	if _, err := ParseOutputContract(`{"done":true}`); err == nil {
		t.Error("expected error for missing summary")
	}
}
