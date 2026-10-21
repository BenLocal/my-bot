package core

import (
	"testing"
)

func TestParseDecisionPureJSON(t *testing.T) {
	raw := `{"action":"finish","done":true,"reason":"completed"}`
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
	raw := "First attempt:\n```json\n{\"action\":\"ask_user\",\"askUser\":{\"question\":\"which?\"}}\n```\nSecond:\n```json\n{\"action\":\"dispatch\",\"steps\":[{\"role\":\"coder\",\"instruction\":\"do it\"}]}\n```"
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
			{Role: "coder", Instruction: "write code"},
			{Role: "reviewer", Instruction: "review"},
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
			{Role: "nonexistent", Instruction: "do something"},
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
			{Role: "coder", Instruction: "1"},
			{Role: "coder", Instruction: "2"},
			{Role: "coder", Instruction: "3"},
			{Role: "coder", Instruction: "4"},
			{Role: "coder", Instruction: "5"},
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
