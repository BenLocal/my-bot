package core

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// DecisionSchema returns the JSON Schema for brain decisions.
// Used to inject into the brain prompt (spec §5.3).
func DecisionSchema() map[string]interface{} {
	return map[string]interface{}{
		"$schema":  "https://json-schema.org/draft/2020-12/schema",
		"type":     "object",
		"required": []string{"action"},
		"properties": map[string]interface{}{
			"action": map[string]interface{}{
				"type":        "string",
				"enum":        []string{"dispatch", "finish", "ask_user"},
				"description": "What the brain wants to do this round",
			},
			"steps": map[string]interface{}{
				"type":        "array",
				"description": "Steps to dispatch to members (only for action=dispatch)",
				"items": map[string]interface{}{
					"type":     "object",
					"required": []string{"role", "instruction"},
					"properties": map[string]interface{}{
						"role":        map[string]interface{}{"type": "string"},
						"instruction": map[string]interface{}{"type": "string"},
						"inputs": map[string]interface{}{
							"type":  "array",
							"items": map[string]interface{}{"type": "string"},
						},
					},
				},
			},
			"done": map[string]interface{}{
				"type":        "boolean",
				"description": "Whether the goal is complete (only for action=finish)",
			},
			"reason": map[string]interface{}{
				"type":        "string",
				"description": "Reason for finishing or asking",
			},
			"askUser": map[string]interface{}{
				"type":        "object",
				"description": "User question (only for action=ask_user)",
				"properties": map[string]interface{}{
					"question": map[string]interface{}{"type": "string"},
				},
				"required": []string{"question"},
			},
			"memories": map[string]interface{}{
				"type":        "array",
				"description": "Memories to commit to L1",
				"items": map[string]interface{}{
					"type":     "object",
					"required": []string{"topic", "text", "kind"},
					"properties": map[string]interface{}{
						"topic": map[string]interface{}{"type": "string"},
						"text":  map[string]interface{}{"type": "string"},
						"kind": map[string]interface{}{
							"type": "string",
							"enum": []string{"fact", "pitfall", "preference"},
						},
					},
				},
			},
		},
	}
}

// DecisionSchemaJSON returns the JSON Schema as a pretty-printed string.
func DecisionSchemaJSON() string {
	b, _ := json.MarshalIndent(DecisionSchema(), "", "  ")
	return string(b)
}

// ParseDecision parses a raw string into a Decision, with deterministic patching.
// Steps: 1. Strip markdown fences, extract last JSON block / first complete object.
//  2. Strict JSON decoding (unknown fields rejected).
func ParseDecision(raw string) (Decision, error) {
	text := extractJSON(raw)
	if text == "" {
		return Decision{}, fmt.Errorf("no JSON found in decision text")
	}

	dec := json.NewDecoder(strings.NewReader(text))
	dec.DisallowUnknownFields()
	var d Decision
	if err := dec.Decode(&d); err != nil {
		return Decision{}, fmt.Errorf("decision JSON decode: %w", err)
	}
	return d, nil
}

// extractJSON pulls JSON out of arbitrary text:
// - If the text is pure JSON, return it.
// - If it has fenced code blocks, return the last one.
// - Otherwise find the first { ... } balanced object.
func extractJSON(raw string) string {
	raw = strings.TrimSpace(raw)

	// Try direct parse first
	if json.Valid([]byte(raw)) {
		return raw
	}

	// Try fenced code blocks (take the last one)
	var blocks []string
	inBlock := false
	var currentBlock strings.Builder
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			if inBlock {
				// End of block
				inBlock = false
				blocks = append(blocks, currentBlock.String())
				currentBlock.Reset()
			} else {
				// Start of block
				inBlock = true
			}
		} else if inBlock {
			currentBlock.WriteString(line)
			currentBlock.WriteString("\n")
		}
	}
	if len(blocks) > 0 {
		return strings.TrimSpace(blocks[len(blocks)-1])
	}

	// Find the first balanced { ... } object
	start := strings.Index(raw, "{")
	if start < 0 {
		return ""
	}
	// Use json.Decoder to find the first complete object
	dec := json.NewDecoder(strings.NewReader(raw[start:]))
	var v interface{}
	if err := dec.Decode(&v); err == nil {
		// Re-encode just the first object
		b, _ := json.Marshal(v)
		return string(b)
	}

	return ""
}

// Violation describes a single validation error.
type Violation struct {
	Rule    string // rule number from spec §5.3
	Field   string // which field
	Message string // human-readable description (can be sent back to brain)
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] %s: %s", v.Rule, v.Field, v.Message)
}

// ValidateDecision checks a Decision against spec §5.3 rules.
// ctx provides context for validation (member roles, artifact references, limits).
type ValidationContext struct {
	MemberRoles       []string
	ExistingArtifacts []string
	MaxStepsPerRound  int
	MaxTokens         int64
}

func ValidateDecision(d Decision, ctx ValidationContext) []Violation {
	var violations []Violation

	// Rule 1: action must be one of dispatch, finish, ask_user
	switch d.Action {
	case "dispatch":
	case "finish":
	case "ask_user":
	case "":
		violations = append(violations, Violation{"R1", "action", "action is required and must be dispatch, finish, or ask_user"})
	default:
		violations = append(violations, Violation{"R1", "action", fmt.Sprintf("unknown action %q", d.Action)})
	}

	// Rule 2: for dispatch, steps must be non-empty
	if d.Action == "dispatch" && len(d.Steps) == 0 {
		violations = append(violations, Violation{"R2", "steps", "dispatch action requires at least one step"})
	}

	// Rule 3: dispatch step roles must be in member roles
	for i, step := range d.Steps {
		role := step.Role
		found := false
		for _, m := range ctx.MemberRoles {
			if m == role {
				found = true
				break
			}
		}
		if !found {
			violations = append(violations, Violation{"R3", fmt.Sprintf("steps[%d].role", i),
				fmt.Sprintf("role %q is not a member role", role)})
		}
	}

	// Rule 4: for dispatch, step count must not exceed maxStepsPerRound
	if d.Action == "dispatch" && ctx.MaxStepsPerRound > 0 && len(d.Steps) > ctx.MaxStepsPerRound {
		violations = append(violations, Violation{"R4", "steps",
			fmt.Sprintf("too many steps: %d > max %d", len(d.Steps), ctx.MaxStepsPerRound)})
	}

	// Rule 5: for dispatch, input artifact references must exist
	for i, step := range d.Steps {
		for j, input := range step.Inputs {
			found := false
			for _, a := range ctx.ExistingArtifacts {
				if a == input {
					found = true
					break
				}
			}
			if !found {
				violations = append(violations, Violation{"R5", fmt.Sprintf("steps[%d].inputs[%d]", i, j),
					fmt.Sprintf("artifact %q not found", input)})
			}
		}
	}

	// Rule 6: for finish, done must be true
	if d.Action == "finish" && !d.Done {
		violations = append(violations, Violation{"R6", "done", "finish action requires done=true"})
	}

	// Memory entry validation
	for i, m := range d.Memories {
		if len(m.Text) > 300 {
			violations = append(violations, Violation{"RM1", fmt.Sprintf("memories[%d].text", i),
				"memory text exceeds 300 characters"})
		}
		if !isValidTopic(m.Topic) {
			violations = append(violations, Violation{"RM2", fmt.Sprintf("memories[%d].topic", i),
				fmt.Sprintf("topic %q must match [a-z0-9-]{1,48}", m.Topic)})
		}
		if m.Kind != "fact" && m.Kind != "pitfall" && m.Kind != "preference" {
			violations = append(violations, Violation{"RM3", fmt.Sprintf("memories[%d].kind", i),
				fmt.Sprintf("kind %q must be fact, pitfall, or preference", m.Kind)})
		}
	}

	// Credential scan on memory text
	for i, m := range d.Memories {
		if containsCredential(m.Text) {
			violations = append(violations, Violation{"RM4", fmt.Sprintf("memories[%d].text", i),
				"memory text contains credential-like patterns"})
		}
	}

	return violations
}

var topicRe = regexp.MustCompile(`^[a-z0-9-]{1,48}$`)

func isValidTopic(topic string) bool {
	return topicRe.MatchString(topic)
}

// credentialPatterns detects common credential patterns in text.
var credentialPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)-----BEGIN (RSA |EC |DSA |OPENSSH |)PRIVATE KEY-----`),
	regexp.MustCompile(`\bsk-[a-zA-Z0-9]{20,}\b`),
	regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
	regexp.MustCompile(`\beyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\b`),
	regexp.MustCompile(`(?i)password\s*=\s*\S+`),
}

func containsCredential(text string) bool {
	for _, re := range credentialPatterns {
		if re.MatchString(text) {
			return true
		}
	}
	return false
}
