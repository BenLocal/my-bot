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
			"summary": map[string]interface{}{
				"type":        "string",
				"description": "Progress summary of the goal so far (used as the next round's [progress])",
			},
			"steps": map[string]interface{}{
				"type":        "array",
				"description": "Steps to dispatch to members in parallel (only for action=dispatch)",
				"items": map[string]interface{}{
					"type":     "object",
					"required": []string{"id", "role", "instruction"},
					"properties": map[string]interface{}{
						"id":          map[string]interface{}{"type": "string", "pattern": "^[a-z0-9-]{1,32}$"},
						"role":        map[string]interface{}{"type": "string"},
						"instruction": map[string]interface{}{"type": "string"},
						"inputs": map[string]interface{}{
							"type":  "array",
							"items": map[string]interface{}{"type": "string"},
						},
						"workspace": map[string]interface{}{"type": "string", "pattern": "^[a-z0-9-]{1,32}$"},
					},
				},
			},
			"done": map[string]interface{}{
				"type":        "boolean",
				"description": "Whether the goal is complete (required true for action=finish)",
			},
			"question": map[string]interface{}{
				"type":        "string",
				"description": "Question for the user (only for action=ask_user)",
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

// DefaultMaxStepsPerRound is the default per-round step limit (spec §5.3 rule 2).
const DefaultMaxStepsPerRound = 4

// ValidationContext provides context for validation (member roles, artifact references, limits).
type ValidationContext struct {
	MemberRoles       []string
	ExistingArtifacts []string // 已登记产物引用 r<round>/<stepID>/<name>
	InputMounts       []string // Goal 声明的输入挂载名
	Workspaces        []string // Goal 声明的工作区名
	MaxStepsPerRound  int
	MaxTokens         int64
}

var idRe = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// ValidateDecision checks a Decision against spec §5.3 rules. Rule labels:
// R0 action/字段形状；R1 成员角色；R2 数量与 id；R3 inputs 与 workspace；R6 finish 需 done。
func ValidateDecision(d Decision, ctx ValidationContext) []Violation {
	var violations []Violation

	switch d.Action {
	case "dispatch", "finish":
	case "ask_user":
		if strings.TrimSpace(d.Question) == "" {
			violations = append(violations, Violation{"R0", "question", "ask_user requires a non-empty question"})
		}
	case "":
		violations = append(violations, Violation{"R0", "action", "action is required and must be dispatch, finish, or ask_user"})
	default:
		violations = append(violations, Violation{"R0", "action", fmt.Sprintf("unknown action %q", d.Action)})
	}

	if d.Action == "dispatch" {
		maxSteps := ctx.MaxStepsPerRound
		if maxSteps <= 0 {
			maxSteps = DefaultMaxStepsPerRound
		}
		if len(d.Steps) == 0 {
			violations = append(violations, Violation{"R2", "steps", "dispatch action requires at least one step"})
		}
		if len(d.Steps) > maxSteps {
			violations = append(violations, Violation{"R2", "steps",
				fmt.Sprintf("too many steps: %d > maxStepsPerRound %d", len(d.Steps), maxSteps)})
		}
	} else if len(d.Steps) > 0 {
		violations = append(violations, Violation{"R0", "steps", fmt.Sprintf("steps are only allowed for action=dispatch, got action %q", d.Action)})
	}

	seen := map[string]bool{}
	for i, step := range d.Steps {
		if !contains(ctx.MemberRoles, step.Role) {
			violations = append(violations, Violation{"R1", fmt.Sprintf("steps[%d].role", i),
				fmt.Sprintf("role %q is not a member of this goal (members: %s)", step.Role, strings.Join(ctx.MemberRoles, ", "))})
		}
		if !idRe.MatchString(step.ID) {
			violations = append(violations, Violation{"R2", fmt.Sprintf("steps[%d].id", i),
				fmt.Sprintf("id %q must match [a-z0-9-]{1,32}", step.ID)})
		} else if seen[step.ID] {
			violations = append(violations, Violation{"R2", fmt.Sprintf("steps[%d].id", i),
				fmt.Sprintf("id %q is not unique in this round", step.ID)})
		}
		seen[step.ID] = true
		for j, input := range step.Inputs {
			if !contains(ctx.ExistingArtifacts, input) && !contains(ctx.InputMounts, input) {
				violations = append(violations, Violation{"R3", fmt.Sprintf("steps[%d].inputs[%d]", i, j),
					fmt.Sprintf("artifact %q not found (expected a registered r<round>/<stepID>/<name> or a declared input mount)", input)})
			}
		}
		if step.Workspace != "" && !contains(ctx.Workspaces, step.Workspace) && !idRe.MatchString(step.Workspace) {
			violations = append(violations, Violation{"R3", fmt.Sprintf("steps[%d].workspace", i),
				fmt.Sprintf("workspace %q is neither declared nor a valid new name [a-z0-9-]{1,32}", step.Workspace)})
		}
	}

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
		if containsCredential(m.Text) {
			violations = append(violations, Violation{"RM4", fmt.Sprintf("memories[%d].text", i),
				"memory text contains credential-like patterns"})
		}
	}

	return violations
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// OutputContract is the member step output contract (spec §7.1).
type OutputContract struct {
	Summary   string   `json:"summary"`
	Done      bool     `json:"done"`
	Artifacts []string `json:"artifacts,omitempty"`
}

// ParseOutputContract extracts the member output contract JSON from raw output,
// using the same deterministic patching as ParseDecision. Unknown fields are
// tolerated; a missing summary is an error (spec §7.4 rule 1).
func ParseOutputContract(raw string) (OutputContract, error) {
	text := extractJSON(raw)
	if text == "" {
		return OutputContract{}, fmt.Errorf("no output contract JSON found")
	}
	var oc OutputContract
	if err := json.Unmarshal([]byte(text), &oc); err != nil {
		return OutputContract{}, fmt.Errorf("output contract JSON decode: %w", err)
	}
	if strings.TrimSpace(oc.Summary) == "" {
		return OutputContract{}, fmt.Errorf("output contract: summary is required")
	}
	return oc, nil
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
