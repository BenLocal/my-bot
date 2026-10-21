package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

// NewGoalID generates a new ULID-based goal/loop/task ID with "g_" prefix.
func NewGoalID() string {
	id := ulid.Make().String()
	return "g_" + id
}

// AttemptKey derives a deterministic attempt key from goal, round, step, attempt.
func AttemptKey(goalID string, round int, stepID string, attempt int) string {
	h := fnv.New32a()
	h.Write([]byte(goalID))
	hash := hex.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("r%d:s%s:a%d:%s", round, stepID, attempt, hash[:8])
}

// AskRequestID derives a deterministic request ID for ask_user.
func AskRequestID(goalID string, round int) string {
	return fmt.Sprintf("ask_%s_r%d", goalID, round)
}

// EventID derives a deterministic event ID: <goalID>:<seq>
func EventID(goalID string, seq int) string {
	return fmt.Sprintf("%s:%d", goalID, seq)
}

// SessionID derives a deterministic session ID for reuse: uuid5(goalID + ":" + role)
func SessionID(goalID, role string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(goalID+":"+role)).String()
}

// SessionIDFresh derives a fresh session ID for a new attempt: uuid5(attemptKey)
func SessionIDFresh(attemptKey string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(attemptKey)).String()
}

// MemoryEntryID derives a deterministic memory entry ID: uuid5(goalID + round + seq)
func MemoryEntryID(goalID string, round, seq int) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s:%d:%d", goalID, round, seq))).String()
}

// StepID generates a deterministic step ID within a round.
func StepID(round int, role string, n int) string {
	return fmt.Sprintf("s%d:%s:%d", round, role, n)
}

// RoundID is simply the round number as string.
func RoundID(n int) string {
	return fmt.Sprintf("r%d", n)
}

// HashSHA256 computes sha256 hex of data.
func HashSHA256(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// HashString computes sha256 hex of a string.
func HashString(s string) string {
	return HashSHA256([]byte(s))
}

// NormalizeText normalizes text for NoProgress comparison (whitespace, case, punctuation).
func NormalizeText(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	// Remove extra whitespace
	var result strings.Builder
	space := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			space = true
			continue
		}
		if space && result.Len() > 0 {
			result.WriteByte(' ')
		}
		space = false
		result.WriteRune(r)
	}
	return result.String()
}
