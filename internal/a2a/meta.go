// Package a2a 封装 my-bot 的 A2A 协议适配层：客户端与 Worker 服务端。
//
// 本包负责 mybot/* metadata 的编解码、A2A 客户端封装，以及把 A2A 请求
// 转成 Worker 内部接口调用的服务端适配。协议固定为 A2A 1.0（HTTP JSON-RPC + SSE）。
package a2a

import (
	"fmt"
	"strconv"
)

// mybot/* metadata 键（spec §7.1）。
const (
	MetaAttemptKey    = "mybot/attemptKey"
	MetaGoalID        = "mybot/goalID"
	MetaRound         = "mybot/round"
	MetaWorkspace     = "mybot/workspace"
	MetaSessionPolicy = "mybot/sessionPolicy"
	MetaInputs        = "mybot/inputs"
	MetaTraceparent   = "mybot/traceparent"
	MetaRole          = "mybot/role"
	MetaStepID        = "mybot/stepID"
)

// Worker 写在返回 Task.Metadata 上的结果键（spec §7.1、§7.2）。
const (
	// MetaAttemptStatus 的值是 core.AttemptStatus 字符串。
	MetaAttemptStatus = "mybot/attemptStatus"
	// MetaReason 是失败原因（auth_required、rate_limited、timeout、driver_bug、crashed 等）。
	MetaReason = "mybot/reason"
	// MetaRetryable 是失败是否可重试（bool）。
	MetaRetryable = "mybot/retryable"
	// MetaUsage 是本次 Attempt 的增量用量，JSON 形如 core.Usage（{inputTokens, outputTokens}）。
	MetaUsage = "mybot/usage"
)

// Worker 写在每个 Task Artifact.Metadata 上的产物键；Artifact.Name 为文件名。
const (
	// MetaArtifactURI 形如 mybot-artifact://<goalID>/r<round>/<stepID>/<name>。
	MetaArtifactURI = "mybot/uri"
	// MetaArtifactPath 是共享产物目录上的绝对路径。
	MetaArtifactPath   = "mybot/path"
	MetaArtifactSha256 = "mybot/sha256"
	MetaArtifactSize   = "mybot/size"
)

// ArtifactURIScheme 是产物 URI 的前缀。
const ArtifactURIScheme = "mybot-artifact://"

// SessionPolicy 控制一次 Attempt 对执行器会话的复用策略。
type SessionPolicy string

const (
	// SessionPolicyReuse 复用同一 Goal + 角色的上一次会话。
	SessionPolicyReuse SessionPolicy = "reuse"
	// SessionPolicyFresh 为本次 Attempt 新建会话。
	SessionPolicyFresh SessionPolicy = "fresh"
)

// Metadata 是 my-bot 调度所需的最小元数据。
type Metadata struct {
	// AttemptKey 必填，是去重与跨重启恢复的唯一键（形如 g1/r1/s1/a1）。
	AttemptKey    string
	GoalID        string
	Round         int
	Workspace     string
	SessionPolicy SessionPolicy
	Inputs        []string
	Traceparent   string
	// Role 是本 Step 的角色名，用于派生 reuse 会话 ID。
	Role string
	// StepID 是本 Step 在轮内的 ID，用于产物路径。
	StepID string
}

// EncodeMetadata 把 Metadata 写入请求级 metadata 容器并返回该容器。
// 空值字段不写入，避免无意义地污染 Task.metadata；传入 nil 时会创建新容器。
func EncodeMetadata(m map[string]any, meta Metadata) map[string]any {
	if m == nil {
		m = make(map[string]any)
	}
	if meta.AttemptKey != "" {
		m[MetaAttemptKey] = meta.AttemptKey
	}
	if meta.GoalID != "" {
		m[MetaGoalID] = meta.GoalID
	}
	if meta.Round != 0 {
		m[MetaRound] = meta.Round
	}
	if meta.Workspace != "" {
		m[MetaWorkspace] = meta.Workspace
	}
	if meta.SessionPolicy != "" {
		m[MetaSessionPolicy] = string(meta.SessionPolicy)
	}
	if len(meta.Inputs) > 0 {
		m[MetaInputs] = meta.Inputs
	}
	if meta.Traceparent != "" {
		m[MetaTraceparent] = meta.Traceparent
	}
	if meta.Role != "" {
		m[MetaRole] = meta.Role
	}
	if meta.StepID != "" {
		m[MetaStepID] = meta.StepID
	}
	return m
}

// DecodeMetadata 从请求级 metadata 容器解码。attemptKey 必填，缺失时返回错误。
func DecodeMetadata(m map[string]any) (Metadata, error) {
	var meta Metadata

	meta.AttemptKey = asString(m[MetaAttemptKey])
	if meta.AttemptKey == "" {
		return meta, fmt.Errorf("%s required", MetaAttemptKey)
	}

	meta.GoalID = asString(m[MetaGoalID])
	meta.Workspace = asString(m[MetaWorkspace])
	meta.Traceparent = asString(m[MetaTraceparent])
	meta.Role = asString(m[MetaRole])
	meta.StepID = asString(m[MetaStepID])
	meta.SessionPolicy = SessionPolicy(asString(m[MetaSessionPolicy]))

	if v, ok := m[MetaRound]; ok {
		meta.Round = asInt(v)
	}
	if v, ok := m[MetaInputs]; ok {
		meta.Inputs = asStringSlice(v)
	}
	return meta, nil
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		if i, err := strconv.Atoi(n); err == nil {
			return i
		}
	}
	return 0
}

func asStringSlice(v any) []string {
	switch items := v.(type) {
	case []string:
		return items
	case []any:
		out := make([]string, 0, len(items))
		for _, it := range items {
			if s, ok := it.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
