package a2a

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/BenLocal/my-bot/internal/core"
)

// TaskResult 是从 Worker 返回的 Task 中解出的 Attempt 结果。
type TaskResult struct {
	Status    core.AttemptStatus
	Reason    string
	Retryable bool
	Usage     core.Usage
	// Text 是 Task.Status.Message 中文本 part 的拼接。
	Text string
	// Artifacts 的 Path 为 r<round>/<stepID>/<name> 相对引用（大脑决策 inputs 使用的形式）。
	Artifacts []core.ArtifactRef
}

// EncodeUsage 把 core.Usage 编码为 Task.Metadata 可用的值。
// 使用 map[string]any + int64，保证 SDK 内部 gob 深拷贝可以处理。
func EncodeUsage(u core.Usage) map[string]any {
	return map[string]any{
		"inputTokens":  u.InputTokens,
		"outputTokens": u.OutputTokens,
	}
}

// EncodeResultMeta 把 Attempt 结果写入 metadata 容器并返回该容器（nil 时新建）。
// reason 为空时不写；retryable 与 usage 总是写入。
func EncodeResultMeta(m map[string]any, status core.AttemptStatus, reason string, retryable bool, usage core.Usage) map[string]any {
	if m == nil {
		m = make(map[string]any)
	}
	m[MetaAttemptStatus] = string(status)
	if reason != "" {
		m[MetaReason] = reason
	} else {
		delete(m, MetaReason)
	}
	m[MetaRetryable] = retryable
	m[MetaUsage] = EncodeUsage(usage)
	return m
}

// ArtifactRelPath 返回产物的相对引用 r<round>/<stepID>/<name>。
func ArtifactRelPath(round int, stepID, name string) string {
	return "r" + strconv.Itoa(round) + "/" + stepID + "/" + name
}

// ArtifactURI 返回 mybot-artifact://<goalID>/r<round>/<stepID>/<name>。
func ArtifactURI(goalID string, round int, stepID, name string) string {
	return ArtifactURIScheme + goalID + "/" + ArtifactRelPath(round, stepID, name)
}

// NewArtifact 构造带 mybot 产物 metadata 的 A2A Artifact；唯一 part 为 URI 文本。
func NewArtifact(goalID string, round int, stepID, name, path, sha256 string, size int64) *a2a.Artifact {
	uri := ArtifactURI(goalID, round, stepID, name)
	return &a2a.Artifact{
		ID:    a2a.NewArtifactID(),
		Name:  name,
		Parts: []*a2a.Part{a2a.NewTextPart(uri)},
		Metadata: map[string]any{
			MetaArtifactURI:    uri,
			MetaArtifactPath:   path,
			MetaArtifactSha256: sha256,
			MetaArtifactSize:   size,
		},
	}
}

// DecodeTaskResult 解析 Task 上的 mybot 结果 metadata；缺少 MetaAttemptStatus
// 时按 A2A Task 状态推导。
func DecodeTaskResult(task *a2a.Task) TaskResult {
	var r TaskResult
	if task == nil {
		r.Status = core.AttemptUnknown
		return r
	}
	m := task.Metadata
	r.Reason = asString(m[MetaReason])
	r.Retryable = asBool(m[MetaRetryable])
	r.Usage = decodeUsage(m[MetaUsage])
	if s := asString(m[MetaAttemptStatus]); s != "" {
		r.Status = core.AttemptStatus(s)
	} else {
		r.Status = statusFromState(task.Status.State)
		if task.Status.State == a2a.TaskStateAuthRequired && r.Reason == "" {
			r.Reason = "auth_required"
		}
	}
	if msg := task.Status.Message; msg != nil {
		var texts []string
		for _, p := range msg.Parts {
			if p == nil {
				continue
			}
			if t := p.Text(); t != "" {
				texts = append(texts, t)
			}
		}
		r.Text = strings.Join(texts, "\n")
	}
	for _, art := range task.Artifacts {
		if art == nil {
			continue
		}
		ref, ok := decodeArtifactRef(art)
		if ok {
			r.Artifacts = append(r.Artifacts, ref)
		}
	}
	return r
}

// statusFromState 把 A2A Task 状态映射为 AttemptStatus。
func statusFromState(st a2a.TaskState) core.AttemptStatus {
	switch st {
	case a2a.TaskStateCompleted:
		return core.AttemptCompleted
	case a2a.TaskStateFailed, a2a.TaskStateAuthRequired:
		return core.AttemptFailed
	case a2a.TaskStateCanceled:
		return core.AttemptCancelled
	case a2a.TaskStateRejected:
		return core.AttemptRejected
	case a2a.TaskStateWorking:
		return core.AttemptWorking
	case a2a.TaskStateSubmitted:
		return core.AttemptSubmitted
	default:
		return core.AttemptUnknown
	}
}

// decodeArtifactRef 从 Artifact.Metadata 解出相对产物引用。没有 MetaArtifactURI
// 的产物不是 mybot 产物，忽略。
func decodeArtifactRef(art *a2a.Artifact) (core.ArtifactRef, bool) {
	uri := asString(art.Metadata[MetaArtifactURI])
	rest, ok := strings.CutPrefix(uri, ArtifactURIScheme)
	if !ok {
		return core.ArtifactRef{}, false
	}
	// rest = <goalID>/r<round>/<stepID>/<name>，去掉 goalID 段得到相对引用。
	_, rel, ok := strings.Cut(rest, "/")
	if !ok || rel == "" {
		return core.ArtifactRef{}, false
	}
	ref := core.ArtifactRef{
		Path:   rel,
		Sha256: asString(art.Metadata[MetaArtifactSha256]),
		Size:   asInt64(art.Metadata[MetaArtifactSize]),
	}
	if seg, _, _ := strings.Cut(rel, "/"); strings.HasPrefix(seg, "r") {
		if n, err := strconv.Atoi(seg[1:]); err == nil {
			ref.Round = n
		}
	}
	return ref, true
}

// decodeUsage 兼容进程内（map / core.Usage）与 JSON 解码后（map[string]any 中 float64）的表示。
func decodeUsage(v any) core.Usage {
	if v == nil {
		return core.Usage{}
	}
	if u, ok := v.(core.Usage); ok {
		return u
	}
	b, err := json.Marshal(v)
	if err != nil {
		return core.Usage{}
	}
	var u core.Usage
	if err := json.Unmarshal(b, &u); err != nil {
		return core.Usage{}
	}
	return u
}

func asBool(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case string:
		ok, _ := strconv.ParseBool(b)
		return ok
	}
	return false
}

func asInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i
		}
	}
	return 0
}
