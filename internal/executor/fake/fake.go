// Package fake 提供进程内模拟执行器驱动，用于本地开发与端到端测试。
//
// 它按 Script 脚本化执行：可配置耗时、结果文本、失败及 Retryable、挂起、
// 重复回调、产物文件；Recover 也可脚本化，模拟 spec §7.2 的四种判据。
package fake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/executor"
)

// ErrUnknownHandle 表示句柄不存在。
var ErrUnknownHandle = errors.New("fake: unknown handle")

// Duration 是支持 "20ms" 字符串与纳秒整数两种 JSON 表示的时间长度。
type Duration time.Duration

// UnmarshalJSON 解析字符串（time.ParseDuration）或整数（纳秒）表示。
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch t := v.(type) {
	case string:
		parsed, err := time.ParseDuration(t)
		if err != nil {
			return fmt.Errorf("fake: invalid duration %q: %w", t, err)
		}
		*d = Duration(parsed)
		return nil
	case float64:
		*d = Duration(time.Duration(t))
		return nil
	case json.Number:
		n, err := t.Int64()
		if err != nil {
			return err
		}
		*d = Duration(time.Duration(n))
		return nil
	default:
		return fmt.Errorf("fake: invalid duration type %T", v)
	}
}

// Script 是一次 fake Attempt 的脚本。
type Script struct {
	// Delay 是开始工作前的延迟。
	Delay Duration `json:"delay,omitempty"`
	// Result 是最终结果文本，可包含大脑决策 JSON 或 output contract JSON。
	Result string `json:"result,omitempty"`
	// Fail 为 true 时最终失败。
	Fail bool `json:"fail,omitempty"`
	// Retryable 仅 Fail 有意义，标记失败是否可重试。
	Retryable bool `json:"retryable,omitempty"`
	// Reason 是失败/暂停原因（auth_required、rate_limited、timeout 等）。
	Reason string `json:"reason,omitempty"`
	// Hang 为 true 时不发终态事件，一直停在 working，直到被 Cancel。
	Hang bool `json:"hang,omitempty"`
	// Repeats 是终态前发出的 progress 事件数量（模拟重复回调）。
	Repeats int `json:"repeats,omitempty"`
	// Phase 是终态阶段；为空时按 Fail 推导（Fail→failed，否则→completed）。
	Phase executor.Phase `json:"phase,omitempty"`
	// Usage 是本次 Attempt 的增量用量（fake 固定值）。
	Usage core.Usage `json:"usage,omitempty"`
	// Artifacts 是要产出的文件。
	Artifacts []ArtifactSpec `json:"artifacts,omitempty"`

	// RecoverPhase 是 Recover 返回的阶段，缺省 PhaseInterrupted。
	RecoverPhase executor.Phase `json:"recoverPhase,omitempty"`
	// RecoverReason 是 Recover 返回的原因，用于区分 interrupted 的两种判据。
	RecoverReason string `json:"recoverReason,omitempty"`
}

// ArtifactSpec 是产物文件的脚本描述。
type ArtifactSpec struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// 编译期断言：Fake 实现 Driver。
var _ executor.Driver = (*Fake)(nil)

// Fake 是可脚本化的进程内执行器驱动。
type Fake struct {
	mu            sync.Mutex
	defaultScript Script
	scripts       map[string]Script
	artifactRoot  string
	handles       map[executor.Handle]*fakeHandle
	seq           int
}

type fakeHandle struct {
	events chan executor.Event
	mu     sync.Mutex
	status executor.Status
	cancel context.CancelFunc
}

// Option 配置 Fake。
type Option func(*Fake)

// WithArtifactRoot 指定产物写入目录；缺省用 os.TempDir()/mybot-fake-artifacts。
func WithArtifactRoot(dir string) Option {
	return func(f *Fake) {
		if dir != "" {
			f.artifactRoot = dir
		}
	}
}

// WithDefaultScript 指定默认脚本（未按 attemptKey 覆盖时使用）。
func WithDefaultScript(s Script) Option {
	return func(f *Fake) { f.defaultScript = s }
}

// New 创建 Fake 驱动。
func New(opts ...Option) *Fake {
	f := &Fake{
		scripts:      map[string]Script{},
		handles:      map[executor.Handle]*fakeHandle{},
		artifactRoot: filepath.Join(os.TempDir(), "mybot-fake-artifacts"),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// NewWithScript 以指定脚本创建 Fake。
func NewWithScript(s Script, opts ...Option) *Fake {
	return New(append([]Option{WithDefaultScript(s)}, opts...)...)
}

// SetDefaultScript 设置默认脚本。
func (f *Fake) SetDefaultScript(s Script) {
	f.mu.Lock()
	f.defaultScript = s
	f.mu.Unlock()
}

// SetScript 按 attemptKey 覆盖脚本。
func (f *Fake) SetScript(attemptKey string, s Script) {
	f.mu.Lock()
	f.scripts[attemptKey] = s
	f.mu.Unlock()
}

// SetRecover 按 attemptKey 脚本化 Recover 的返回结果。
func (f *Fake) SetRecover(attemptKey string, phase executor.Phase, reason string) {
	f.SetScript(attemptKey, Script{RecoverPhase: phase, RecoverReason: reason})
}

func (f *Fake) scriptFor(attemptKey string) Script {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.scripts[attemptKey]; ok {
		return s
	}
	return f.defaultScript
}

// Start 按脚本启动一次异步执行。
func (f *Fake) Start(ctx context.Context, req executor.StartReq) (executor.Handle, error) {
	script := f.scriptFor(req.AttemptKey)

	f.mu.Lock()
	f.seq++
	h := executor.Handle(fmt.Sprintf("fake-%d", f.seq))
	eh := &fakeHandle{
		events: make(chan executor.Event, 16),
		status: executor.Status{Phase: executor.PhaseWorking},
	}
	f.handles[h] = eh
	f.mu.Unlock()

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	eh.cancel = cancel

	go f.run(runCtx, eh, req, script)
	return h, nil
}

func (f *Fake) run(ctx context.Context, eh *fakeHandle, req executor.StartReq, s Script) {
	defer close(eh.events)
	defer eh.cancel()

	// 1. 延迟。
	if s.Delay > 0 {
		select {
		case <-time.After(time.Duration(s.Delay)):
		case <-ctx.Done():
			return
		}
	}

	// 2. 重复 progress 回调。
	for i := 0; i < s.Repeats; i++ {
		select {
		case eh.events <- executor.Event{Type: executor.EventProgress, Text: s.Result}:
		case <-ctx.Done():
			return
		}
	}

	// 3. 产物（挂起模式不产出）。
	if len(s.Artifacts) > 0 && !s.Hang {
		arts, err := f.writeArtifacts(req, s.Artifacts)
		if err != nil {
			reason := fmt.Sprintf("artifact write failed: %v", err)
			eh.setStatus(executor.Status{
				Phase:     executor.PhaseFailed,
				Retryable: s.Retryable,
				Reason:    reason,
				Usage:     s.Usage,
			})
			select {
			case eh.events <- executor.Event{
				Type:      executor.EventTerminal,
				Phase:     executor.PhaseFailed,
				Retryable: s.Retryable,
				Reason:    reason,
				Usage:     s.Usage,
			}:
			case <-ctx.Done():
			}
			return
		}
		select {
		case eh.events <- executor.Event{Type: executor.EventArtifact, Artifacts: arts}:
		case <-ctx.Done():
			return
		}
	}

	// 4. 终态或挂起。
	if s.Hang {
		<-ctx.Done()
		return
	}

	phase, retryable, reason := terminalPhase(s)
	ev := executor.Event{
		Type:      executor.EventTerminal,
		Text:      s.Result,
		Phase:     phase,
		Retryable: retryable,
		Reason:    reason,
		Usage:     s.Usage,
	}
	select {
	case eh.events <- ev:
	case <-ctx.Done():
		return
	}
	eh.setStatus(executor.Status{
		Phase:     phase,
		Retryable: retryable,
		Reason:    reason,
		Usage:     s.Usage,
	})
}

func (f *Fake) writeArtifacts(req executor.StartReq, specs []ArtifactSpec) ([]executor.Artifact, error) {
	dir := filepath.Join(f.artifactRoot, req.AttemptKey)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact dir: %w", err)
	}
	out := make([]executor.Artifact, 0, len(specs))
	for _, spec := range specs {
		path := filepath.Join(dir, spec.Name)
		if err := os.WriteFile(path, []byte(spec.Content), 0o644); err != nil {
			return nil, fmt.Errorf("write artifact %s: %w", spec.Name, err)
		}
		fi, err := os.Stat(path)
		size := int64(0)
		if err == nil {
			size = fi.Size()
		}
		out = append(out, executor.Artifact{
			Name:   spec.Name,
			Path:   path,
			Sha256: core.HashSHA256([]byte(spec.Content)),
			Size:   size,
		})
	}
	return out, nil
}

func terminalPhase(s Script) (executor.Phase, bool, string) {
	if s.Phase != "" {
		return s.Phase, s.Retryable, s.Reason
	}
	if s.Fail {
		return executor.PhaseFailed, s.Retryable, s.Reason
	}
	return executor.PhaseCompleted, false, ""
}

func (eh *fakeHandle) setStatus(st executor.Status) {
	eh.mu.Lock()
	eh.status = st
	eh.mu.Unlock()
}

// Status 返回句柄当前阶段。
func (f *Fake) Status(ctx context.Context, h executor.Handle) (executor.Status, error) {
	f.mu.Lock()
	eh, ok := f.handles[h]
	f.mu.Unlock()
	if !ok {
		return executor.Status{}, ErrUnknownHandle
	}
	eh.mu.Lock()
	defer eh.mu.Unlock()
	return eh.status, nil
}

// Events 返回句柄的事件通道。
func (f *Fake) Events(h executor.Handle) <-chan executor.Event {
	f.mu.Lock()
	eh, ok := f.handles[h]
	f.mu.Unlock()
	if !ok {
		closed := make(chan executor.Event)
		close(closed)
		return closed
	}
	return eh.events
}

// Cancel 取消句柄对应的执行。
func (f *Fake) Cancel(ctx context.Context, h executor.Handle) error {
	f.mu.Lock()
	eh, ok := f.handles[h]
	f.mu.Unlock()
	if !ok {
		return ErrUnknownHandle
	}
	eh.cancel()
	return nil
}

// Recover 按 attemptKey 的脚本模拟 spec §7.2 的四种判据。
func (f *Fake) Recover(ctx context.Context, rec executor.AttemptRecord) (executor.Status, error) {
	s := f.scriptFor(rec.AttemptKey)
	phase := s.RecoverPhase
	if phase == "" {
		phase = executor.PhaseInterrupted
	}
	return executor.Status{
		Phase:     phase,
		Retryable: s.Retryable,
		Reason:    s.RecoverReason,
		Usage:     s.Usage,
	}, nil
}

// ParseScript 从提示正文中提取 fake-script 指令块并解析为 Script。
//
// 支持两种形式：带 ```fake-script 围栏的 JSON，或整段为纯 JSON。
func ParseScript(text string) (Script, error) {
	raw := extractFakeScript(text)
	if raw == "" {
		return Script{}, fmt.Errorf("fake: no fake-script block found")
	}
	var s Script
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return Script{}, fmt.Errorf("fake: parse script: %w", err)
	}
	return s, nil
}

// LoadScriptFile 从 role 配置文件（JSON）读取 Script。
func LoadScriptFile(path string) (Script, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Script{}, fmt.Errorf("fake: read script file: %w", err)
	}
	var s Script
	if err := json.Unmarshal(b, &s); err != nil {
		return Script{}, fmt.Errorf("fake: parse script file: %w", err)
	}
	return s, nil
}

func extractFakeScript(text string) string {
	lines := strings.Split(text, "\n")
	var current []string
	var blocks []string
	inBlock := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			lang := strings.TrimPrefix(trimmed, "```")
			if inBlock {
				blocks = append(blocks, strings.Join(current, "\n"))
				current = nil
				inBlock = false
				continue
			}
			if lang == "fake-script" || lang == "fake" {
				inBlock = true
			}
			continue
		}
		if inBlock {
			current = append(current, line)
		}
	}
	if len(blocks) > 0 {
		return strings.TrimSpace(blocks[len(blocks)-1])
	}
	trimmed := strings.TrimSpace(text)
	if json.Valid([]byte(trimmed)) {
		return trimmed
	}
	return ""
}
