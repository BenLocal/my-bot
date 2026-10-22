package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/url"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
)

// Client 封装 A2A 客户端，并附带 my-bot 的 attemptKey 查询扩展。
type Client struct {
	inner      *a2aclient.Client
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewClient 解析 Agent Card 并创建带 Bearer 认证的 A2A 客户端。
func NewClient(ctx context.Context, baseURL, token string) (*Client, error) {
	card, err := agentcard.DefaultResolver.Resolve(ctx, baseURL,
		agentcard.WithRequestHeader("Authorization", "Bearer "+token))
	if err != nil {
		return nil, err
	}
	inner, err := a2aclient.NewFromCard(ctx, card,
		a2aclient.WithCallInterceptors(&bearerInterceptor{token: token}))
	if err != nil {
		return nil, err
	}
	return &Client{
		inner:      inner,
		baseURL:    baseURL,
		token:      token,
		httpClient: &http.Client{},
	}, nil
}

// Submit 发送消息并立即返回 Task。强制 configuration.returnImmediately=true，
// 且要求请求级 metadata 中包含 mybot/attemptKey。
func (c *Client) Submit(ctx context.Context, req *a2a.SendMessageRequest) (*a2a.Task, error) {
	// Validate attemptKey in request metadata
	if _, err := DecodeMetadata(req.Metadata); err != nil {
		return nil, err
	}
	if req.Config == nil {
		req.Config = &a2a.SendMessageConfig{}
	}
	req.Config.ReturnImmediately = true

	res, err := c.inner.SendMessage(ctx, req)
	if err != nil {
		return nil, err
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		return nil, fmt.Errorf("expected *a2a.Task, got %T", res)
	}
	return task, nil
}

// Get 按 remoteTaskID 查询 Task（A2A GetTask）。
func (c *Client) Get(ctx context.Context, taskID a2a.TaskID) (*a2a.Task, error) {
	return c.inner.GetTask(ctx, &a2a.GetTaskRequest{ID: taskID})
}

// GetByAttemptKey 通过普通 HTTP 端点按 attemptKey 查询 Task。
// 找不到时返回 a2a.ErrTaskNotFound。
func (c *Client) GetByAttemptKey(ctx context.Context, attemptKey string) (*a2a.Task, error) {
	u := c.baseURL + "/mybot/attempts/" + url.PathEscape(attemptKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, a2a.ErrTaskNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get by attempt key: unexpected HTTP status %s", resp.Status)
	}
	var task a2a.Task
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		return nil, err
	}
	return &task, nil
}

// Cancel 取消任务（A2A CancelTask）。对已取消的任务是幂等的。
func (c *Client) Cancel(ctx context.Context, taskID a2a.TaskID) (*a2a.Task, error) {
	return c.inner.CancelTask(ctx, &a2a.CancelTaskRequest{ID: taskID})
}

// Subscribe 订阅任务事件流（A2A SubscribeToTask，SSE）。
func (c *Client) Subscribe(ctx context.Context, taskID a2a.TaskID) iter.Seq2[a2a.Event, error] {
	return c.inner.SubscribeToTask(ctx, &a2a.SubscribeToTaskRequest{ID: taskID})
}

// bearerInterceptor 在每次 A2A 调用前附加 Bearer 认证头。
type bearerInterceptor struct {
	a2aclient.PassthroughInterceptor
	token string
}

func (b *bearerInterceptor) Before(ctx context.Context, req *a2aclient.Request) (context.Context, any, error) {
	if b.token != "" {
		req.ServiceParams["Authorization"] = []string{"Bearer " + b.token}
	}
	return ctx, nil, nil
}
