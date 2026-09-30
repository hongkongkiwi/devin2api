// 编码器拒绝合法事件时的收口契约：200 已提交的流不得无声截断——
// writeProtocolStream 在返回原编码错误前，先合成协议内的错误终结帧
// 下发并落调试记录；未提交的流保持真实状态码路径不抢先吐帧。
package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/WncFht/devin2api/internal/config"
	"github.com/WncFht/devin2api/internal/llm"
)

// TestStreamEncoderFailureEmitsErrorFrameAfterCommit 驱动 anthropic 流到
// 「索引错位的 text delta 被编码器显式拒绝」（message_start 已提交 200），
// 断言客户端在流尾拿到 event: error 而非无声截断。
func TestStreamEncoderFailureEmitsErrorFrameAfterCommit(t *testing.T) {
	fake := &fakeAdapter{events: []llm.ResponseEvent{
		{Type: llm.ResponseEventStart, Partial: &llm.AssistantMessage{Model: "claude-test"}},
		{Type: llm.ResponseEventTextStart, ContentIndex: 0, Partial: &llm.AssistantMessage{Model: "claude-test"}},
		{Type: llm.ResponseEventTextDelta, ContentIndex: 5, Delta: "orphan"},
	}}
	application := New(fake, config.ServerConfig{Listen: ":0"}, nil)
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-test","messages":[{"role":"user","content":"hi"}],"max_tokens":256,"stream":true}`))
	response := httptest.NewRecorder()
	application.Router().ServeHTTP(response, request)
	body := response.Body.String()
	if !strings.Contains(body, "event: message_start") {
		t.Fatalf("stream should have committed message_start before the failure: %s", body)
	}
	if !strings.Contains(body, "event: error") {
		t.Fatalf("committed stream must end with an in-band error frame: %s", body)
	}
}
