// 本文件验证 stateful Responses 存储（store/previous_response_id 落库、
// 续链物化、取回端点）与 Anthropic count_tokens 本地估算。
package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/WncFht/devin2api/internal/authtoken"
	"github.com/WncFht/devin2api/internal/config"
	"github.com/WncFht/devin2api/internal/llm"
)

// statefulEvents 产出一次「文本回复」的完整事件流，供非流式收口出
// 完整 Response 对象。
func statefulEvents(text string) []llm.ResponseEvent {
	partial := &llm.AssistantMessage{Content: []llm.Content{llm.TextContent{Text: text}}, StopReason: llm.StopReasonPending}
	final := &llm.AssistantMessage{ResponseModel: "gpt-test", Content: []llm.Content{llm.TextContent{Text: text}}, StopReason: llm.StopReasonStop}
	return []llm.ResponseEvent{
		{Type: llm.ResponseEventStart, Partial: &llm.AssistantMessage{StopReason: llm.StopReasonPending}},
		{Type: llm.ResponseEventTextStart, ContentIndex: 0, Partial: partial},
		{Type: llm.ResponseEventTextDelta, ContentIndex: 0, Delta: text, Partial: partial},
		{Type: llm.ResponseEventTextEnd, ContentIndex: 0, Content: text, Partial: partial},
		{Type: llm.ResponseEventDone, Reason: llm.StopReasonStop, Message: final},
	}
}

// doRequest 用给定凭据发一条无 body 的请求，返回响应。
func doRequest(t *testing.T, application *App, method, path, credential string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	recorder := httptest.NewRecorder()
	application.Router().ServeHTTP(recorder, request)
	return recorder
}

// postResponses 用给定凭据向 /v1/responses 发一条非流式请求，返回响应。
func postResponses(t *testing.T, application *App, credential, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	recorder := httptest.NewRecorder()
	application.Router().ServeHTTP(recorder, request)
	return recorder
}

// newStatefulApp 装配带响应存储的应用与配套 fake 适配器。
func newStatefulApp(t *testing.T, events []llm.ResponseEvent) (*App, *fakeAdapter) {
	t.Helper()
	fake := &fakeAdapter{events: events}
	application := New(fake, config.ServerConfig{Listen: ":0"}, nil)
	application.SetResponsesStore(openTokenDB(t))
	return application, fake
}

func TestStatefulResponseStoredAndRetrievable(t *testing.T) {
	application, _ := newStatefulApp(t, statefulEvents("assistant says hi"))
	recorder := postResponses(t, application, "", `{"model":"gpt-test","store":true,"input":"hi there"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("store request: status %d body %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["store"] != true {
		t.Fatalf("response should advertise store:true, got %v", response["store"])
	}
	id, _ := response["id"].(string)
	if !strings.HasPrefix(id, "resp_") {
		t.Fatalf("response id should be resp_ prefixed, got %q", id)
	}

	// 取回端点原样回放定稿对象。
	get := doRequest(t, application, http.MethodGet, "/v1/responses/"+id, "")
	if get.Code != http.StatusOK {
		t.Fatalf("retrieve: status %d body %s", get.Code, get.Body.String())
	}
	var retrieved map[string]any
	if err := json.Unmarshal(get.Body.Bytes(), &retrieved); err != nil {
		t.Fatalf("decode retrieved: %v", err)
	}
	if retrieved["id"] != id {
		t.Fatalf("retrieved id %v should equal %q", retrieved["id"], id)
	}

	// input_items 回放归一化的原始 input。
	items := doRequest(t, application, http.MethodGet, "/v1/responses/"+id+"/input_items", "")
	if items.Code != http.StatusOK {
		t.Fatalf("input_items: status %d body %s", items.Code, items.Body.String())
	}
	var list struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(items.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode input_items: %v", err)
	}
	if len(list.Data) != 1 {
		t.Fatalf("input_items should hold the single normalized input item, got %d", len(list.Data))
	}
	encoded, _ := json.Marshal(list.Data[0])
	if !strings.Contains(string(encoded), "hi there") {
		t.Fatalf("input item should carry the original text, got %s", encoded)
	}

	// cancel：已完结响应不可取消，按 status 拒绝。
	cancel := doRequest(t, application, http.MethodPost, "/v1/responses/"+id+"/cancel", "")
	if cancel.Code != http.StatusBadRequest {
		t.Fatalf("cancel completed response: status %d body %s", cancel.Code, cancel.Body.String())
	}

	// delete 后取回与续链都按 404 收口。
	del := doRequest(t, application, http.MethodDelete, "/v1/responses/"+id, "")
	if del.Code != http.StatusOK {
		t.Fatalf("delete: status %d body %s", del.Code, del.Body.String())
	}
	gone := doRequest(t, application, http.MethodGet, "/v1/responses/"+id, "")
	if gone.Code != http.StatusNotFound {
		t.Fatalf("retrieve after delete: status %d", gone.Code)
	}
	recorder = postResponses(t, application, "", `{"model":"gpt-test","store":true,"previous_response_id":"`+id+`","input":"next"}`)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("chain after delete: status %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestStatefulChainMaterializesFullContext(t *testing.T) {
	application, fake := newStatefulApp(t, statefulEvents("assistant says hi"))
	first := postResponses(t, application, "", `{"model":"gpt-test","store":true,"input":"hi there"}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first turn: status %d body %s", first.Code, first.Body.String())
	}
	var response struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode first response: %v", err)
	}

	// 增量续链：适配器收到的必须是「祖先 input+output 全量 + 增量」，
	// 新响应回显 previous_response_id 且自身入库。
	fake.events = statefulEvents("second answer")
	second := postResponses(t, application, "", `{"model":"gpt-test","store":true,"previous_response_id":"`+response.ID+`","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"second question"}]}]}`)
	if second.Code != http.StatusOK {
		t.Fatalf("second turn: status %d body %s", second.Code, second.Body.String())
	}
	var messages llm.RequestMessages
	if fake.lastRequest.Model != "gpt-test" || len(fake.lastRequest.Messages) != 3 {
		t.Fatalf("adapter should see the full 3-turn context, got %+v", fake.lastRequest)
	}
	messages = fake.lastRequest
	var transcript []string
	for _, message := range messages.Messages {
		transcript = append(transcript, messageContentBlocks(message)...)
	}
	joined := strings.Join(transcript, "\n")
	for _, want := range []string{"hi there", "assistant says hi", "second question"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("materialized context missing %q, got %s", want, joined)
		}
	}
	var secondResponse struct {
		ID                 string `json:"id"`
		PreviousResponseID string `json:"previous_response_id"`
		Store              bool   `json:"store"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondResponse); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if secondResponse.PreviousResponseID != response.ID {
		t.Fatalf("second response should echo previous_response_id %q, got %q", response.ID, secondResponse.PreviousResponseID)
	}
	if !secondResponse.Store {
		t.Fatalf("second response should advertise store:true")
	}
	// 第二轮只存增量 input：input_items 是「second question」一项。
	items := doRequest(t, application, http.MethodGet, "/v1/responses/"+secondResponse.ID+"/input_items", "")
	if items.Code != http.StatusOK || strings.Count(items.Body.String(), "\"type\":\"message\"") != 1 {
		t.Fatalf("second turn input_items should hold one incremental item, got %s", items.Body.String())
	}
}

// messageContentBlocks 把中间模型消息的文字面抽出（测试断言用）。
func messageContentBlocks(message llm.Message) []string {
	var texts []string
	var content []llm.Content
	switch typed := message.(type) {
	case llm.UserMessage:
		content = typed.Content
	case llm.AssistantMessage:
		content = typed.Content
	case llm.ToolResultMessage:
		content = typed.Content
	}
	for _, block := range content {
		switch typed := block.(type) {
		case llm.TextContent:
			texts = append(texts, typed.Text)
		case llm.ToolCall:
			texts = append(texts, typed.Name+" "+string(typed.Arguments))
		}
	}
	return texts
}

func TestStatefulChainRequiresStoreAndExistingRoot(t *testing.T) {
	application, _ := newStatefulApp(t, statefulEvents("hi"))
	// store 缺省 false 时续链显式 400——增量 input 没有可回放的链。
	recorder := postResponses(t, application, "", `{"model":"gpt-test","previous_response_id":"resp_missing","input":"hi"}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("chain without store: status %d body %s", recorder.Code, recorder.Body.String())
	}
	// 未知祖先 404。
	recorder = postResponses(t, application, "", `{"model":"gpt-test","store":true,"previous_response_id":"resp_missing","input":"hi"}`)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("chain from unknown root: status %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestStatefulResponsesAreTenantIsolated(t *testing.T) {
	db := openTokenDB(t)
	tokens, err := authtoken.New(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tokens.Close)
	for _, plain := range []string{"key-a", "key-b"} {
		if _, _, err := tokens.Ensure(plain, &authtoken.Token{Description: plain, IsActive: true}); err != nil {
			t.Fatal(err)
		}
	}
	fake := &fakeAdapter{events: statefulEvents("hi")}
	application := New(fake, config.ServerConfig{Listen: ":0"}, nil)
	application.SetResponsesStore(db)
	application.SetAuthTokens(tokens, nil)

	first := postResponses(t, application, "key-a", `{"model":"gpt-test","store":true,"input":"hi there"}`)
	if first.Code != http.StatusOK {
		t.Fatalf("key-a store: status %d body %s", first.Code, first.Body.String())
	}
	var response struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	// key-b 取回与续链 key-a 的响应都按 404 收口。
	cross := doRequest(t, application, http.MethodGet, "/v1/responses/"+response.ID, "key-b")
	if cross.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant retrieve: status %d", cross.Code)
	}
	cross = postResponses(t, application, "key-b", `{"model":"gpt-test","store":true,"previous_response_id":"`+response.ID+`","input":"hi"}`)
	if cross.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant chain: status %d body %s", cross.Code, cross.Body.String())
	}
}

func TestPreviousResponseIDRejectedWithoutStoreWiring(t *testing.T) {
	// 存储仓未接线（历史测试装配口径）：续链保持 store=false 时代的 400。
	application := New(&fakeAdapter{events: statefulEvents("hi")}, config.ServerConfig{Listen: ":0"}, nil)
	recorder := postResponses(t, application, "", `{"model":"gpt-test","store":true,"previous_response_id":"resp_x","input":"hi"}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unwired store chain: status %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestCountTokensEstimatesLocally(t *testing.T) {
	application := New(&fakeAdapter{}, config.ServerConfig{Listen: ":0"}, nil)
	body := `{"model":"claude-test","system":"be brief","messages":[{"role":"user","content":"hello token counter, how many tokens is this?"}]}`
	recorder := httptest.NewRecorder()
	application.Router().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(body)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("count_tokens: status %d body %s", recorder.Code, recorder.Body.String())
	}
	var result struct {
		InputTokens int `json:"input_tokens"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode count_tokens: %v", err)
	}
	if result.InputTokens <= 0 {
		t.Fatalf("input_tokens should be positive, got %d", result.InputTokens)
	}

	// 畸形体按 Anthropic 错误信封 400。
	recorder = httptest.NewRecorder()
	application.Router().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(`{"messages":`)))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("malformed count_tokens: status %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"type":"error"`) {
		t.Fatalf("error body should use the Anthropic envelope, got %s", recorder.Body.String())
	}
}
