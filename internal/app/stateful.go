// 本文件实现 OpenAI Responses 的 stateful 语义与 Anthropic 辅助端点：
// store/previous_response_id 的服务端响应存储适配（createCompletion 的
// 解码前改写与完成后的落库）、GET/DELETE /v1/responses/{id} 及
// input_items/cancel 取回端点、POST /v1/messages/count_tokens 本地
// token 估算。这些端点都不触上游、不占并发槽（同 /v1/models 口径）。
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/WncFht/devin2api/internal/api/anthropic/messages"
	"github.com/WncFht/devin2api/internal/api/common"
	"github.com/WncFht/devin2api/internal/llm"
	"github.com/WncFht/devin2api/internal/store"
)

// maxResponseChainDepth 是 previous_response_id 续链物化的祖先深度上限：
// 链由插入时的既有行 id 构成，环结构不可能出现，上限只防病态长链把
// 单次物化变成整库回放——超限链拒绝继续增长，客户端须重发全量。
const maxResponseChainDepth = 32

// statefulPersistTimeout 是响应完成后的落库写时限：响应已交付客户端，
// 落库失败只留 WARN，不该拖住请求收尾。
const statefulPersistTimeout = 5 * time.Second

// statefulTurn 记录一次 openai-responses 请求的 stateful 适配产物：
// 解码前由 prepareResponsesStateful 填前四个字段，响应完成后由
// persistStateful 消费 responseJSON 落库。
type statefulTurn struct {
	// store 是客户端是否要求存储本次响应（store:true；缺省 false，
	// 与本代理历史声明的 store=false 缺省一致）。
	store bool
	// parentID 是续链父响应 id；空表示独立请求。
	parentID string
	// keyHash 是下游凭据哈希，存储行的租户隔离键。
	keyHash string
	// inputJSON 是归一化成 item 数组的客户端原始 input（增量部分），
	// 落库后供下一轮续链物化。
	inputJSON []byte
	// responseJSON 是完成后回填的定稿 Response 对象（流式取自终帧、
	// 非流式取自 EncodeFinal 产物）；空表示响应未成功完成、不落库。
	responseJSON []byte
}

// prepareResponsesStateful 在解码前消费请求体里的 store 与
// previous_response_id：续链请求按 key_hash 隔离上溯祖先链，把增量
// input 物化成「祖先 input+output 全量 + 本次增量」后改写 body
// （剥离 previous_response_id——与 WS 会话同口径，管线只认全量），
// store 请求登记落库意图。未接线存储仓或请求无 stateful 字段时原样
// 返回（previous_response_id 仍会被 protocols.go 的安全网 400 拒绝）。
// status 非 0 表示管线前拒绝（404 链断 / 400 链过深 / body 畸形）。
func (application *App) prepareResponsesStateful(ctx context.Context, body []byte, keyHash string) (turn *statefulTurn, rewritten []byte, status int, err error) {
	if application.responsesStore == nil {
		return nil, body, 0, nil
	}
	var probe struct {
		Store    bool            `json:"store"`
		Previous string          `json:"previous_response_id"`
		Input    json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, body, http.StatusBadRequest, fmt.Errorf("decode responses request: %w", err)
	}
	if !probe.Store && probe.Previous == "" {
		return nil, body, 0, nil
	}
	// 续链要求存储：增量 input 的正确性依赖整条链可回放，而回放依赖
	// 本次响应入库成为下一轮祖先——store:false 的续链在 OpenAI 侧同样
	// 不成立，显式 400 比静默断链便宜。
	if probe.Previous != "" && !probe.Store {
		return nil, body, http.StatusBadRequest,
			errors.New("previous_response_id requires store=true; resend the full conversation input without previous_response_id")
	}
	turn = &statefulTurn{store: probe.Store, parentID: probe.Previous, keyHash: keyHash}
	turn.inputJSON, err = normalizeResponsesInput(probe.Input)
	if err != nil {
		return nil, body, http.StatusBadRequest, err
	}
	if probe.Previous == "" {
		return turn, body, 0, nil
	}
	chain, err := application.responsesStore.ResponseChain(ctx, probe.Previous, keyHash, maxResponseChainDepth)
	switch {
	case errors.Is(err, store.ErrResponseNotFound):
		return nil, body, http.StatusNotFound, fmt.Errorf("no response found with id '%s' (previous_response_id)", probe.Previous)
	case errors.Is(err, store.ErrResponseChainTooDeep):
		return nil, body, http.StatusBadRequest, fmt.Errorf("previous_response_id chain from '%s' exceeds %d ancestors; resend the full conversation input", probe.Previous, maxResponseChainDepth)
	case err != nil:
		return nil, body, http.StatusInternalServerError, fmt.Errorf("resolve response chain from '%s': %w", probe.Previous, err)
	}
	merged, err := materializeChainInput(chain, turn.inputJSON)
	if err != nil {
		return nil, body, http.StatusInternalServerError, err
	}
	// map[string]json.RawMessage 的往返不建值树：未触碰字段原样搬运，
	// 只有 input 被替换、previous_response_id 被剥离。
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, body, http.StatusBadRequest, fmt.Errorf("decode responses request: %w", err)
	}
	doc["input"] = merged
	delete(doc, "previous_response_id")
	rewritten, err = json.Marshal(doc)
	if err != nil {
		return nil, body, http.StatusInternalServerError, fmt.Errorf("rewrite stateful input: %w", err)
	}
	return turn, rewritten, 0, nil
}

// normalizeResponsesInput 把 Responses input 的三种形态归一成 item 数组：
// 缺席/空白 → 空数组；字符串 → 单条 user message item；数组原样。
// 其余形态是请求错误——落库前不定案，续链物化会把畸形传染给下一轮。
func normalizeResponsesInput(input json.RawMessage) ([]byte, error) {
	if common.JSONBlank(input) {
		return []byte("[]"), nil
	}
	var text string
	if json.Unmarshal(input, &text) == nil {
		return json.Marshal([]json.RawMessage{json.RawMessage(fmt.Sprintf(
			`{"type":"message","role":"user","content":[{"type":"input_text","text":%s}]}`,
			mustQuoteString(text)))})
	}
	var items []json.RawMessage
	if err := json.Unmarshal(input, &items); err != nil {
		return nil, fmt.Errorf("decode responses input: %w", err)
	}
	return json.Marshal(items)
}

// mustQuoteString 把文本编码成 JSON 字符串字面量（含转义）。
func mustQuoteString(text string) string {
	data, _ := json.Marshal(text)
	return string(data)
}

// materializeChainInput 把祖先链（链首→链尾）的 input 与 output items
// 依次展开，尾部接本次增量 input，产出续链请求的全量 input 数组。
// output items 从存储的 Response 对象 output 键提取——reasoning 签名、
// function_call 等都是我们自己下发过的回放形态，输入解码器原样认领。
func materializeChainInput(chain []store.StoredResponse, input []byte) ([]byte, error) {
	items := make([]json.RawMessage, 0, 16)
	appendItems := func(raw json.RawMessage, source string) error {
		var parsed []json.RawMessage
		if err := json.Unmarshal(raw, &parsed); err != nil {
			return fmt.Errorf("stored response %s %s is malformed: %w", source, "payload", err)
		}
		items = append(items, parsed...)
		return nil
	}
	for _, link := range chain {
		if err := appendItems(link.InputJSON, link.ID); err != nil {
			return nil, err
		}
		var response struct {
			Output []json.RawMessage `json:"output"`
		}
		if err := json.Unmarshal(link.ResponseJSON, &response); err != nil {
			return nil, fmt.Errorf("stored response %s payload is malformed: %w", link.ID, err)
		}
		items = append(items, response.Output...)
	}
	if err := appendItems(input, "request"); err != nil {
		return nil, err
	}
	return json.Marshal(items)
}

// persistStateful 在响应成功交付后把定稿 Response 对象落库；落库失败
// 只留 WARN——存储是尽力而为的增值面，不该让已完成的请求报错。
// turn 为 nil（无 stateful 语义）、未要求 store、或 responseJSON 未回填
// （流中途失败）时静默跳过。
func (application *App) persistStateful(requestCtx context.Context, turn *statefulTurn) {
	if application.responsesStore == nil || turn == nil || !turn.store || len(turn.responseJSON) == 0 {
		return
	}
	var response struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(turn.responseJSON, &response); err != nil || response.ID == "" {
		slog.Warn("stateful response persist skipped", "reason", "unmarshal final response", "error", err)
		return
	}
	if response.Status == "" {
		response.Status = "completed"
	}
	now := time.Now()
	stored := store.StoredResponse{
		ID: response.ID, ParentID: turn.parentID, KeyHash: turn.keyHash,
		Status: response.Status, CreatedAt: now.UnixMilli(),
		ExpiresAt: now.Add(store.ResponseTTL).UnixMilli(),
		InputJSON: turn.inputJSON, ResponseJSON: turn.responseJSON,
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), statefulPersistTimeout)
	defer cancel()
	if err := application.responsesStore.InsertResponse(ctx, stored); err != nil {
		slog.Warn("stateful response persist failed", "response_id", response.ID, "error", err)
	}
}

// storedResponseOr404 是取回端点的公共前置：按 URL 的 responseID 与
// 请求凭据哈希查存储行；nil 仓或查无此行统一回 OpenAI 形状的 404。
func (application *App) storedResponseOr404(writer http.ResponseWriter, request *http.Request) *store.StoredResponse {
	id := chi.URLParam(request, "responseID")
	if application.responsesStore == nil || id == "" {
		writeJSONError(writer, http.StatusNotFound, fmt.Sprintf("No response found with id '%s'.", id), "invalid_request_error")
		return nil
	}
	response, err := application.responsesStore.ResponseByID(request.Context(), id, requestCredentialHash(request))
	if errors.Is(err, store.ErrResponseNotFound) {
		writeJSONError(writer, http.StatusNotFound, fmt.Sprintf("No response found with id '%s'.", id), "invalid_request_error")
		return nil
	}
	if err != nil {
		slog.Warn("stored response lookup failed", "response_id", id, "error", err)
		writeJSONError(writer, http.StatusInternalServerError, "stored response lookup failed", "api_error")
		return nil
	}
	return &response
}

// getStoredResponse 是 GET /v1/responses/{id}：原样回放定稿 Response 对象。
func (application *App) getStoredResponse(writer http.ResponseWriter, request *http.Request) {
	response := application.storedResponseOr404(writer, request)
	if response == nil {
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(response.ResponseJSON)
}

// deleteStoredResponse 是 DELETE /v1/responses/{id}：删除存储行并回
// OpenAI 的删除确认信封。子链引用不级联，祖先缺失的链在续链时显式 404。
func (application *App) deleteStoredResponse(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "responseID")
	if application.responsesStore == nil || id == "" {
		writeJSONError(writer, http.StatusNotFound, fmt.Sprintf("No response found with id '%s'.", id), "invalid_request_error")
		return
	}
	deleted, err := application.responsesStore.DeleteResponse(request.Context(), id, requestCredentialHash(request))
	if err != nil {
		slog.Warn("stored response delete failed", "response_id", id, "error", err)
		writeJSONError(writer, http.StatusInternalServerError, "stored response delete failed", "api_error")
		return
	}
	if !deleted {
		writeJSONError(writer, http.StatusNotFound, fmt.Sprintf("No response found with id '%s'.", id), "invalid_request_error")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{"id": id, "object": "response", "deleted": true})
}

// cancelStoredResponse 是 POST /v1/responses/{id}/cancel：本代理只在
// 响应完成后落库（没有 background 态的进行中行可取消），已存响应一律
// 按其 status 拒绝取消——文案对齐 OpenAI 的同型错误。
func (application *App) cancelStoredResponse(writer http.ResponseWriter, request *http.Request) {
	response := application.storedResponseOr404(writer, request)
	if response == nil {
		return
	}
	status := response.Status
	if status == "" {
		status = "completed"
	}
	writeJSONError(writer, http.StatusBadRequest,
		fmt.Sprintf("Cannot cancel a response with status '%s'.", status), "invalid_request_error")
}

// listResponseInputItems 是 GET /v1/responses/{id}/input_items：回放落库
// 时归一化的 input item 数组；first_id/last_id 取首尾 item 的 id（缺席
// 为 null）。单行存储天然单页，has_more 恒 false。
func (application *App) listResponseInputItems(writer http.ResponseWriter, request *http.Request) {
	response := application.storedResponseOr404(writer, request)
	if response == nil {
		return
	}
	var items []json.RawMessage
	if err := json.Unmarshal(response.InputJSON, &items); err != nil {
		writeJSONError(writer, http.StatusInternalServerError, "stored response input is malformed", "api_error")
		return
	}
	firstID, lastID := itemIDBounds(items)
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"object": "list", "data": items,
		"first_id": firstID, "last_id": lastID, "has_more": false,
	})
}

// itemIDBounds 取 item 数组首尾元素的 id 字段；数组为空或元素无 id 时
// 该侧为 nil。
func itemIDBounds(items []json.RawMessage) (first, last any) {
	probe := func(raw json.RawMessage) any {
		var item struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &item) == nil && item.ID != "" {
			return item.ID
		}
		return nil
	}
	if len(items) == 0 {
		return nil, nil
	}
	return probe(items[0]), probe(items[len(items)-1])
}

// count_tokens 估算常量：bytesPerToken 与适配器前缀保温的
// warmBytesPerToken 同一口径（bytes/4）；媒体块 IR 不带尺寸，按固定
// 粗估计入（真实成本随媒体分辨率/页数浮动，估算只做容量参考）。
const (
	countTokensBytesPerToken = 4
	countTokensMediaTokens   = 1024
)

// countTokens 是 POST /v1/messages/count_tokens：Anthropic 形态的本地
// input token 估算，不触上游、不产生上游计费。请求体按 /v1/messages
// 同一解码器解析（含工具展平与校验），估算覆盖 system、工具声明与
// 全部消息内容；结果对Claude Code 等客户端的预检是参考值而非上游
// 计费口径。
func (application *App) countTokens(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 32<<20))
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		failure := llm.Classify(err)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(status)
		_, _ = writer.Write(anthropicProtocol{}.EncodeHTTPError(httpError{
			Failure: failure, ClientFixable: status < 500, Stage: "http_read",
		}))
		return
	}
	adapted, err := messages.DecodeRequest(body, false)
	if err != nil {
		failure := llm.Classify(err)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write(anthropicProtocol{}.EncodeHTTPError(httpError{
			Failure: failure, ClientFixable: true, Stage: "http_decode",
		}))
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"input_tokens": estimateRequestTokens(adapted.Context),
	})
}

// estimateRequestTokens 按 bytes/4 口径估算中间请求的 input token 量：
// system 提示、工具声明（展平后 JSON）与全部消息内容块依次累计；
// 思考签名随思考正文计入（回放时它们真实上行），图片/文档/视频按
// 固定值粗估。
func estimateRequestTokens(context llm.RequestMessages) int {
	total := len(context.SystemPrompt)
	for _, tool := range context.Tools {
		data, err := json.Marshal(tool)
		if err == nil {
			total += len(data)
		}
	}
	for _, message := range context.Messages {
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
				total += len(typed.Text)
			case llm.ThinkingContent:
				total += len(typed.Thinking) + len(typed.Signature)
			case llm.ToolCall:
				total += len(typed.Name) + len(typed.Arguments)
			case llm.ImageContent, llm.DocumentContent, llm.VideoContent:
				total += countTokensMediaTokens * countTokensBytesPerToken
			}
		}
	}
	return (total + countTokensBytesPerToken - 1) / countTokensBytesPerToken
}
