// 本文件实现 stateful Responses API 的服务端响应存储（responses 表）：
// 写方是 app 层在响应完成后的持久化（app/stateful.go），读方是取回端点
// 与 previous_response_id 续链物化。载荷按原样存取——本层不解读
// Response 对象与 input items 的内部形状。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ResponseTTL 是存储响应的保留时长；链式引用不续命——过期链在续链
// 物化时按「祖先缺失」拒绝，客户端须重发全量。OpenAI 侧同为 30 天。
const ResponseTTL = 30 * 24 * time.Hour

// StoredResponse 是 responses 表的一行。
type StoredResponse struct {
	// ID 是 resp_ 前缀的响应标识（Response 对象顶层 id）。
	ID string
	// ParentID 是 previous_response_id 续链的父响应 id；空表示链首。
	ParentID string
	// KeyHash 是下游凭据哈希（auth_tokens.key_hash 同口径），租户隔离
	// 边界——跨令牌不可互查、不可互链。匿名通道为空串，天然同桶。
	KeyHash string
	// Status 是 Response 顶层 status 字段副本（completed/incomplete）。
	Status string
	// CreatedAt/ExpiresAt 是落库与过期时刻（unix 毫秒）。
	CreatedAt int64
	ExpiresAt int64
	// InputJSON 是归一化成 item 数组的原始 input（字符串 input 在写侧
	// 已包成单条 user message item），续链物化按数组拼接。
	InputJSON []byte
	// ResponseJSON 是下发客户端的完整 Response 对象（终帧/非流式体），
	// 取回端点原样回放。
	ResponseJSON []byte
}

// ErrResponseNotFound 是按 id 查无此行（或 key_hash 不属当前租户）的
// 统一信号；调用方据此回 404。
var ErrResponseNotFound = errors.New("response not found")

// ErrResponseChainTooDeep 是续链祖先数超过链深上限的信号；超过上限的
// 链不允许继续增长。
var ErrResponseChainTooDeep = errors.New("response chain too deep")

// InsertResponse 落一行存储响应。同 id 重复落库（重试/竞态重放）按
// INSERT OR REPLACE 后写者胜——响应对象不可变，内容一致，无合并语义。
func (s *Store) InsertResponse(ctx context.Context, response StoredResponse) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO responses (id, parent_id, key_hash, status, created_at, expires_at, input_json, response_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		response.ID, response.ParentID, response.KeyHash, response.Status,
		response.CreatedAt, response.ExpiresAt, response.InputJSON, response.ResponseJSON)
	if err != nil {
		return fmt.Errorf("insert response %s: %w", response.ID, err)
	}
	return nil
}

// ResponseByID 取一条本租户的存储响应；查无此行返回 ErrResponseNotFound。
func (s *Store) ResponseByID(ctx context.Context, id, keyHash string) (StoredResponse, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT parent_id, status, created_at, expires_at, input_json, response_json
		 FROM responses WHERE id = ? AND key_hash = ?`, id, keyHash)
	response, err := scanStoredResponse(row, id)
	if err != nil {
		return StoredResponse{}, err
	}
	return response, nil
}

// DeleteResponse 删除一条本租户的存储响应；返回是否确实删了行。
// 子链引用不级联——祖先缺失的链在续链物化时显式拒绝。
func (s *Store) DeleteResponse(ctx context.Context, id, keyHash string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM responses WHERE id = ? AND key_hash = ?`, id, keyHash)
	if err != nil {
		return false, fmt.Errorf("delete response %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete response %s rows affected: %w", id, err)
	}
	return n > 0, nil
}

// ResponseChain 从 parentID 沿 parent_id 逐级上溯收集祖先（不含 parentID
// 自身的 id 查询行，返回序为链首→链尾），供续链时物化全量 input。
// 任一祖先缺失即 ErrResponseNotFound——链不完整时增量 input 无法安全
// 展开；深度超 maxDepth 报 ErrResponseChainTooDeep。
func (s *Store) ResponseChain(ctx context.Context, parentID, keyHash string, maxDepth int) ([]StoredResponse, error) {
	var chain []StoredResponse
	cursor := parentID
	for depth := 0; cursor != ""; depth++ {
		if depth >= maxDepth {
			return nil, ErrResponseChainTooDeep
		}
		link, err := s.ResponseByID(ctx, cursor, keyHash)
		if err != nil {
			return nil, err
		}
		chain = append(chain, link)
		cursor = link.ParentID
	}
	// 上溯得到的是链尾→链首，反转为物化顺序。
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

// PruneExpiredResponses 删除早于 before（unix 毫秒）过期的行，返回删除数。
func (s *Store) PruneExpiredResponses(ctx context.Context, before int64) (int64, error) {
	return s.deleteRowsChunked(ctx, "responses", `expires_at < ?`, before)
}

// rowScanner 抽出 QueryRow/Row 共有的 Scan 接口，两处查询共用解码。
type rowScanner interface {
	Scan(dest ...any) error
}

// scanStoredResponse 把一行查询结果解码成 StoredResponse；sql.ErrNoRows
// 翻译成 ErrResponseNotFound，其余失败原样上抛。
func scanStoredResponse(row rowScanner, id string) (StoredResponse, error) {
	var response StoredResponse
	response.ID = id
	if err := row.Scan(&response.ParentID, &response.Status, &response.CreatedAt,
		&response.ExpiresAt, &response.InputJSON, &response.ResponseJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return StoredResponse{}, ErrResponseNotFound
		}
		return StoredResponse{}, fmt.Errorf("scan response %s: %w", id, err)
	}
	return response, nil
}
