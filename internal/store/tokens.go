package store

import (
	"context"
	"encoding/json"
	"strings"
)

// TokenRow 是 auth_tokens 表一行的领域形状，字段镜像
// authtoken.Token 的持久化字段；瞬态字段（inflight/rpm*）不在此。
// 时间一律 unix 毫秒；allowed_models 序列化为 JSON 数组文本。
type TokenRow struct {
	ID                       int64
	Token                    string
	Description              string
	CreatedAt                int64
	ExpiresAt                *int64
	LastUsedAt               *int64
	IsActive                 bool
	SuccessCount             int64
	FailureCount             int64
	StreamAvgTTFB            float64
	NonStreamAvgRT           float64
	StreamCount              int64
	NonStreamCount           int64
	PromptTokensTotal        int64
	CompletionTokensTotal    int64
	CacheReadTokensTotal     int64
	CacheCreationTokensTotal int64
	TotalCostUSD             float64
	EffectiveCostUSD         float64
	CostUsedMicroUSD         int64
	CostLimitMicroUSD        int64
	DailyUsedMicroUSD        int64
	DailyLimitMicroUSD       int64
	DailyPeriodStart         int64
	MonthlyUsedMicroUSD      int64
	MonthlyLimitMicroUSD     int64
	MonthlyPeriodStart       int64
	Cost5hUsedMicroUSD       int64
	Cost5hLimitMicroUSD      int64
	Cost5hAnchor             int64
	WeeklyUsedMicroUSD       int64
	WeeklyLimitMicroUSD      int64
	WeeklyPeriodStart        int64
	AllowedModels            []string
	MaxConcurrency           int
	MaxRPM                   int
	Class                    string
}

// tokenColumnList 是 auth_tokens 的全部列（含 id，37 列），
// INSERT/SELECT 共用同一份列清单，占位符数量由它派生。
var tokenColumnList = []string{
	"id", "token", "description", "created_at", "expires_at", "last_used_at", "is_active",
	"success_count", "failure_count", "stream_avg_ttfb", "non_stream_avg_rt", "stream_count", "non_stream_count",
	"prompt_tokens_total", "completion_tokens_total", "cache_read_tokens_total", "cache_creation_tokens_total",
	"total_cost_usd", "effective_cost_usd",
	"cost_used_microusd", "cost_limit_microusd",
	"cost_daily_used_microusd", "cost_daily_limit_microusd", "cost_daily_period_start",
	"cost_monthly_used_microusd", "cost_monthly_limit_microusd", "cost_monthly_period_start",
	"cost_5h_used_microusd", "cost_5h_limit_microusd", "cost_5h_anchor",
	"cost_weekly_used_microusd", "cost_weekly_limit_microusd", "cost_weekly_period_start",
	"allowed_models", "max_concurrency", "max_rpm", "class",
}

var (
	tokenColumns   = strings.Join(tokenColumnList, ", ")
	tokenInsertAll = `INSERT OR REPLACE INTO auth_tokens(` + tokenColumns + `) VALUES(` +
		placeholders(len(tokenColumnList)) + `)`
	tokenInsertAuto = `INSERT INTO auth_tokens(` + strings.Join(tokenColumnList[1:], ", ") + `) VALUES(` +
		placeholders(len(tokenColumnList)-1) + `)`
)

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func tokenArgs(t *TokenRow, allowed string) []any {
	return []any{
		t.ID, t.Token, t.Description, t.CreatedAt, t.ExpiresAt, t.LastUsedAt, t.IsActive,
		t.SuccessCount, t.FailureCount, t.StreamAvgTTFB, t.NonStreamAvgRT, t.StreamCount, t.NonStreamCount,
		t.PromptTokensTotal, t.CompletionTokensTotal, t.CacheReadTokensTotal, t.CacheCreationTokensTotal,
		t.TotalCostUSD, t.EffectiveCostUSD,
		t.CostUsedMicroUSD, t.CostLimitMicroUSD,
		t.DailyUsedMicroUSD, t.DailyLimitMicroUSD, t.DailyPeriodStart,
		t.MonthlyUsedMicroUSD, t.MonthlyLimitMicroUSD, t.MonthlyPeriodStart,
		t.Cost5hUsedMicroUSD, t.Cost5hLimitMicroUSD, t.Cost5hAnchor,
		t.WeeklyUsedMicroUSD, t.WeeklyLimitMicroUSD, t.WeeklyPeriodStart,
		allowed, t.MaxConcurrency, t.MaxRPM, t.Class,
	}
}

func scanToken(row sqlScanner) (*TokenRow, error) {
	var t TokenRow
	var allowed string
	err := row.Scan(&t.ID, &t.Token, &t.Description, &t.CreatedAt, &t.ExpiresAt, &t.LastUsedAt, &t.IsActive,
		&t.SuccessCount, &t.FailureCount, &t.StreamAvgTTFB, &t.NonStreamAvgRT, &t.StreamCount, &t.NonStreamCount,
		&t.PromptTokensTotal, &t.CompletionTokensTotal, &t.CacheReadTokensTotal, &t.CacheCreationTokensTotal,
		&t.TotalCostUSD, &t.EffectiveCostUSD,
		&t.CostUsedMicroUSD, &t.CostLimitMicroUSD,
		&t.DailyUsedMicroUSD, &t.DailyLimitMicroUSD, &t.DailyPeriodStart,
		&t.MonthlyUsedMicroUSD, &t.MonthlyLimitMicroUSD, &t.MonthlyPeriodStart,
		&t.Cost5hUsedMicroUSD, &t.Cost5hLimitMicroUSD, &t.Cost5hAnchor,
		&t.WeeklyUsedMicroUSD, &t.WeeklyLimitMicroUSD, &t.WeeklyPeriodStart,
		&allowed, &t.MaxConcurrency, &t.MaxRPM, &t.Class)
	if err != nil {
		return nil, err
	}
	if allowed != "" && allowed != "[]" {
		if err := json.Unmarshal([]byte(allowed), &t.AllowedModels); err != nil {
			return nil, err
		}
	}
	return &t, nil
}

// InsertToken 插入令牌行并返回自增 id；调用方持零 ID 传入。
func (s *Store) InsertToken(ctx context.Context, t *TokenRow) (int64, error) {
	allowed, err := json.Marshal(t.AllowedModels)
	if err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx, tokenInsertAuto, tokenArgs(t, string(allowed))[1:]...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// UpsertToken 按 id 覆盖整行——Token 的窗口折叠等域逻辑都在
// 调用方内存里完成，本方法只是持久化快照（替代整文件重写）。
// 管理面写（建号/改限额/启停）走这里：管理动作是即时且显式的意图，
// 快照即真相。统计写不走这里——见 ApplyTokenDelta。
func (s *Store) UpsertToken(ctx context.Context, t *TokenRow) error {
	allowed, err := json.Marshal(t.AllowedModels)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, tokenInsertAll, tokenArgs(t, string(allowed))...)
	return err
}

// TokenResultDelta 是一次完成请求对 auth_tokens 行的加性贡献：计数与
// 费用列是本次请求的增量而非行快照，窗口列携带提交进程翻窗后的窗口键。
// Success=false 时 token/费用列与窗口键必须全零——SQL 按 success_count
// 门控窗口列，失败增量不触碰任何窗口。
type TokenResultDelta struct {
	ID               int64
	LastUsedAt       int64
	Stream           bool
	LatencySampleSec float64 // Stream ? 单次首字秒 : 单次整响秒
	Success          bool
	PromptTokens     int64
	CompletionTokens int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	CostUSD          float64 // total/effective 列的加数（目录价估算）
	CostMicroUSD     int64   // 各费用窗口列的加数
	DayStart         int64   // 日窗键
	MonthStart       int64   // 月窗键
	Anchor5h         int64   // 5h 窗锚
	WeekAnchor       int64   // weekly 窗锚
}

// tokenDeltaUpsert 是 ApplyTokenDelta 的语句：INSERT 分支以增量值起新行
// （配置列取 base）；冲突分支计数与费用列 SQL 侧累加，均值列按
// 「旧均值×旧样本数 + 新样本×新样本数」合成（增量携带单样本，公式与
// 内存侧增量均值一致），四个费用窗口按窗口键同键累加、异窗以增量替换
// （翻窗）——reuseport 交接的重叠期里新旧进程同活同写，全量快照互覆
// 会抹掉先提交方的贡献，增量对任意交错收敛。
var tokenDeltaUpsert = `INSERT INTO auth_tokens(` + tokenColumns + `) VALUES(` +
	placeholders(len(tokenColumnList)) + `)
ON CONFLICT(id) DO UPDATE SET
	last_used_at = excluded.last_used_at,
	success_count = success_count + excluded.success_count,
	failure_count = failure_count + excluded.failure_count,
	stream_count = stream_count + excluded.stream_count,
	non_stream_count = non_stream_count + excluded.non_stream_count,
	stream_avg_ttfb = CASE WHEN excluded.stream_count > 0 THEN
		(stream_avg_ttfb * stream_count + excluded.stream_avg_ttfb * excluded.stream_count)
			/ (stream_count + excluded.stream_count)
		ELSE stream_avg_ttfb END,
	non_stream_avg_rt = CASE WHEN excluded.non_stream_count > 0 THEN
		(non_stream_avg_rt * non_stream_count + excluded.non_stream_avg_rt * excluded.non_stream_count)
			/ (non_stream_count + excluded.non_stream_count)
		ELSE non_stream_avg_rt END,
	prompt_tokens_total = prompt_tokens_total + excluded.prompt_tokens_total,
	completion_tokens_total = completion_tokens_total + excluded.completion_tokens_total,
	cache_read_tokens_total = cache_read_tokens_total + excluded.cache_read_tokens_total,
	cache_creation_tokens_total = cache_creation_tokens_total + excluded.cache_creation_tokens_total,
	total_cost_usd = total_cost_usd + excluded.total_cost_usd,
	effective_cost_usd = effective_cost_usd + excluded.effective_cost_usd,
	cost_used_microusd = cost_used_microusd + excluded.cost_used_microusd,
	cost_daily_period_start = CASE WHEN excluded.success_count > 0
		THEN excluded.cost_daily_period_start ELSE cost_daily_period_start END,
	cost_daily_used_microusd = CASE
		WHEN excluded.success_count = 0 THEN cost_daily_used_microusd
		WHEN cost_daily_period_start = excluded.cost_daily_period_start
			THEN cost_daily_used_microusd + excluded.cost_daily_used_microusd
		ELSE excluded.cost_daily_used_microusd END,
	cost_monthly_period_start = CASE WHEN excluded.success_count > 0
		THEN excluded.cost_monthly_period_start ELSE cost_monthly_period_start END,
	cost_monthly_used_microusd = CASE
		WHEN excluded.success_count = 0 THEN cost_monthly_used_microusd
		WHEN cost_monthly_period_start = excluded.cost_monthly_period_start
			THEN cost_monthly_used_microusd + excluded.cost_monthly_used_microusd
		ELSE excluded.cost_monthly_used_microusd END,
	cost_5h_anchor = CASE WHEN excluded.success_count > 0
		THEN excluded.cost_5h_anchor ELSE cost_5h_anchor END,
	cost_5h_used_microusd = CASE
		WHEN excluded.success_count = 0 THEN cost_5h_used_microusd
		WHEN cost_5h_anchor = excluded.cost_5h_anchor
			THEN cost_5h_used_microusd + excluded.cost_5h_used_microusd
		ELSE excluded.cost_5h_used_microusd END,
	cost_weekly_period_start = CASE WHEN excluded.success_count > 0
		THEN excluded.cost_weekly_period_start ELSE cost_weekly_period_start END,
	cost_weekly_used_microusd = CASE
		WHEN excluded.success_count = 0 THEN cost_weekly_used_microusd
		WHEN cost_weekly_period_start = excluded.cost_weekly_period_start
			THEN cost_weekly_used_microusd + excluded.cost_weekly_used_microusd
		ELSE excluded.cost_weekly_used_microusd END`

// ApplyTokenDelta 把一次完成请求的增量合入 auth_tokens 行。base 是提交
// 进程的当前内存投影，仅供 INSERT 分支补配置列（行缺席时按本次增量
// 起新行）；冲突分支不读 base。窗口键取提交进程翻窗后的内存值：同窗
// 正常相等（两进程水合同一锚），边界交错由替换分支吸收。
func (s *Store) ApplyTokenDelta(ctx context.Context, base *TokenRow, d *TokenResultDelta) error {
	allowed, err := json.Marshal(base.AllowedModels)
	if err != nil {
		return err
	}
	success, failure := 0, 0
	if d.Success {
		success = 1
	} else {
		failure = 1
	}
	streamSample, nonStreamSample := 0.0, 0.0
	streamCount, nonStreamCount := 0, 0
	if d.Stream {
		streamSample, streamCount = d.LatencySampleSec, 1
	} else {
		nonStreamSample, nonStreamCount = d.LatencySampleSec, 1
	}
	args := []any{
		d.ID, base.Token, base.Description, base.CreatedAt, base.ExpiresAt, d.LastUsedAt, base.IsActive,
		success, failure, streamSample, nonStreamSample, streamCount, nonStreamCount,
		d.PromptTokens, d.CompletionTokens, d.CacheReadTokens, d.CacheWriteTokens,
		d.CostUSD, d.CostUSD,
		d.CostMicroUSD, base.CostLimitMicroUSD,
		d.CostMicroUSD, base.DailyLimitMicroUSD, d.DayStart,
		d.CostMicroUSD, base.MonthlyLimitMicroUSD, d.MonthStart,
		d.CostMicroUSD, base.Cost5hLimitMicroUSD, d.Anchor5h,
		d.CostMicroUSD, base.WeeklyLimitMicroUSD, d.WeekAnchor,
		string(allowed), base.MaxConcurrency, base.MaxRPM, base.Class,
	}
	_, err = s.db.ExecContext(ctx, tokenDeltaUpsert, args...)
	return err
}

// ListTokens 返回全部令牌行（含停用），按 id 升序——与旧文件
// 的排序语义一致。
func (s *Store) ListTokens(ctx context.Context) ([]*TokenRow, error) {
	rows, err := s.ro.QueryContext(ctx, `SELECT `+tokenColumns+` FROM auth_tokens ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []*TokenRow
	for rows.Next() {
		t, err := scanToken(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// DeleteToken 按 id 删除令牌行。
func (s *Store) DeleteToken(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM auth_tokens WHERE id=?`, id)
	return err
}
