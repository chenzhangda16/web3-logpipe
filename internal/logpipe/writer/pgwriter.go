package writer

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/out"
)

type PGWriter struct {
	db *sql.DB
}

// NewPGWriterFromEnv: 最省事的连接方式
// 需要环境变量：PG_DSN
// 示例：postgres://user:pass@127.0.0.1:5432/web3log?sslmode=disable
func NewPGWriterFromEnv() (*PGWriter, error) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("PG_DSN is empty")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	// 可选：调一点连接池参数（你先跑通就行）
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &PGWriter{db: db}, nil
}

func (w *PGWriter) Close() error {
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}

// EnsureSchema: 把 schema.sql 里的内容复制进来执行（避免读文件路径麻烦）
// 你后面想优雅点再用 embed.FS。
func (w *PGWriter) EnsureSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS win_ticks (
  id       bigserial PRIMARY KEY,
  ts       timestamptz NOT NULL DEFAULT now(),
  win_idx  int         NOT NULL,
  head     bigint      NOT NULL,
  tail     bigint      NOT NULL,
  openwin  boolean     NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_win_ticks_ts ON win_ticks(ts);
CREATE INDEX IF NOT EXISTS idx_win_ticks_win_idx_ts ON win_ticks(win_idx, ts);
`
	_, err := w.db.ExecContext(ctx, ddl)
	return err
}

func (w *PGWriter) InsertWinTick(ctx context.Context, t out.WinTick) error {
	_, err := w.db.ExecContext(ctx,
		`INSERT INTO win_ticks(win_idx, head, tail, openwin) VALUES ($1,$2,$3,$4)`,
		t.WinIdx, t.Head, t.Tail, t.OpenWin,
	)
	return err
}

func (w *PGWriter) Run(ctx context.Context, in <-chan any) error {
	// 最小实现：逐条写。后面你要 batch 再优化。
	const ins = `INSERT INTO win_ticks(win_idx, head, tail, openwin) VALUES ($1,$2,$3,$4)`

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-in:
			if !ok {
				return nil
			}
			switch x := v.(type) {
			case out.WinTick:
				if _, err := w.db.ExecContext(ctx, ins, x.WinIdx, x.Head, x.Tail, x.OpenWin); err != nil {
					return err
				}
			default:
				// 先忽略未知 out 类型；你后面加类型再扩展 switch
			}
		}
	}
}
