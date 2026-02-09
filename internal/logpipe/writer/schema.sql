CREATE TABLE IF NOT EXISTS win_ticks (
                                         id       bigserial PRIMARY KEY,
                                         ts       timestamptz NOT NULL DEFAULT now(),
    win_idx  int         NOT NULL,
    head     bigint      NOT NULL,
    tail     bigint      NOT NULL,
    openwin  boolean     NOT NULL
    );

-- 幂等关键：同一个 (win_idx, head, tail) 视为同一条 tick 状态快照
ALTER TABLE win_ticks
    ADD CONSTRAINT IF NOT EXISTS uq_win_ticks_win_idx_head_tail
    UNIQUE (win_idx, head, tail);

CREATE INDEX IF NOT EXISTS idx_win_ticks_ts ON win_ticks(ts);
CREATE INDEX IF NOT EXISTS idx_win_ticks_win_idx_ts ON win_ticks(win_idx, ts);
