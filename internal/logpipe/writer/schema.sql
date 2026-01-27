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
