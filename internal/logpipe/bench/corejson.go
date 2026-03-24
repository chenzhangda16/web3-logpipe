package bench

type CoreJSON struct {
	Tag        string  `json:"tag"`
	Tick       int64   `json:"tick"`
	TsMs       int64   `json:"ts_ms"`
	CpuPct     float64 `json:"cpu_pct"`
	Gomaxprocs int     `json:"gomaxprocs"`
	Goroutines int     `json:"goroutines"`
}
