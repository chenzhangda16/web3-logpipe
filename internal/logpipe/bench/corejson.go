package bench

type CoreJSON struct {
	CpuPct     float64 `json:"cpu_pct"`
	Gomaxprocs int     `json:"gomaxprocs"`
	Goroutines int     `json:"goroutines"`
}
