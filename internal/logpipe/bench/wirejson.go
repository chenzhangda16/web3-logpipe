package bench

type WireJSON struct {
	Tag   string  `json:"tag"`
	Tick  int64   `json:"tick"`
	TsMs  int64   `json:"ts_ms"`
	Iface string  `json:"iface"`
	RxBps float64 `json:"rx_bps"`
	TxBps float64 `json:"tx_bps"`
	RxPct float64 `json:"rx_pct"`
	TxPct float64 `json:"tx_pct"`
}
