package bench

type WireJSON struct {
	Iface string  `json:"iface"`
	RxBps float64 `json:"rx_bps"`
	TxBps float64 `json:"tx_bps"`
	RxPct float64 `json:"rx_pct"`
	TxPct float64 `json:"tx_pct"`
}
