package bench

type FetchFlowJSON struct {
	Tag  string `json:"tag"`
	Tick int64  `json:"tick"`
	TsMs int64  `json:"ts_ms"`

	RPC FetchRPCJSON `json:"rpc"`
	Blk FetchBlkJSON `json:"blk"`

	Event      FetchEventJSON      `json:"event"`
	InputBlock FetchBlockStageJSON `json:"input_block"`
	Q          FetchQueueJSON      `json:"q"`
}

type FetchRPCJSON struct {
	PPS   float64 `json:"pps"`
	BPS   float64 `json:"bps"`
	Ok    int64   `json:"ok"`
	Err   int64   `json:"err"`
	AvgNs int64   `json:"avg_ns"`
	P50Ns int64   `json:"p50_ns"`
	P90Ns int64   `json:"p90_ns"`
	P99Ns int64   `json:"p99_ns"`
	MaxNs int64   `json:"max_ns"`
}

type FetchBlkJSON struct {
	EnqBPS float64 `json:"enq_bps"`
	AckBPS float64 `json:"ack_bps"`
}

type FetchEventJSON struct {
	ProdErr  int64 `json:"prod_err"`
	LagFatal int64 `json:"lag_fatal"`
	CkptSave int64 `json:"ckpt_save"`
}

type FetchBlockStageJSON struct {
	SumNs int64 `json:"sum_ns"`
	Ev    int64 `json:"ev"`
	AvgNs int64 `json:"avg_ns"`
	MaxNs int64 `json:"max_ns"`
}

type FetchQueueJSON struct {
	Req  FetchQueueDepthJSON `json:"req"`
	Resp FetchQueueDepthJSON `json:"resp"`
}

type FetchQueueDepthJSON struct {
	Len int `json:"len"`
	Cap int `json:"cap"`
}
