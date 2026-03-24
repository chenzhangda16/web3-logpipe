package bench

type ProcFlowJSON struct {
	Tag   string `json:"tag"`
	Tick  int64  `json:"tick"`
	TsMs  int64  `json:"ts_ms"`
	Phase string `json:"phase"`

	ReOff int64 `json:"re_off"`
	Blk   int64 `json:"blk"`

	MsgPS float64 `json:"msg_ps"`
	Msgs  int64   `json:"msgs"`

	Spool        ProcSpoolJSON          `json:"spool"`
	Decode       ProcDecodeJSON         `json:"decode"`
	RawSendBlock ProcBlockStageJSON     `json:"raw_send_block"`
	WinMove      ProcWinMoveJSON        `json:"winmove"`
	Q            ProcQueueJSON          `json:"q"`
	Wins         map[string]ProcWinJSON `json:"wins"`
}

type ProcSpoolJSON struct {
	Ok    int64 `json:"ok"`
	Err   int64 `json:"err"`
	AvgNs int64 `json:"avg_ns"`
	MaxNs int64 `json:"max_ns"`
}

type ProcDecodeJSON struct {
	Ok    int64 `json:"ok"`
	Err   int64 `json:"err"`
	AvgNs int64 `json:"avg_ns"`
	P50Ns int64 `json:"p50_ns"`
	P90Ns int64 `json:"p90_ns"`
	P99Ns int64 `json:"p99_ns"`
	MaxNs int64 `json:"max_ns"`
}

type ProcBlockStageJSON struct {
	Ev    int64 `json:"ev"`
	SumNs int64 `json:"sum_ns"`
	AvgNs int64 `json:"avg_ns"`
	MaxNs int64 `json:"max_ns"`
}

type ProcWinMoveJSON struct {
	N     int64                         `json:"n"`
	Block ProcBlockStageJSON            `json:"block"`
	W     map[string]ProcBlockStageJSON `json:"w"`
}

type ProcQueueJSON struct {
	Raw ProcQueueDepthJSON            `json:"raw"`
	Win map[string]ProcQueueDepthJSON `json:"win"`
}

type ProcQueueDepthJSON struct {
	Len int `json:"len"`
	Cap int `json:"cap"`
}

type ProcWinJSON struct {
	BusyPct   float64 `json:"busy_pct"`
	Moves     int64   `json:"moves"`
	AvgWaitNs int64   `json:"avg_wait_ns"`
	AvgWorkNs int64   `json:"avg_work_ns"`
	MaxWaitNs int64   `json:"max_wait_ns"`
	MaxWorkNs int64   `json:"max_work_ns"`
}

type ProcCoreJSON struct {
	CoreJSON
	W map[string]ProcCoreWin `json:"w"`
}

type ProcCoreWin struct {
	BusyPct   float64 `json:"busy_pct"`
	WorkCoreS float64 `json:"work_core_s"`
	WaitCoreS float64 `json:"wait_core_s"`
	Moves     int64   `json:"moves"`
}
