package event

type TxEvent struct {
	Seq    uint64 // Dispatcher 分配，全局顺序
	Ts     int64  // Unix 秒或毫秒（避免 time.Time）
	From   uint64 // addressID
	To     uint64
	Token  uint32
	Amount int64
}
