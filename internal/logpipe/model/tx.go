package model

type Tx struct {
	Hash      string `json:"hash"`
	From      string `json:"from"`
	To        string `json:"to"`
	Token     string `json:"token"`
	Amount    int64  `json:"amount"`
	Timestamp int64  `json:"timestamp"`
	BlockNum  int64  `json:"block_num"`
}

type Block struct {
	Number    int64 `json:"number"`
	Timestamp int64 `json:"timestamp"`
	Txs       []Tx  `json:"txs"`
}

type EnrichedEvent struct {
	Tx

	Whale      bool
	SelfLoop   bool
	ShortCycle bool
	HighFreqIO bool
	RiskScore  float64
	// 未来可加更多字段
}
