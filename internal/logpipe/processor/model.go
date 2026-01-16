package processor

// TxEvent is the unit fed into sliding window.
// You can extend it with token, amount, etc.
type TxEvent struct {
	TxHash    string
	From      string
	To        string
	Timestamp int64
	BlockNum  int64
	BlockHash string
}
