package model

import (
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/hash"
)

type BlockHeader struct {
	Number     int64       `json:"number"`
	ParentHash hash.Hash32 `json:"parent_hash"`
	Timestamp  int64       `json:"timestamp"`
	TxRoot     hash.Hash32 `json:"tx_root"`
	Nonce      uint64      `json:"-"`
}

type Block struct {
	Header BlockHeader `json:"header"`
	Hash   hash.Hash32 `json:"hash"`
	Txs    []Tx        `json:"txs"`
}

type Tx struct {
	Hash     hash.Hash32 `json:"hash"`
	TxBody   TxBody      `json:"tx_body"`
	BlockNum int64       `json:"block_num"`
}

type TxBody struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Token     string `json:"token"`
	Amount    int64  `json:"amount"`
	Timestamp int64  `json:"timestamp"`
	Nonce     uint64 `json:"-"`
}
