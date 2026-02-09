package store

import (
	"strconv"

	"github.com/chenzhangda16/web3-logpipe/pkg/hash"
)

const (
	keyHeadHash = "meta:head_hash"
	keyHeadNum  = "meta:head_num"
)

func KeyHeadHash() []byte { return []byte(keyHeadHash) }

func KeyHeadNum() []byte { return []byte(keyHeadNum) }

func KeyCanon(n int64) []byte {
	return []byte("canon:" + strconv.FormatInt(n, 10))
}

func KeyBlockHash(h hash.Hash32) []byte {
	return []byte("block_hash:" + h.Hex())
}
