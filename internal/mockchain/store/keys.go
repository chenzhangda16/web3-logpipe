package store

import "fmt"

const (
	keyHead = "meta:head"
)

func KeyHead() []byte { return []byte(keyHead) }

func KeyBlock(n int64) []byte {
	return []byte(fmt.Sprintf("block:%020d", n)) // 固定宽度便于按字典序范围扫
}
