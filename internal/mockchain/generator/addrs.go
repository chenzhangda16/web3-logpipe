package generator

import (
	"encoding/hex"
	"math/rand"
)

func GenAddrs(n int, rng *rand.Rand) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 20)
		_, _ = rng.Read(b)
		out[i] = "0x" + hex.EncodeToString(b)
	}
	return out
}
