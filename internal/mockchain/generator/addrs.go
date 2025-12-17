package generator

import (
	"encoding/hex"
	"math/rand"
)

func GenAddrs(n int, seed int64) []string {
	rng := rand.New(rand.NewSource(seed))
	out := make([]string, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 20)
		_, _ = rng.Read(b)
		out[i] = "0x" + hex.EncodeToString(b)
	}
	return out
}
