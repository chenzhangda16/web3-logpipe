package mockchain

import (
	"crypto/rand"
	"encoding/hex"
)

func randomHash() string {
	b := make([]byte, 32) // 32 bytes = 256-bit hash
	_, _ = rand.Read(b)
	return "0x" + hex.EncodeToString(b)
}
