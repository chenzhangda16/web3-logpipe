package hash

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

// Builder builds a canonical byte sequence then hashes it to Hash32 (sha256).
//
// Encoding rules:
//   - Fixed-width integers: big-endian
//   - Bytes/string: u32(len) big-endian + bytes
//   - Hex strings (addresses/tx hash): normalize (trim 0x, lowercase), decode to bytes, then length-prefix
//
// This is intended for idempotency/dedup keys across Kafka/DB/Redis and for canonical hashing.
type Builder struct {
	b []byte
}

func NewBuilder() *Builder { return &Builder{b: make([]byte, 0, 128)} }

func (d *Builder) Reset() { d.b = d.b[:0] }

func (d *Builder) Bytes() []byte { return append([]byte(nil), d.b...) }

func (d *Builder) PutU64(v uint64) *Builder {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	d.b = append(d.b, buf[:]...)
	return d
}

func (d *Builder) PutI64(v int64) *Builder { return d.PutU64(uint64(v)) }

func (d *Builder) PutBool(v bool) *Builder {
	if v {
		d.b = append(d.b, 1)
	} else {
		d.b = append(d.b, 0)
	}
	return d
}

// PutBytes appends: u32(len) + bytes
func (d *Builder) PutBytes(p []byte) *Builder {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(p)))
	d.b = append(d.b, buf[:]...)
	d.b = append(d.b, p...)
	return d
}

func (d *Builder) PutString(s string) *Builder { return d.PutBytes([]byte(s)) }

// PutHexBytes normalizes "0x..." hex string -> raw bytes, length-prefixed.
// Useful for tx hash/address/token.
func (d *Builder) PutHexBytes(hexStr string) (*Builder, error) {
	s := strings.TrimSpace(hexStr)
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	s = strings.ToLower(s)
	if s == "" {
		// empty allowed; still deterministic
		return d.PutBytes(nil), nil
	}
	if len(s)%2 != 0 {
		// canonical: left-pad one nibble
		s = "0" + s
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return d, fmt.Errorf("hash: decode hex: %w", err)
	}
	d.PutBytes(b)
	return d, nil
}

func (d *Builder) Sum32() Hash32 {
	return sha256.Sum256(d.b)
}

// Convenience helpers

func SumU64(vals ...uint64) Hash32 {
	b := NewBuilder()
	for _, v := range vals {
		b.PutU64(v)
	}
	return b.Sum32()
}
