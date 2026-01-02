package hash

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Hash32 [32]byte

var (
	ErrInvalidHex   = errors.New("invalid hex")
	ErrInvalidLen   = errors.New("invalid hash length")
	ErrEmptyHashStr = errors.New("empty hash string")
)

// Hex 返回带 0x 前缀的小写 hex
func (h Hash32) Hex() string {
	return "0x" + hex.EncodeToString(h[:])
}

func (h Hash32) Bytes() []byte {
	return append([]byte(nil), h[:]...)
}

func ByteSlice2Hash32(b []byte) (Hash32, error) {
	if len(b) != 32 {
		return Hash32{}, fmt.Errorf("invalid hash length: %d", len(b))
	}

	var h Hash32
	copy(h[:], b)
	return h, nil
}

func String2Hash32(s string) (Hash32, error) {
	var h Hash32

	s = strings.TrimSpace(s)
	if s == "" {
		return h, ErrEmptyHashStr
	}
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")

	if len(s) != 64 {
		return h, fmt.Errorf("%w: want 64 hex chars, got %d", ErrInvalidLen, len(s))
	}

	b, err := hex.DecodeString(s)
	if err != nil {
		return h, fmt.Errorf("%w: %v", ErrInvalidHex, err)
	}
	copy(h[:], b)
	return h, nil
}

// IsZero 判断是否全 0
func (h Hash32) IsZero() bool {
	var z Hash32
	return h == z
}

func (h Hash32) MarshalText() ([]byte, error) {
	// JSON 最终会把这个 []byte 当字符串输出
	return []byte(h.Hex()), nil
}

func (h *Hash32) UnmarshalText(text []byte) error {
	parsed, err := String2Hash32(string(text))
	if err != nil {
		return err
	}
	*h = parsed
	return nil
}

func (h Hash32) MarshalJSON() ([]byte, error) {
	// 强制 JSON 字符串
	return json.Marshal(h.Hex())
}

func (h *Hash32) UnmarshalJSON(data []byte) error {
	// 允许 "0x..." 或 null
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*h = Hash32{}
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	return h.UnmarshalText([]byte(s))
}
