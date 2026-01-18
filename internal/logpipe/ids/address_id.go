package ids

import (
	"encoding/hex"
	"errors"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrBadAddress = errors.New("bad address")
)

// Addr20 is the canonical address key (20 bytes).
type Addr20 [20]byte

// ParseAddr20 parses "0x" prefixed or plain 40-hex string into 20 bytes.
// It does not allocate on success except temporary slicing; no big heap objects.
func ParseAddr20(s string) (Addr20, error) {
	var out Addr20
	s = strings.TrimSpace(s)
	if len(s) == 42 && (s[0:2] == "0x" || s[0:2] == "0X") {
		s = s[2:]
	}
	if len(s) != 40 {
		return out, ErrBadAddress
	}
	// hex.DecodeString allocates; use hex.Decode into fixed buffer to avoid.
	_, err := hex.Decode(out[:], []byte(s))
	if err != nil {
		return out, ErrBadAddress
	}
	return out, nil
}

// AddressID maps 20-byte addresses to uint64 IDs.
// - Thread-safe
// - Low GC: keys are fixed arrays, values are integers
// - Sharded map reduces lock contention
type AddressID struct {
	next  atomic.Uint64
	parts []addrShard

	// Optional reverse lookup for debug/UI (id -> Addr20).
	// If you don't need reverse, you can delete these fields.
	revMu sync.Mutex
	rev   []Addr20 // index = id-1
}

type addrShard struct {
	mu sync.RWMutex
	m  map[Addr20]uint64
}

func NewAddressID(shards int, initialPerShard int) *AddressID {
	if shards <= 0 {
		shards = 64
	}
	if initialPerShard < 0 {
		initialPerShard = 0
	}
	a := &AddressID{
		parts: make([]addrShard, shards),
		rev:   make([]Addr20, 0, shards*max(1, initialPerShard/2)),
	}
	for i := range a.parts {
		a.parts[i].m = make(map[Addr20]uint64, initialPerShard)
	}
	return a
}

// ID returns stable ID for an address string.
// ok=false if address is invalid.
func (a *AddressID) ID(addr string) (id uint64, ok bool) {
	k, err := ParseAddr20(addr)
	if err != nil {
		return 0, false
	}
	return a.ID20(k), true
}

// ID20 returns stable ID for an already-parsed 20-byte key.
func (a *AddressID) ID20(k Addr20) uint64 {
	sh := &a.parts[a.pick(k)]

	// Fast path: read lock
	sh.mu.RLock()
	if id, ok := sh.m[k]; ok {
		sh.mu.RUnlock()
		return id
	}
	sh.mu.RUnlock()

	// Slow path: write lock + double check
	sh.mu.Lock()
	if id, ok := sh.m[k]; ok {
		sh.mu.Unlock()
		return id
	}
	id := a.next.Add(1) // IDs start from 1
	sh.m[k] = id
	sh.mu.Unlock()

	// Optional reverse table (rare path: only first time per address)
	a.revMu.Lock()
	a.rev = append(a.rev, k)
	a.revMu.Unlock()

	return id
}

// Addr20ByID returns the canonical 20-byte address for debugging.
// ok=false if id out of range.
func (a *AddressID) Addr20ByID(id uint64) (Addr20, bool) {
	if id == 0 {
		return Addr20{}, false
	}
	a.revMu.Lock()
	defer a.revMu.Unlock()
	idx := int(id - 1)
	if idx < 0 || idx >= len(a.rev) {
		return Addr20{}, false
	}
	return a.rev[idx], true
}

// pick chooses shard index; using a cheap hash on bytes.
func (a *AddressID) pick(k Addr20) int {
	h := fnv.New32a()
	_, _ = h.Write(k[:])
	return int(h.Sum32() % uint32(len(a.parts)))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
