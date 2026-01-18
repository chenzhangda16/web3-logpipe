package ids

import (
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
)

type TokenID struct {
	next  atomic.Uint32
	parts []tokenShard

	// Optional reverse lookup: id -> token string
	revMu sync.Mutex
	rev   []string
}

type tokenShard struct {
	mu sync.RWMutex
	m  map[string]uint32
}

func NewTokenID(shards int, initialPerShard int) *TokenID {
	if shards <= 0 {
		shards = 32
	}
	if initialPerShard < 0 {
		initialPerShard = 0
	}
	t := &TokenID{
		parts: make([]tokenShard, shards),
		rev:   make([]string, 0, shards*max(1, initialPerShard/2)),
	}
	for i := range t.parts {
		t.parts[i].m = make(map[string]uint32, initialPerShard)
	}
	return t
}

// ID returns stable ID for token string (normalized).
// You can remove normalization if token strings are already canonical.
func (t *TokenID) ID(token string) uint32 {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0
	}
	sh := &t.parts[t.pick(token)]

	sh.mu.RLock()
	if id, ok := sh.m[token]; ok {
		sh.mu.RUnlock()
		return id
	}
	sh.mu.RUnlock()

	sh.mu.Lock()
	if id, ok := sh.m[token]; ok {
		sh.mu.Unlock()
		return id
	}
	id := t.next.Add(1) // IDs start from 1
	sh.m[token] = id
	sh.mu.Unlock()

	t.revMu.Lock()
	t.rev = append(t.rev, token)
	t.revMu.Unlock()

	return id
}

func (t *TokenID) TokenByID(id uint32) (string, bool) {
	if id == 0 {
		return "", false
	}
	t.revMu.Lock()
	defer t.revMu.Unlock()
	idx := int(id - 1)
	if idx < 0 || idx >= len(t.rev) {
		return "", false
	}
	return t.rev[idx], true
}

func (t *TokenID) pick(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % uint32(len(t.parts)))
}
