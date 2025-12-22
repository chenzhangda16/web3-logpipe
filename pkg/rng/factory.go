package rng

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

type Mode int

const (
	Deterministic Mode = iota
	Real
)

type Factory struct {
	baseSeed int64
	mode     Mode

	mu      sync.Mutex
	streams map[string]*rand.Rand
}

func New(mode Mode, seed int64) *Factory {
	if mode == Real {
		// Real 模式：只在这里用一次时间 seed（初始化状态），而不是每次取随机数都用时间
		seed = time.Now().UnixNano()
	}
	return &Factory{
		baseSeed: seed,
		mode:     mode,
		streams:  make(map[string]*rand.Rand),
	}
}

// R 返回一个“命名随机流”。第一次调用会初始化并缓存。
// 你可以在 hot path 里先把常用流取出来存字段里，避免反复 map 查找。
func (f *Factory) R(name string) *rand.Rand {
	f.mu.Lock()
	defer f.mu.Unlock()

	if r, ok := f.streams[name]; ok {
		return r
	}
	s := deriveSeed(f.baseSeed, name)
	r := rand.New(rand.NewSource(s))
	f.streams[name] = r
	return r
}

func deriveSeed(base int64, name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum64()) ^ base
}
