package dedup

// HotDeduper is an in-memory TTL deduper.
// Key is fixed [32]byte (e.g., tx hash) to avoid allocations.
type HotDeduper struct {
	m    map[[32]byte]int64 // key -> expireTs
	q    []hotItem          // insertion order
	head int                // pop index
}

type hotItem struct {
	key      [32]byte
	expireTs int64
}

func NewHotDeduper(capHint int) *HotDeduper {
	if capHint < 0 {
		capHint = 0
	}
	return &HotDeduper{
		m: make(map[[32]byte]int64, capHint),
		q: make([]hotItem, 0, capHint),
	}
}

// SeenOrAdd returns true if key exists and not expired at nowTs.
// If not seen (or expired), it records key with expireTs.
func (d *HotDeduper) SeenOrAdd(key [32]byte, expireTs int64, nowTs int64) bool {
	if exp, ok := d.m[key]; ok {
		if exp >= nowTs {
			return true
		}
		// expired: treat as not seen; overwrite below
	}
	d.m[key] = expireTs
	d.q = append(d.q, hotItem{key: key, expireTs: expireTs})
	return false
}

// Evict removes expired keys to bound memory.
func (d *HotDeduper) Evict(nowTs int64) {
	// Pop from queue while expired.
	for d.head < len(d.q) {
		it := d.q[d.head]
		if it.expireTs >= nowTs {
			break
		}
		// Only delete if map still points to this expireTs (avoid deleting overwritten keys).
		if exp, ok := d.m[it.key]; ok && exp == it.expireTs {
			delete(d.m, it.key)
		}
		d.head++
	}

	// Optional compaction to avoid slice growing forever
	if d.head > 4096 && d.head*2 > len(d.q) {
		newQ := make([]hotItem, 0, len(d.q)-d.head)
		newQ = append(newQ, d.q[d.head:]...)
		d.q = newQ
		d.head = 0
	}
}
