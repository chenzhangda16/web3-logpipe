package processor

type Deduper struct {
	m    map[string]int64 // key -> expireTs
	q    []dedupItem      // insertion order
	head int              // pop index
}

type dedupItem struct {
	key      string
	expireTs int64
}

func NewDeduper(capHint int) *Deduper {
	if capHint < 0 {
		capHint = 0
	}
	return &Deduper{
		m: make(map[string]int64, capHint),
		q: make([]dedupItem, 0, capHint),
	}
}

// Seen returns true if key exists and not expired at nowTs.
// If not seen, it records with expireTs.
func (d *Deduper) SeenOrAdd(key string, expireTs int64, nowTs int64) bool {
	if exp, ok := d.m[key]; ok {
		if exp >= nowTs {
			return true
		}
		// expired: treat as not seen; overwrite below
	}
	d.m[key] = expireTs
	d.q = append(d.q, dedupItem{key: key, expireTs: expireTs})
	return false
}

func (d *Deduper) Evict(nowTs int64) {
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
		newQ := make([]dedupItem, 0, len(d.q)-d.head)
		newQ = append(newQ, d.q[d.head:]...)
		d.q = newQ
		d.head = 0
	}
}
