package processor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/tecbot/gorocksdb"
)

type LongDeduper interface {
	SeenOrAdd(key32 []byte, nowTs int64, ttlSec int64) (seen bool, err error)
	Evict(nowTs int64) error
	Close()
}

type RocksLongDeduper struct {
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions

	bucketSec int64

	// eviction progress (bucket index)
	lastCleanedBucket int64
}

func OpenRocksLongDeduper(path string, bucketSec int64) (*RocksLongDeduper, error) {
	if bucketSec <= 0 {
		return nil, errors.New("bucketSec must be > 0")
	}
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	// Optional: tune a bit; keep minimal now
	opts.IncreaseParallelism(2)

	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}

	d := &RocksLongDeduper{
		db:        db,
		ro:        gorocksdb.NewDefaultReadOptions(),
		wo:        gorocksdb.NewDefaultWriteOptions(),
		bucketSec: bucketSec,
	}
	if err := d.loadLastCleanedBucket(); err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

func (d *RocksLongDeduper) Close() {
	if d.ro != nil {
		d.ro.Destroy()
	}
	if d.wo != nil {
		d.wo.Destroy()
	}
	if d.db != nil {
		d.db.Close()
	}
}

// SeenOrAdd: exact dedup within [nowTs-ttl, nowTs] style fence using expireTs=nowTs+ttlSec.
// key32 must be 32 bytes (tx hash).
func (d *RocksLongDeduper) SeenOrAdd(key32 []byte, nowTs int64, ttlSec int64) (bool, error) {
	if len(key32) != 32 {
		return false, fmt.Errorf("key32 length must be 32, got=%d", len(key32))
	}
	if ttlSec <= 0 {
		ttlSec = 1
	}

	expireTs := nowTs + ttlSec
	mainKey := makeMainKey(key32)

	// check existing
	val, err := d.db.Get(d.ro, mainKey)
	if err != nil {
		return false, err
	}
	if val.Exists() {
		exp := decodeI64(val.Data())
		val.Free()
		if exp >= nowTs {
			return true, nil
		}
	} else {
		val.Free()
	}

	// add/overwrite
	bucket := expireTs / d.bucketSec
	idxKey := makeIdxKey(bucket, key32)

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	wb.Put(mainKey, encodeI64(expireTs))
	wb.Put(idxKey, encodeI64(expireTs))

	if err := d.db.Write(d.wo, wb); err != nil {
		return false, err
	}
	return false, nil
}

// Evict cleans buckets strictly older than nowBucket.
// It progresses from lastCleanedBucket+1 up to nowBucket-1.
func (d *RocksLongDeduper) Evict(nowTs int64) error {
	nowBucket := nowTs / d.bucketSec
	target := nowBucket - 1
	if target <= d.lastCleanedBucket {
		return nil
	}
	// clean bucket by bucket to bound work
	for b := d.lastCleanedBucket + 1; b <= target; b++ {
		if err := d.cleanBucket(b); err != nil {
			return err
		}
		d.lastCleanedBucket = b
		if err := d.saveLastCleanedBucket(); err != nil {
			return err
		}
	}
	return nil
}

func (d *RocksLongDeduper) cleanBucket(bucket int64) error {
	prefix := makeIdxPrefix(bucket)
	it := d.db.NewIterator(d.ro)
	defer it.Close()

	// seek prefix
	it.Seek(prefix)

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	for it.Valid() {
		k := it.Key()
		if !hasPrefix(k.Data(), prefix) {
			k.Free()
			break
		}
		v := it.Value()

		// parse tx hash from idx key: "ldx:" + bucket(8) + ":" + hash(32)
		txHash, ok := parseIdxKeyTxHash(k.Data())
		expIdx := decodeI64(v.Data())

		// Always delete the idx entry itself
		wb.Delete(k.Data())

		if ok {
			mainKey := makeMainKey(txHash)
			mv, err := d.db.Get(d.ro, mainKey)
			if err != nil {
				k.Free()
				v.Free()
				it.Err() // touch
				return err
			}
			if mv.Exists() {
				expMain := decodeI64(mv.Data())
				// delete main only if it still matches this expire (avoid deleting overwritten newer entry)
				if expMain == expIdx {
					wb.Delete(mainKey)
				}
			}
			mv.Free()
		}
		k.Free()
		v.Free()
		// batch flush occasionally to avoid huge batches
		if wb.Count() >= 5000 {
			if err := d.db.Write(d.wo, wb); err != nil {
				return err
			}
			wb.Clear()
		}
		it.Next()
	}

	if err := it.Err(); err != nil {
		return err
	}
	if wb.Count() > 0 {
		if err := d.db.Write(d.wo, wb); err != nil {
			return err
		}
	}
	return nil
}

// ---- meta: last cleaned bucket ----
func (d *RocksLongDeduper) loadLastCleanedBucket() error {
	val, err := d.db.Get(d.ro, []byte("meta:ld_last_clean_bucket"))
	if err != nil {
		return err
	}
	defer val.Free()
	if !val.Exists() {
		d.lastCleanedBucket = -1
		return nil
	}
	d.lastCleanedBucket = decodeI64(val.Data())
	return nil
}

func (d *RocksLongDeduper) saveLastCleanedBucket() error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	wb.Put([]byte("meta:ld_last_clean_bucket"), encodeI64(d.lastCleanedBucket))
	return d.db.Write(d.wo, wb)
}

// ---- key helpers ----
func makeMainKey(hash32 []byte) []byte {
	// "ld:" + 32 bytes
	k := make([]byte, 0, 3+32)
	k = append(k, 'l', 'd', ':')
	k = append(k, hash32...)
	return k
}

func makeIdxPrefix(bucket int64) []byte {
	// "ldx:" + bucket(8) + ":"
	k := make([]byte, 0, 4+8+1)
	k = append(k, 'l', 'd', 'x', ':')
	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], uint64(bucket))
	k = append(k, b8[:]...)
	k = append(k, ':')
	return k
}

func makeIdxKey(bucket int64, hash32 []byte) []byte {
	p := makeIdxPrefix(bucket)
	k := make([]byte, 0, len(p)+32)
	k = append(k, p...)
	k = append(k, hash32...)
	return k
}

func parseIdxKeyTxHash(k []byte) ([]byte, bool) {
	// expect len = 4 + 8 + 1 + 32 = 45, but allow longer keys if you extend later
	if len(k) < 4+8+1+32 {
		return nil, false
	}
	// last 32 bytes
	h := make([]byte, 32)
	copy(h, k[len(k)-32:])
	return h, true
}

func hasPrefix(b, p []byte) bool {
	if len(b) < len(p) {
		return false
	}
	for i := range p {
		if b[i] != p[i] {
			return false
		}
	}
	return true
}

func encodeI64(x int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(x))
	return b[:]
}

func decodeI64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b[:8]))
}

// Optional helper to build a default path like ./data/processor_state/longdedup.db
func DefaultLongDedupPath(baseDir string) string {
	return filepath.Join(baseDir, "longdedup.db")
}
