package store

import (
	"bytes"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/pkg/hash"
	"github.com/tecbot/gorocksdb"
)

type RocksStore struct {
	db         *gorocksdb.DB
	ro         *gorocksdb.ReadOptions
	wo         *gorocksdb.WriteOptions
	gapRuleSec int64

	lastCanonHeight int64
	lastCanonTs     int64
	lastCanonValid  bool
}

type TailAction int

const (
	// TailRebuild means: delete all canonical blocks (or treat as empty) and rebuild from target time.
	TailRebuild TailAction = iota

	// TailKeepAllCatchUp means: keep all existing blocks, just mine forward to catch up to now.
	TailKeepAllCatchUp

	// TailTrimAfterKeep means: keep blocks up to keepHeight, delete blocks after that, then mine forward to catch up.
	TailTrimAfterKeep
)

func (s *RocksStore) GapRuleSec() int64 {
	return s.gapRuleSec
}

func (a TailAction) String() string {
	switch a {
	case TailRebuild:
		return "REBUILD"
	case TailKeepAllCatchUp:
		return "KEEP_ALL_CATCH_UP"
	case TailTrimAfterKeep:
		return "TRIM_AFTER_KEEP"
	default:
		return "UNKNOWN"
	}
}

func Open(path string, gapRuleSec int64) (*RocksStore, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}

	return &RocksStore{
		db:         db,
		ro:         gorocksdb.NewDefaultReadOptions(),
		wo:         gorocksdb.NewDefaultWriteOptions(),
		gapRuleSec: gapRuleSec,
	}, nil
}

func (s *RocksStore) Close() {
	if s.ro != nil {
		s.ro.Destroy()
	}
	if s.wo != nil {
		s.wo.Destroy()
	}
	if s.db != nil {
		s.db.Close()
	}
}

// HeadHash returns head hash. ok=false means empty DB.
func (s *RocksStore) HeadHash() (hash.Hash32, bool, error) {
	val, err := s.db.Get(s.ro, KeyHeadHash())
	if err != nil {
		return hash.Hash32{}, false, err
	}
	defer val.Free()

	if !val.Exists() {
		return hash.Hash32{}, false, nil
	}
	h, err := hash.ByteSlice2Hash32(val.Data())
	if err != nil {
		return hash.Hash32{}, false, err
	}
	return h, true, nil
}

// HeadNum returns head num. ok=false means empty DB.
func (s *RocksStore) HeadNum() (int64, bool, error) {
	val, err := s.db.Get(s.ro, KeyHeadNum())
	if err != nil {
		return 0, false, err
	}
	defer val.Free()

	if !val.Exists() {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(string(val.Data()), 10, 64)
	if err != nil {
		return 0, false, err
	}
	return n, true, nil
}

// GetBlockByHashRaw gets the block bytes by block hash.
func (s *RocksStore) GetBlockByHashRaw(h hash.Hash32) ([]byte, error) {
	val, err := s.db.Get(s.ro, KeyBlockHash(h))
	if err != nil {
		return nil, err
	}
	defer val.Free()

	if !val.Exists() {
		return nil, errors.New("block not found")
	}
	return append([]byte(nil), val.Data()...), nil
}

// GetCanonicalBlockRaw gets canonical block at height n.
func (s *RocksStore) GetCanonicalBlockRaw(n int64) ([]byte, error) {
	val, err := s.db.Get(s.ro, KeyCanon(n))
	if err != nil {
		return nil, err
	}
	defer val.Free()
	if !val.Exists() {
		return nil, errors.New("canonical block not found")
	}

	h, err := hash.ByteSlice2Hash32(val.Data())
	if err != nil {
		return nil, err
	}
	return s.GetBlockByHashRaw(h)
}

// AppendCanonicalBlock writes block by hash and updates canonical + head.
func (s *RocksStore) AppendCanonicalBlock(b model.Block, raw []byte) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	// 1) blockhash:{hash} -> raw
	wb.Put(KeyBlockHash(b.Hash), raw)

	// 2) canon:{number} -> hash
	wb.Put(KeyCanon(b.Header.Number), b.Hash.Bytes())

	// 2.1) canon_ts:{number} -> ts (8 bytes BE)
	wb.Put(KeyCanonTS(b.Header.Number), encodeI64BE(b.Header.Timestamp))

	// 3) meta:head_hash -> hash
	wb.Put(KeyHeadHash(), b.Hash.Bytes())

	// 4) meta:head_num -> number
	wb.Put(KeyHeadNum(), []byte(strconv.FormatInt(b.Header.Number, 10)))

	// 5) gap event index (shape-2): gap_end_ts:{gapSec}:{endTs} -> height
	// only if gap rule is set for this run
	gapSec := s.gapRuleSec
	if gapSec > 0 && b.Header.Number > 1 {
		var prevTs int64
		var ok bool

		// fast path: in-memory tail cache
		if s.lastCanonValid && s.lastCanonHeight == b.Header.Number-1 {
			prevTs = s.lastCanonTs
			ok = true
		} else {
			// fallback: read prev timestamp once (still fast due to canon_ts)
			var err error
			prevTs, ok, err = s.GetCanonicalTimestamp(b.Header.Number - 1)
			if err != nil {
				// 读失败不致命：先不写 gap 事件[mark:unreliable]
				ok = false
			}
		}

		if ok && b.Header.Timestamp-prevTs > gapSec {
			wb.Put(KeyGapEndTS(gapSec, b.Header.Timestamp), encodeI64BE(b.Header.Number))
		}
	}

	if err := s.db.Write(s.wo, wb); err != nil {
		return err
	}

	// update in-memory tail cache after successful write
	s.lastCanonHeight = b.Header.Number
	s.lastCanonTs = b.Header.Timestamp
	s.lastCanonValid = true

	return nil
}

// DecideTailAction decides how to handle tail continuity for backfill window.
// Definitions:
// - curTs: current wall-clock timestamp you want the chain to catch up to
// - backfillSec: T (e.g., 86400)
// - gapSec: t, the max allowed gap between adjacent blocks to be considered "contiguous"
// Semantics:
// We care about the ability to build a contiguous suffix that covers [curTs-backfillSec, curTs].
// It returns:
// - action:
//   - KEEP_ALL_CATCH_UP: chain head is still before target; no deletion needed, just mine forward
//   - TRIM_AFTER_KEEP: keep up to keepHeight, delete after, then mine forward
//   - REBUILD: delete all and rebuild from target
//
// - keepHeight: only meaningful when action == TRIM_AFTER_KEEP (or keep-all uses headNum)
func (s *RocksStore) DecideTailAction(curTs int64, backfillSec int64) (action TailAction, keepHeight int64, err error) {
	start := time.Now()
	gapSec := s.gapRuleSec
	// --- sanitize ---
	if backfillSec <= 0 {
		log.Printf("[tail] decide: backfillSec<=0 => KEEP_ALL_CATCH_UP (curTs=%d backfillSec=%d gapSec=%d) cost=%s",
			curTs, backfillSec, gapSec, time.Since(start))
		return TailKeepAllCatchUp, 0, nil
	}

	target := curTs - backfillSec
	windowEnd := target + gapSec
	log.Printf("[tail] decide: curTs=%d backfillSec=%d gapSec=%d target=%d windowEnd=%d", curTs, backfillSec, gapSec, target, windowEnd)

	// --- head ---
	headNum, okHead, err := s.HeadNum()
	if err != nil {
		log.Printf("[tail] headnum failed: err=%v cost=%s", err, time.Since(start))
		return TailRebuild, 0, err
	}
	if !okHead || headNum <= 0 {
		log.Printf("[tail] no head => REBUILD (okHead=%v headNum=%d) cost=%s", okHead, headNum, time.Since(start))
		return TailRebuild, 0, nil
	}

	headTs, okTs, err := s.GetCanonicalTimestamp(headNum)
	if err != nil {
		log.Printf("[tail] head timestamp failed: headNum=%d err=%v cost=%s", headNum, err, time.Since(start))
		return TailRebuild, 0, err
	}
	if !okTs {
		log.Printf("[tail] no head timestamp => REBUILD (headNum=%d) cost=%s", headNum, time.Since(start))
		return TailRebuild, 0, nil
	}
	log.Printf("[tail] head: headNum=%d headTs=%d target=%d", headNum, headTs, target)

	// If head hasn't reached target time -> keep all and just mine forward.
	if headTs < target {
		log.Printf("[tail] headTs<target => KEEP_ALL_CATCH_UP (headTs=%d target=%d headNum=%d) cost=%s",
			headTs, target, headNum, time.Since(start))
		return TailKeepAllCatchUp, headNum, nil
	}

	// --- lower_bound(target) to get starting position near target ---
	pos, tsPos, okPos, err := s.LowerBoundByTimestamp(target)
	if err != nil {
		log.Printf("[tail] lower_bound failed: target=%d err=%v cost=%s", target, err, time.Since(start))
		return TailRebuild, 0, err
	}
	if !okPos {
		log.Printf("[tail] lower_bound not found => KEEP_ALL_CATCH_UP (target=%d headNum=%d) cost=%s",
			target, headNum, time.Since(start))
		return TailKeepAllCatchUp, headNum, nil
	}
	log.Printf("[tail] lower_bound: pos=%d tsPos=%d (target=%d windowEnd=%d)", pos, tsPos, target, windowEnd)

	// Quick hole detection around target:
	// If first ts>=target is already beyond windowEnd, there is a hole near target -> trim to pos-1 (or rebuild)
	if tsPos > windowEnd {
		if pos-1 >= 1 {
			log.Printf("[tail] tsPos>windowEnd => TRIM_AFTER_KEEP keep=%d (pos=%d tsPos=%d windowEnd=%d) cost=%s",
				pos-1, pos, tsPos, windowEnd, time.Since(start))
			return TailTrimAfterKeep, pos - 1, nil
		}
		log.Printf("[tail] tsPos>windowEnd but pos-1<1 => REBUILD (pos=%d tsPos=%d windowEnd=%d) cost=%s",
			pos, tsPos, windowEnd, time.Since(start))
		return TailRebuild, 0, nil
	}

	// --- gap index fast path (shape-2) ---
	storedGap, hasStored, err := s.getStoredGapRuleSec()
	if err != nil {
		log.Printf("[tail] gap_rule meta read failed: err=%v cost=%s", err, time.Since(start))
		return TailRebuild, 0, err
	}

	log.Printf(
		"[tail][rule] runtimeGap=%d storedGap=%v storedGapSec=%d",
		s.GapRuleSec(),
		hasStored,
		func() int64 {
			if hasStored {
				return storedGap
			}
			return 0
		}(),
	)

	if hasStored && storedGap == gapSec {
		log.Printf("[tail] gap_index fastpath: storedGap=%d matches gapSec=%d (target=%d)", storedGap, gapSec, target)

		it := s.db.NewIterator(s.ro)
		defer it.Close()

		prefix := GapPrefix(gapSec)
		seekKey := KeyGapEndTS(gapSec, target)

		seen := 0
		for it.Seek(seekKey); it.Valid(); it.Next() {
			seen++

			k := it.Key()
			v := it.Value()
			kBytes := append([]byte(nil), k.Data()...)
			vBytes := append([]byte(nil), v.Data()...)
			k.Free()
			v.Free()

			if !bytes.HasPrefix(kBytes, prefix) {
				break
			}

			h, ok := decodeI64BE(vBytes)
			if !ok {
				log.Printf("[tail] gap_index bad value: seen=%d (skip) cost=%s", seen, time.Since(start))
				continue
			}

			valid, err := s.validateGapAtHeight(h, headNum, gapSec)
			if err != nil {
				log.Printf("[tail] gap_index validate failed: h=%d err=%v cost=%s", h, err, time.Since(start))
				return TailRebuild, 0, err
			}
			if !valid {
				log.Printf("[tail] gap_index stale/invalid: h=%d (skip) seen=%d", h, seen)
				continue
			}

			keep := h - 1
			if keep < 1 {
				log.Printf("[tail] gap_index hit => REBUILD (h=%d keep=%d) cost=%s", h, keep, time.Since(start))
				return TailRebuild, 0, nil
			}

			log.Printf("[tail] gap_index hit => TRIM_AFTER_KEEP keep=%d (gapHeight=%d) seen=%d cost=%s",
				keep, h, seen, time.Since(start))
			return TailTrimAfterKeep, keep, nil
		}

		log.Printf("[tail] gap_index miss => KEEP_ALL_CATCH_UP (seen=%d) cost=%s", seen, time.Since(start))
		return TailKeepAllCatchUp, headNum, nil
	}

	// --- fallback: sequential scan after pos (fast because canon_ts is 8 bytes) ---
	if hasStored {
		log.Printf("[tail] gap_index fallback: storedGap=%d != gapSec=%d (target=%d) => scan", storedGap, gapSec, target)
	} else {
		log.Printf("[tail] gap_index fallback: no stored meta (gapSec=%d target=%d) => scan", gapSec, target)
	}

	gapH, gapEndTs, okGap, err := s.findFirstGapAfterPos(pos, headNum, gapSec, target)
	if err != nil {
		log.Printf("[tail] fallback scan failed: pos=%d headNum=%d err=%v cost=%s", pos, headNum, err, time.Since(start))
		return TailRebuild, 0, err
	}

	// we completed fallback for this rule -> store meta for future fast path
	if err := s.setStoredGapRuleSec(gapSec); err != nil {
		// 不把 meta 写入失败当作致命错误
		log.Printf("[tail] set gap_rule meta failed: gapSec=%d err=%v (ignored)", gapSec, err)
	} else {
		log.Printf("[tail] set gap_rule meta: gapSec=%d", gapSec)
	}

	if okGap {
		keep := gapH - 1
		if keep < 1 {
			log.Printf("[tail] fallback found gap => REBUILD (gapHeight=%d gapEndTs=%d keep=%d) cost=%s",
				gapH, gapEndTs, keep, time.Since(start))
			return TailRebuild, 0, nil
		}
		log.Printf("[tail] fallback found gap => TRIM_AFTER_KEEP keep=%d (gapHeight=%d gapEndTs=%d) cost=%s",
			keep, gapH, gapEndTs, time.Since(start))
		return TailTrimAfterKeep, keep, nil
	}

	log.Printf("[tail] fallback scan no gap => KEEP_ALL_CATCH_UP keep=%d cost=%s", headNum, time.Since(start))
	return TailKeepAllCatchUp, headNum, nil
}

// DeleteCanonicalAfter deletes canonical blocks with height > keepHeight, deletes their raw blocks,
// and updates meta head to keepHeight.
// If keepHeight == 0, it deletes the entire canonical chain and clears head metadata.
func (s *RocksStore) DeleteCanonicalAfter(keepHeight int64) error {
	headNum, okHead, err := s.HeadNum()
	if err != nil {
		return err
	}
	if !okHead || headNum <= 0 {
		return nil
	}
	if keepHeight >= headNum {
		return nil
	}
	if keepHeight < 0 {
		keepHeight = 0
	}

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	// delete (keepHeight+1 .. headNum)
	for n := keepHeight + 1; n <= headNum; n++ {
		h, ok, err := s.GetCanonicalHash(n)
		if err != nil {
			return err
		}
		if ok {
			wb.Delete(KeyBlockHash(h))
		}
		wb.Delete(KeyCanon(n))
		// NEW: delete timestamp index for canonical height
		wb.Delete(KeyCanonTS(n))
	}

	// update head metadata
	if keepHeight == 0 {
		wb.Delete(KeyHeadHash())
		wb.Delete(KeyHeadNum())
	} else {
		newHeadHash, ok, err := s.GetCanonicalHash(keepHeight)
		if err != nil {
			return err
		}
		if !ok {
			// broken canonical mapping; safest is to clear head
			wb.Delete(KeyHeadHash())
			wb.Delete(KeyHeadNum())
		} else {
			wb.Put(KeyHeadHash(), newHeadHash.Bytes())
			wb.Put(KeyHeadNum(), []byte(strconv.FormatInt(keepHeight, 10)))
		}
	}

	// OPTIONAL NEW: if you later add meta:gap_head, keep it consistent.
	// Conservative approach: if trim happens, clear gap_head to avoid stale pointers.
	// (If you want "recompute gap_head up to keepHeight", that would require scanning; don't do it here.)
	wb.Delete(KeyGapHead())

	if err := s.db.Write(s.wo, wb); err != nil {
		return err
	}

	// canonical tail has changed; invalidate cache
	s.lastCanonValid = false
	s.lastCanonHeight = 0
	s.lastCanonTs = 0

	return nil
}

// GetCanonicalHash gets canonical block hash at height n.
func (s *RocksStore) GetCanonicalHash(n int64) (hash.Hash32, bool, error) {
	val, err := s.db.Get(s.ro, KeyCanon(n))
	if err != nil {
		return hash.Hash32{}, false, err
	}
	defer val.Free()

	if !val.Exists() {
		return hash.Hash32{}, false, nil
	}
	h, err := hash.ByteSlice2Hash32(val.Data())
	if err != nil {
		return hash.Hash32{}, false, err
	}
	return h, true, nil
}

// GetCanonicalTimestamp gets canonical block timestamp at height n (decode raw).
func (s *RocksStore) GetCanonicalTimestamp(n int64) (int64, bool, error) {
	// FAST PATH: canon_ts:{n} -> 8 bytes BE int64
	v, err := s.db.Get(s.ro, KeyCanonTS(n))
	if err != nil {
		return 0, false, err
	}
	defer v.Free()
	if data := v.Data(); len(data) > 0 {
		if ts, ok := decodeI64BE(data); ok {
			return ts, true, nil
		}
		// 如果存在但长度不对，当作损坏：走 fallback
	}

	// FALLBACK (legacy DB): decode raw block
	raw, err := s.GetCanonicalBlockRaw(n)
	if err != nil {
		// canonical not found
		return 0, false, nil
	}
	blk, err := model.DecodeBlock(raw)
	if err != nil {
		return 0, false, err
	}

	// Best-effort self-heal: write back canon_ts for next time.
	// 不要把错误上抛影响主流程；写失败就算了。
	_ = s.db.Put(s.wo, KeyCanonTS(n), encodeI64BE(blk.Header.Timestamp))

	return blk.Header.Timestamp, true, nil
}

// LowerBoundByTimestamp returns the smallest canonical height n such that ts(n) >= targetTs,
// along with ts(n).
// ok=false means: empty chain, broken canonical, or all ts < targetTs.
func (s *RocksStore) LowerBoundByTimestamp(targetTs int64) (n int64, nTs int64, ok bool, err error) {
	headNum, okHead, err := s.HeadNum()
	if err != nil {
		return 0, 0, false, err
	}
	if !okHead || headNum <= 0 {
		return 0, 0, false, nil
	}

	lo, hi := int64(1), headNum
	pos := int64(-1)
	posTs := int64(0)

	for lo <= hi {
		mid := lo + (hi-lo)/2
		midTs, okTs, err := s.GetCanonicalTimestamp(mid)
		if err != nil {
			return 0, 0, false, err
		}
		if !okTs {
			// canonical broken
			return 0, 0, false, nil
		}

		if midTs >= targetTs {
			pos = mid
			posTs = midTs
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	if pos == -1 {
		return 0, 0, false, nil
	}
	return pos, posTs, true, nil
}

func KeyCanonTS(height int64) []byte {
	return []byte("canon_ts:" + strconv.FormatInt(height, 10))
}

func KeyGapHead() []byte {
	return []byte("meta:gap_head")
}

func encodeI64BE(v int64) []byte {
	var b [8]byte
	u := uint64(v)
	b[0] = byte(u >> 56)
	b[1] = byte(u >> 48)
	b[2] = byte(u >> 40)
	b[3] = byte(u >> 32)
	b[4] = byte(u >> 24)
	b[5] = byte(u >> 16)
	b[6] = byte(u >> 8)
	b[7] = byte(u)
	return b[:]
}

func decodeI64BE(b []byte) (int64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	u := (uint64(b[0]) << 56) |
		(uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) |
		(uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) |
		(uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) |
		uint64(b[7])
	return int64(u), true
}

func KeyGapRuleSec() []byte {
	return []byte("meta:gap_rule_sec")
}

// gap_end_ts:{gapSecBE}:{endTsBE} -> heightBE
func KeyGapEndTS(gapSec int64, endTs int64) []byte {
	// prefix keeps lexicographic order: group by gapSec then by endTs
	p := []byte("gap_end_ts:")
	p = append(p, encodeI64BE(gapSec)...)
	p = append(p, ':')
	p = append(p, encodeI64BE(endTs)...)
	return p
}

func GapPrefix(gapSec int64) []byte {
	p := []byte("gap_end_ts:")
	p = append(p, encodeI64BE(gapSec)...)
	p = append(p, ':')
	return p
}

func (s *RocksStore) getStoredGapRuleSec() (int64, bool, error) {
	v, err := s.db.Get(s.ro, KeyGapRuleSec())
	if err != nil {
		return 0, false, err
	}
	defer v.Free()
	if len(v.Data()) == 0 {
		return 0, false, nil
	}
	sec, ok := decodeI64BE(v.Data())
	if !ok {
		return 0, false, nil
	}
	return sec, true, nil
}

func (s *RocksStore) setStoredGapRuleSec(sec int64) error {
	return s.db.Put(s.wo, KeyGapRuleSec(), encodeI64BE(sec))
}

func (s *RocksStore) validateGapAtHeight(h int64, headNum int64, gapSec int64) (bool, error) {
	if h <= 1 || h > headNum {
		return false, nil
	}
	// canonical must exist at h
	_, okHash, err := s.GetCanonicalHash(h)
	if err != nil {
		return false, err
	}
	if !okHash {
		return false, nil
	}

	ts1, ok1, err := s.GetCanonicalTimestamp(h - 1)
	if err != nil || !ok1 {
		return false, err
	}
	ts2, ok2, err := s.GetCanonicalTimestamp(h)
	if err != nil || !ok2 {
		return false, err
	}
	return (ts2 - ts1) > gapSec, nil
}

func (s *RocksStore) findFirstGapAfterPos(pos int64, headNum int64, gapSec int64, targetTs int64) (gapHeight int64, gapEndTs int64, ok bool, err error) {
	if pos < 1 {
		pos = 1
	}
	if pos >= headNum {
		return 0, 0, false, nil
	}

	prevTs, okTs, err := s.GetCanonicalTimestamp(pos)
	if err != nil {
		return 0, 0, false, err
	}
	if !okTs {
		return 0, 0, false, nil
	}

	for n := pos + 1; n <= headNum; n++ {
		ts, ok, err := s.GetCanonicalTimestamp(n)
		if err != nil {
			return 0, 0, false, err
		}
		if !ok {
			return 0, 0, false, nil
		}
		if ts-prevTs > gapSec {
			// lazy index rebuild: record this gap event for current rule
			_ = s.db.Put(s.wo, KeyGapEndTS(gapSec, ts), encodeI64BE(n))
			return n, ts, true, nil
		}
		prevTs = ts
	}
	return 0, 0, false, nil
}
