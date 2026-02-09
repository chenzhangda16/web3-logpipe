package store

import (
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

func Open(path string) (*RocksStore, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}

	return &RocksStore{
		db: db,
		ro: gorocksdb.NewDefaultReadOptions(),
		wo: gorocksdb.NewDefaultWriteOptions(),
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

	// 5) meta:gap_head -> earliest height where (ts[n]-ts[n-1]) > gapSec
	//    NOTE: 这里 gapSec 必须来自配置。你目前 store 层没有 gapSec 参数。
	//    最小侵入做法：先不在这里做 gap 判定，只做 canon_ts 索引（立刻消灭 15s 扫描）。
	//    若你坚持“更狠更准”在写入时维护 gap，则需要：
	//      - RocksStore 持有 cfg.GapSec（或方法入参带 gapSec）
	//      - 或者把 gapSec 固化为 tick*3 这种固定策略（不推荐）
	//
	// 我先给你“可选块”，你把 gapSec 接进来后再启用：

	/*
		if gapSec > 0 && b.Header.Number > 1 {
			// prev ts from canon_ts (fast path)
			prevRaw, err := s.db.Get(s.ro, KeyCanonTS(b.Header.Number-1))
			if err == nil {
				defer prevRaw.Free()
				if prevTs, ok := decodeI64BE(prevRaw.Data()); ok {
					if b.Header.Timestamp-prevTs > gapSec {
						// if gap_head is not set (or is 0), set it to current height
						gh, _ := s.getGapHead() // small helper reads KeyGapHead
						if gh == 0 {
							wb.Put(KeyGapHead(), []byte(strconv.FormatInt(b.Header.Number, 10)))
						}
					}
				}
			}
		}
	*/

	return s.db.Write(s.wo, wb)
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
func (s *RocksStore) DecideTailAction(curTs int64, backfillSec int64, gapSec int64) (action TailAction, keepHeight int64, err error) {
	start := time.Now()

	if backfillSec <= 0 {
		// no window requirement, just keep all
		log.Printf("[tail] decide: backfillSec<=0 => KEEP_ALL_CATCH_UP (curTs=%d backfillSec=%d gapSec=%d)", curTs, backfillSec, gapSec)
		return TailKeepAllCatchUp, 0, nil
	}
	if gapSec <= 0 {
		gapSec = 1
	}

	target := curTs - backfillSec
	windowEnd := target + gapSec
	log.Printf("[tail] decide: curTs=%d backfillSec=%d gapSec=%d target=%d windowEnd=%d", curTs, backfillSec, gapSec, target, windowEnd)

	headNum, okHead, err := s.HeadNum()
	if err != nil {
		log.Printf("[tail] headnum failed: err=%v cost=%s", err, time.Since(start))
		return TailRebuild, 0, err
	}
	if !okHead || headNum <= 0 {
		log.Printf("[tail] no head => REBUILD (okHead=%v headNum=%d) cost=%s", okHead, headNum, time.Since(start))
		return TailRebuild, 0, nil
	}

	// Read head timestamp.
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

	// Case 1: chain hasn't reached target time yet -> keep all, just mine forward to catch up.
	if headTs < target {
		log.Printf("[tail] headTs<target => KEEP_ALL_CATCH_UP (headTs=%d target=%d headNum=%d) cost=%s",
			headTs, target, headNum, time.Since(start))
		return TailKeepAllCatchUp, headNum, nil
	}

	// Find lower_bound: smallest n such that ts(n) >= target
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

	// If the first candidate already jumps beyond windowEnd, there is a "hole" around target -> trim or rebuild.
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

	end := pos
	prevTs := tsPos

	// scan forward to find contiguous suffix under gapSec definition
	for n := pos + 1; n <= headNum; n++ {
		ts, ok, err := s.GetCanonicalTimestamp(n)
		if err != nil {
			log.Printf("[tail] scan ts failed => REBUILD (n=%d err=%v) cost=%s", n, err, time.Since(start))
			return TailRebuild, 0, err
		}
		if !ok {
			log.Printf("[tail] scan missing ts: break (n=%d) end=%d headNum=%d cost=%s", n, end, headNum, time.Since(start))
			break
		}

		delta := ts - prevTs
		if delta <= gapSec {
			end = n
			prevTs = ts
			continue
		}

		log.Printf("[tail] non_contiguous: break (n=%d ts=%d prevTs=%d delta=%d gapSec=%d) end=%d headNum=%d cost=%s",
			n, ts, prevTs, delta, gapSec, end, headNum, time.Since(start))
		break
	}

	// If end < headNum, blocks after end are "islands" -> trim them.
	if end < headNum {
		log.Printf("[tail] result: TRIM_AFTER_KEEP keep=%d (end=%d headNum=%d) cost=%s", end, end, headNum, time.Since(start))
		return TailTrimAfterKeep, end, nil
	}

	// Already contiguous all the way to head -> keep all and catch up.
	log.Printf("[tail] result: KEEP_ALL_CATCH_UP keep=%d (contiguous_to_head) cost=%s", headNum, time.Since(start))
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

	return s.db.Write(s.wo, wb)
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
