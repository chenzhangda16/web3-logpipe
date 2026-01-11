package store

import (
	"errors"
	"strconv"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/hash"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/tecbot/gorocksdb"
)

type RocksStore struct {
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
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

	// 3) meta:head_hash -> hash
	wb.Put(KeyHeadHash(), b.Hash.Bytes())

	// 4) meta:head_num -> number
	wb.Put(KeyHeadNum(), []byte(strconv.FormatInt(b.Header.Number, 10)))

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
	if backfillSec <= 0 {
		// no window requirement, just keep all
		return TailKeepAllCatchUp, 0, nil
	}
	if gapSec <= 0 {
		gapSec = 1
	}

	headNum, okHead, err := s.HeadNum()
	if err != nil {
		return TailRebuild, 0, err
	}
	if !okHead || headNum <= 0 {
		return TailRebuild, 0, nil
	}

	target := curTs - backfillSec
	windowEnd := target + gapSec

	// Read head timestamp.
	headTs, okTs, err := s.GetCanonicalTimestamp(headNum)
	if err != nil {
		return TailRebuild, 0, err
	}
	if !okTs {
		return TailRebuild, 0, nil
	}

	// Case 1: chain hasn't reached target time yet -> keep all, just mine forward to catch up.
	if headTs < target {
		return TailKeepAllCatchUp, headNum, nil
	}

	// Find lower_bound: smallest n such that ts(n) >= target
	pos, tsPos, okPos, err := s.LowerBoundByTimestamp(target)
	if err != nil {
		return TailRebuild, 0, err
	}
	if !okPos {
		return TailKeepAllCatchUp, headNum, nil
	}

	if tsPos > windowEnd {
		if pos-1 >= 1 {
			return TailTrimAfterKeep, pos - 1, nil
		}
		return TailRebuild, 0, nil
	}

	end := pos
	prevTs := tsPos

	for n := pos + 1; n <= headNum; n++ {
		ts, ok, err := s.GetCanonicalTimestamp(n)
		if err != nil {
			return TailRebuild, 0, err
		}
		if !ok {
			break
		}
		if ts-prevTs <= gapSec {
			end = n
			prevTs = ts
			continue
		}
		break
	}

	// If end < headNum, it means blocks after end are non-contiguous "islands" for our definition -> trim them.
	if end < headNum {
		return TailTrimAfterKeep, end, nil
	}

	// Already contiguous all the way to head -> keep all and catch up (though catch up might be noop).
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
	raw, err := s.GetCanonicalBlockRaw(n)
	if err != nil {
		// canonical not found
		return 0, false, nil
	}
	blk, err := model.DecodeBlock(raw)
	if err != nil {
		return 0, false, err
	}
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
