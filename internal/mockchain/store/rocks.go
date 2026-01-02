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
