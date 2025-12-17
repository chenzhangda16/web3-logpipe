package store

import (
	"errors"
	"strconv"

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

func (s *RocksStore) Head() (int64, error) {
	val, err := s.db.Get(s.ro, KeyHead())
	if err != nil {
		return 0, err
	}
	defer val.Free()

	if !val.Exists() {
		return 0, nil // 空库 head=0
	}
	n, err := strconv.ParseInt(string(val.Data()), 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (s *RocksStore) GetBlockRaw(n int64) ([]byte, error) {
	val, err := s.db.Get(s.ro, KeyBlock(n))
	if err != nil {
		return nil, err
	}
	defer val.Free()

	if !val.Exists() {
		return nil, errors.New("block not found")
	}
	// 注意：val.Data() 背后是 RocksDB 管理的内存，Free 后会失效，所以要 copy
	b := append([]byte(nil), val.Data()...)
	return b, nil
}

func (s *RocksStore) AppendBlock(blockNum int64, blockBytes []byte) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	wb.Put(KeyBlock(blockNum), blockBytes)
	wb.Put(KeyHead(), []byte(strconv.FormatInt(blockNum, 10)))

	return s.db.Write(s.wo, wb)
}
