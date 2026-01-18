package ingest

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

type Spool interface {
	Append(partition int32, offset int64, raw []byte) error
	Close() error
}

type FileSpool struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func NewFileSpool(path string) (*FileSpool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSpool{f: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
}

func (s *FileSpool) Append(partition int32, offset int64, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// record = [p:int32][off:int64][n:uint32][raw:n]
	var hdr [4 + 8 + 4]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(partition))
	binary.BigEndian.PutUint64(hdr[4:12], uint64(offset))
	binary.BigEndian.PutUint32(hdr[12:16], uint32(len(raw)))

	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(raw); err != nil {
		return err
	}

	// mock/先求正确：每条都 flush+fsync（慢，但语义最稳）
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *FileSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.w.Flush()
	return s.f.Close()
}
