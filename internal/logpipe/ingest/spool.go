package ingest

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
	"time"
)

type Spool interface {
	Append(partition int32, offset int64, raw []byte) error
	Close() error
}

type FileSpool struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer

	// batching knobs
	syncEveryN   int           // e.g. 1000
	syncEveryDur time.Duration // e.g. 10ms

	// state
	pendingN   int
	lastSynced time.Time
	closed     bool
}

type FileSpoolOption func(*FileSpool)

func WithSpoolSyncEveryN(n int) FileSpoolOption {
	return func(s *FileSpool) {
		if n > 0 {
			s.syncEveryN = n
		}
	}
}

func WithSpoolSyncEveryDur(d time.Duration) FileSpoolOption {
	return func(s *FileSpool) {
		if d > 0 {
			s.syncEveryDur = d
		}
	}
}

func NewFileSpool(path string, opts ...FileSpoolOption) (*FileSpool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s := &FileSpool{
		f:            f,
		w:            bufio.NewWriterSize(f, 1<<20), // 1MB buffer
		syncEveryN:   1000,                          // default
		syncEveryDur: 10 * time.Millisecond,         // default
		lastSynced:   time.Now(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func (s *FileSpool) Append(partition int32, offset int64, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}

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

	s.pendingN++

	// decide whether to flush+sync now
	now := time.Now()
	needSync := false
	if s.pendingN >= s.syncEveryN {
		needSync = true
	} else if s.syncEveryDur > 0 && now.Sub(s.lastSynced) >= s.syncEveryDur {
		needSync = true
	}

	if needSync {
		if err := s.flushAndSyncLocked(); err != nil {
			return err
		}
	}

	return nil
}

func (s *FileSpool) flushAndSyncLocked() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.pendingN = 0
	s.lastSynced = time.Now()
	return nil
}

func (s *FileSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	// final durability
	_ = s.w.Flush()
	_ = s.f.Sync()
	return s.f.Close()
}
