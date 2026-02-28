package fetcher

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Ckpt struct {
	LastHeight int64
	LastHash   string // hex string, optional
}

type Checkpoint interface {
	load() (ckpt Ckpt, ok bool, err error)
	save(ckpt Ckpt) error
}

type FileCheckpoint struct {
	path string
}

func NewFileCheckpoint(path string) (*FileCheckpoint, error) {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return &FileCheckpoint{path: path}, nil
}

func (c *FileCheckpoint) load() (Ckpt, bool, error) {
	b, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Ckpt{}, false, nil
		}
		return Ckpt{}, false, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return Ckpt{}, false, nil
	}

	lines := strings.Split(s, "\n")
	if len(lines) == 0 {
		return Ckpt{}, false, nil
	}

	h, err := strconv.ParseInt(strings.TrimSpace(lines[0]), 10, 64)
	if err != nil {
		return Ckpt{}, false, err
	}

	var hashStr string
	if len(lines) >= 2 {
		hashStr = strings.TrimSpace(lines[1])
	} else {
		return Ckpt{}, false, nil
	}

	return Ckpt{LastHeight: h, LastHash: hashStr}, true, nil
}

func (c *FileCheckpoint) save(ckpt Ckpt) error {
	tmp := c.path + ".tmp"

	// new format: height + "\n" + hash + "\n"
	// if hash empty, still write a blank second line to keep format stable
	content := strconv.FormatInt(ckpt.LastHeight, 10) + "\n" + ckpt.LastHash + "\n"

	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}

func pushLatestCkpt(ch chan Ckpt, v Ckpt) {
	select {
	case ch <- v:
		return
	default:
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- v:
		default:
		}
	}
}

func ckptLoopPeriodic(ctx context.Context, ch <-chan Ckpt, ck Checkpoint, every time.Duration, bench *FetchBench) error {
	var pending *Ckpt
	tk := time.NewTicker(every)
	defer tk.Stop()

	flush := func() error {
		if pending == nil {
			return nil
		}
		if err := ck.save(*pending); err != nil {
			return err
		}
		pending = nil
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return flush()
		case v := <-ch:
			pending = &v
		case <-tk.C:
			if err := flush(); err != nil {
				return err
			}
			if bench != nil {
				bench.AddCkptSave()
			}
		}
	}
}
