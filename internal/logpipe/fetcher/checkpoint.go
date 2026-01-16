package fetcher

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Ckpt struct {
	LastHeight int64
	LastHash   string // hex string, optional
}

type Checkpoint interface {
	Load() (ckpt Ckpt, ok bool, err error)
	Save(ckpt Ckpt) error
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

func (c *FileCheckpoint) Load() (Ckpt, bool, error) {
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

func (c *FileCheckpoint) Save(ckpt Ckpt) error {
	tmp := c.path + ".tmp"

	// new format: height + "\n" + hash + "\n"
	// if hash empty, still write a blank second line to keep format stable
	content := strconv.FormatInt(ckpt.LastHeight, 10) + "\n" + ckpt.LastHash + "\n"

	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
