package processor

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type ProcCkpt struct {
	// per partition offset to resume (next offset)
	Offsets map[int32]int64 `json:"offsets"`

	LastBlockNum  int64  `json:"last_block_num"`
	LastBlockHash string `json:"last_block_hash"`
	LastTs        int64  `json:"last_ts"`
}

type Checkpoint interface {
	Load() (ProcCkpt, bool, error)
	Save(ProcCkpt) error
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

func (c *FileCheckpoint) Load() (ProcCkpt, bool, error) {
	b, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return ProcCkpt{}, false, nil
		}
		return ProcCkpt{}, false, err
	}
	var out ProcCkpt
	if err := json.Unmarshal(b, &out); err != nil {
		return ProcCkpt{}, false, err
	}
	if out.Offsets == nil {
		out.Offsets = map[int32]int64{}
	}
	return out, true, nil
}

func (c *FileCheckpoint) Save(v ProcCkpt) error {
	if v.Offsets == nil {
		v.Offsets = map[int32]int64{}
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
