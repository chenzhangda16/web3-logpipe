package out

import (
	"encoding/json"
)

type Envelope struct {
	Type string          `json:"type"` // e.g. "win_tick"
	TS   int64           `json:"ts"`   // unix milli
	Data json.RawMessage `json:"data"`
}

type WinTick struct {
	WinIdx  int   `json:"win_idx"`
	Head    int64 `json:"head"`
	Tail    int64 `json:"tail"`
	OpenWin bool  `json:"open_win"`
}
