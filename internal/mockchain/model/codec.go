package model

import "encoding/json"

func EncodeBlock(b Block) ([]byte, error) { return json.Marshal(b) }
func DecodeBlock(raw []byte) (Block, error) {
	var b Block
	err := json.Unmarshal(raw, &b)
	return b, err
}
