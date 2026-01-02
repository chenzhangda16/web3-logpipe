package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/hash"
)

func BuildBlock(
	number int64,
	parentHash hash.Hash32,
	txs []Tx,
	timestamp int64,
	nonce uint64,
) Block {
	txHashes := make([]hash.Hash32, 0, len(txs))
	for _, tx := range txs {
		txHashes = append(txHashes, tx.Hash)
	}

	txRoot := TxRoot(txHashes)

	header := BlockHeader{
		Number:     number,
		ParentHash: parentHash,
		Timestamp:  timestamp,
		TxRoot:     txRoot,
		Nonce:      nonce,
	}

	return Block{
		Header: header,
		Hash:   HashHeader(header),
		Txs:    txs,
	}
}

func BuildTx(body TxBody, blockNum int64) Tx {
	h := HashTxCanonical(body)
	return Tx{Hash: h, TxBody: body, BlockNum: blockNum}
}

func HashHeader(header BlockHeader) hash.Hash32 {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, header.Number)
	_ = binary.Write(&buf, binary.BigEndian, header.Timestamp)
	buf.Write(header.ParentHash[:])
	buf.Write(header.TxRoot[:])
	_ = binary.Write(&buf, binary.BigEndian, header.Nonce)
	return sha256.Sum256(buf.Bytes())
}

func TxRoot(txHashes []hash.Hash32) hash.Hash32 {
	sorted := make([]hash.Hash32, len(txHashes))
	copy(sorted, txHashes)

	slices.SortFunc(sorted, func(a, b hash.Hash32) int {
		return bytes.Compare(a[:], b[:])
	})
	var buf bytes.Buffer
	for _, h := range txHashes {
		buf.Write(h[:])
	}
	return sha256.Sum256(buf.Bytes())
}

func HashTxCanonical(body TxBody) hash.Hash32 {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint64(len(body.From)))
	buf.WriteString(body.From)
	_ = binary.Write(&buf, binary.BigEndian, uint64(len(body.To)))
	buf.WriteString(body.To)
	_ = binary.Write(&buf, binary.BigEndian, uint64(len(body.Token)))
	buf.WriteString(body.Token)
	_ = binary.Write(&buf, binary.BigEndian, body.Amount)
	_ = binary.Write(&buf, binary.BigEndian, body.Timestamp)
	_ = binary.Write(&buf, binary.BigEndian, body.Nonce)
	return sha256.Sum256(buf.Bytes())
}
