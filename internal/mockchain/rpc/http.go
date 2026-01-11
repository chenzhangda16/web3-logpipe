package rpc

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/hash"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/store"
)

type Server struct {
	st *store.RocksStore
}

func NewServer(st *store.RocksStore) *Server { return &Server{st: st} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// old endpoints (keep compatibility)
	mux.HandleFunc("/block/by-number/", s.handleBlockByNumber)
	mux.HandleFunc("/block/by-hash/", s.handleBlockByHash)

	// new endpoints (time-window primitives)
	mux.HandleFunc("/chain/head", s.handleChainHead)
	mux.HandleFunc("/block/at-or-after", s.handleBlockAtOrAfter)
	mux.HandleFunc("/blocks/range", s.handleBlocksRange)

	return mux
}

// -------------------- helpers --------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, msg string) {
	http.Error(w, msg, http.StatusBadRequest)
}

// -------------------- old handlers --------------------
func (s *Server) handleBlockByNumber(w http.ResponseWriter, r *http.Request) {
	nStr := strings.TrimPrefix(r.URL.Path, "/block/by-number/")
	n, err := strconv.ParseInt(nStr, 10, 64)
	if err != nil || n <= 0 {
		badRequest(w, "bad block number")
		return
	}

	raw, err := s.st.GetCanonicalBlockRaw(n)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}

	blk, err := model.DecodeBlock(raw)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	writeJSON(w, 200, blk)
}

func (s *Server) handleBlockByHash(w http.ResponseWriter, r *http.Request) {
	hashStr := strings.TrimPrefix(r.URL.Path, "/block/by-hash/")
	h, err := hash.String2Hash32(hashStr)
	if err != nil {
		badRequest(w, "bad block hash")
		return
	}
	raw, err := s.st.GetBlockByHashRaw(h)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}

	blk, err := model.DecodeBlock(raw)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	writeJSON(w, 200, blk)
}

// -------------------- new handlers --------------------

// /chain/head returns head {num, hash, timestamp}.
func (s *Server) handleChainHead(w http.ResponseWriter, r *http.Request) {
	headNum, okN, err := s.st.HeadNum()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	headHash, okH, err := s.st.HeadHash()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if !okN || !okH || headNum <= 0 {
		writeJSON(w, 200, map[string]any{"empty": true})
		return
	}

	raw, err := s.st.GetCanonicalBlockRaw(headNum)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	blk, err := model.DecodeBlock(raw)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	writeJSON(w, 200, map[string]any{
		"head_num":       headNum,
		"head_hash":      headHash.Hex(),
		"head_timestamp": blk.Header.Timestamp,
	})
}

// /block/at-or-after?ts=1700000000
// returns the first canonical block whose timestamp >= ts (lower_bound).
func (s *Server) handleBlockAtOrAfter(w http.ResponseWriter, r *http.Request) {
	tsStr := r.URL.Query().Get("ts")
	if tsStr == "" {
		badRequest(w, "missing query param: ts")
		return
	}
	targetTs, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil || targetTs < 0 {
		badRequest(w, "bad ts")
		return
	}

	// lower_bound(timestamp >= targetTs)
	pos, _, ok, err := s.st.LowerBoundByTimestamp(targetTs)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if !ok {
		http.Error(w, "not found", 404)
		return
	}

	raw, err := s.st.GetCanonicalBlockRaw(pos)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	blk, err := model.DecodeBlock(raw)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	writeJSON(w, 200, map[string]any{
		"block_num":       blk.Header.Number,
		"block_hash":      blk.Hash.Hex(),
		"block_timestamp": blk.Header.Timestamp,
		"block":           blk,
	})
}

// /blocks/range?from=100&to=200
func (s *Server) handleBlocksRange(w http.ResponseWriter, r *http.Request) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	if fromStr == "" || toStr == "" {
		badRequest(w, "missing query params: from, to")
		return
	}
	from, err1 := strconv.ParseInt(fromStr, 10, 64)
	to, err2 := strconv.ParseInt(toStr, 10, 64)
	if err1 != nil || err2 != nil || from <= 0 || to <= 0 || from > to {
		badRequest(w, "bad range")
		return
	}

	// Optional: cap to avoid huge response
	const maxRange = int64(2000)
	if to-from+1 > maxRange {
		badRequest(w, "range too large")
		return
	}

	headNum, ok, err := s.st.HeadNum()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if !ok || headNum <= 0 {
		http.Error(w, "empty chain", 404)
		return
	}

	// clamp to head
	usedTo := to
	if usedTo > headNum {
		usedTo = headNum
	}
	if from > usedTo {
		// asked range entirely beyond head
		writeJSON(w, 200, map[string]any{
			"from":      from,
			"to":        usedTo,
			"blocks":    []model.Block{},
			"partial":   false,
			"cancelled": false,
		})
		return
	}

	out := make([]model.Block, 0, usedTo-from+1)
	partial := false
	var lastOK int64 = 0

	for n := from; n <= usedTo; n++ {
		// ctx-aware: if client is gone / server shutting down, stop wasting IO/CPU
		select {
		case <-r.Context().Done():
			// do NOT write response; connection is likely gone anyway
			return
		default:
		}

		raw, err := s.st.GetCanonicalBlockRaw(n)
		if err != nil {
			// canonical missing: return what we have as partial
			partial = true
			break
		}

		blk, err := model.DecodeBlock(raw)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		out = append(out, blk)
		lastOK = n
	}

	writeJSON(w, 200, map[string]any{
		"from":    from,
		"to":      usedTo,
		"blocks":  out,
		"partial": partial,
		"last_ok": lastOK, // 0 means none
	})
}
