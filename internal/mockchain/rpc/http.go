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
	mux.HandleFunc("/head", s.handleHead)
	mux.HandleFunc("/block/by-number/", s.handleBlockByNumber)
	mux.HandleFunc("/block/by-hash/", s.handleBlockByHash)
	return mux
}

func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	headHash, _, err := s.st.HeadHash()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	headNum, _, err := s.st.HeadNum()
	_ = json.NewEncoder(w).Encode(map[string]any{"head_hash": headHash.Hex(), "head_num": headNum})
}

func (s *Server) handleBlockByNumber(w http.ResponseWriter, r *http.Request) {
	nStr := strings.TrimPrefix(r.URL.Path, "/block/by-number/")
	n, err := strconv.ParseInt(nStr, 10, 64)
	if err != nil {
		http.Error(w, "bad block number", 400)
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

	_ = json.NewEncoder(w).Encode(blk)
}

func (s *Server) handleBlockByHash(w http.ResponseWriter, r *http.Request) {
	hashStr := strings.TrimPrefix(r.URL.Path, "/block/by-hash/")
	h, err := hash.String2Hash32(hashStr)
	if err != nil {
		http.Error(w, "bad block hash", 400)
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

	_ = json.NewEncoder(w).Encode(blk)
}
