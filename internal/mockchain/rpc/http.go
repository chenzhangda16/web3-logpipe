package rpc

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

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
	mux.HandleFunc("/block/", s.handleBlockByNumber) // /block/{n}
	return mux
}

func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	head, err := s.st.Head()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"head": head})
}

func (s *Server) handleBlockByNumber(w http.ResponseWriter, r *http.Request) {
	nStr := strings.TrimPrefix(r.URL.Path, "/block/")
	n, err := strconv.ParseInt(nStr, 10, 64)
	if err != nil {
		http.Error(w, "bad block number", 400)
		return
	}

	raw, err := s.st.GetBlockRaw(n)
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
