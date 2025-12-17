package main

//
//import (
//	"encoding/json"
//	"log"
//	"math/rand"
//	"net/http"
//	"time"
//
//	"github.com/chenzhangda16/web3-logpipe/internal/mockchain"
//)
//
//func main() {
//	rand.Seed(time.Now().UnixNano())
//
//	mc := mockchain.NewMockChain(1000) // 比如 1000 个地址
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/block/next", func(w http.ResponseWriter, r *http.Request) {
//		blk := mc.NextBlock()
//		w.Header().Set("Content-Type", "application/json")
//		_ = json.NewEncoder(w).Encode(blk)
//	})
//
//	srv := &http.Server{
//		Addr:    ":8080",
//		Handler: mux,
//	}
//
//	log.Println("MockChain listening on :8080")
//	if err := srv.ListenAndServe(); err != nil {
//		log.Fatal(err)
//	}
//}
