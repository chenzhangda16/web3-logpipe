package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/generator"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/miner"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/rpc"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/store"
	"github.com/chenzhangda16/web3-logpipe/pkg/rng"
)

func main() {
	var (
		dbPath    = flag.String("db", "./data/mockchain.db", "rocksdb path")
		rpcAddr   = flag.String("rpc", ":8080", "rpc listen addr")
		addrCount = flag.Int("addr", 5000, "address pool size")
		det       = flag.Bool("det", false, "determine whether or not the chain is Reproducible")
		seed      = flag.Int64("seed", 1, "seed for deterministic generation")
		tick      = flag.Duration("tick", 1*time.Second, "block interval")
	)
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	// 随机化工厂初始化
	rf := rng.New(map[bool]rng.Mode{true: rng.Deterministic, false: rng.Real}[*det], *seed)

	addrs := generator.GenAddrs(*addrCount, rf.R(rng.AddrPool))
	txgen := generator.NewTxGen(addrs, rf)

	m := miner.NewMiner(st, txgen, rf, *tick)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 启动 Miner（单写）
	go func() {
		if err := m.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("miner stopped: %v", err)
			cancel()
		}
	}()

	// 启动 RPC（只读）
	srv := &http.Server{
		Addr:    *rpcAddr,
		Handler: rpc.NewServer(st).Handler(),
	}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	log.Printf("mockchain rpc listening on %s, db=%s", *rpcAddr, *dbPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
