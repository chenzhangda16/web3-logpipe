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

const AddrPool = "addr_pool"

func main() {
	var (
		dbPath    = flag.String("db", "./data/mockchain.db", "rocksdb path")
		rpcAddr   = flag.String("rpc", ":18080", "rpc listen addr")
		addrCount = flag.Int("addr", 5000, "address pool size")
		det       = flag.Bool("det", false, "determine whether or not the chain is reproducible")
		seed      = flag.Int64("seed", 1, "seed for deterministic generation")
		tick      = flag.Duration("tick", 1*time.Second, "block interval")

		// backfill-sec:
		//   >0 : warmup/backfill on startup (e.g. 86400 for 24h)
		//   =0 : no strict window requirement, still can "catch up" if DecideTailAction says so
		//   <0 : disable warmup/backfill entirely
		backfillSec = flag.Int64("backfill-sec", 86400, "seconds to backfill on startup; -1 disables warmup/backfill")

		// gap-sec:
		//   defines contiguity: adjacent blocks with (ts[i+1]-ts[i])<=gap-sec are considered contiguous
		//   if <=0, defaults to 3*tickSec (min 1)
		gapSec = flag.Int64("gap-sec", 0, "contiguity gap threshold in seconds; <=0 means default=3*tickSec")
	)
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	// RNG factory
	rf := rng.New(map[bool]rng.Mode{true: rng.Deterministic, false: rng.Real}[*det], *seed)

	addrs := generator.GenAddrs(*addrCount, rf.R(AddrPool))
	txgen := generator.NewTxGen(addrs, rf)

	m := miner.NewMiner(st, txgen, rf, *tick)

	// --- Warmup / Backfill (sync) ---
	if *backfillSec > 0 {
		start := time.Now()
		if err := m.Warmup(*backfillSec, *gapSec); err != nil {
			log.Fatal(err)
		}
		log.Printf("warmup done: backfill=%ds gap=%ds tick=%s cost=%s.", *backfillSec, *gapSec, tick.String(), time.Since(start).String())
	} else {
		log.Printf("warmup disabled.")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Miner (single writer) keeps producing blocks
	go func() {
		// 用你实际存在的方法名：Run 或 RunBackup
		if err := m.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("miner stopped: %v", err)
			cancel()
		}
	}()

	// RPC server (read-only)
	srv := &http.Server{
		Addr:    *rpcAddr,
		Handler: rpc.NewServer(st).Handler(),
	}

	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()

	log.Printf("mockchain rpc listening on %s, db=%s", *rpcAddr, *dbPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
