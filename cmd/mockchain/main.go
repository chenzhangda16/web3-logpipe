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

	"golang.org/x/sync/errgroup"

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)

	// 1) miner
	g.Go(func() error {
		err := m.Run(gctx)
		if err == context.Canceled {
			return nil
		}
		return err
	})

	// 2) http server
	srv := &http.Server{
		Addr:    *rpcAddr,
		Handler: rpc.NewServer(st).Handler(),
	}

	// ListenAndServe 放进 errgroup
	g.Go(func() error {
		err := srv.ListenAndServe()
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	})

	// 3) shutdown 协程：ctx cancel 后优雅关 server（带超时，防止卡死）
	g.Go(func() error {
		<-gctx.Done()

		sdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sdCtx) // 忽略错误也行，按你口味

		return nil
	})

	log.Printf("mockchain rpc listening on %s, db=%s", *rpcAddr, *dbPath)

	if err := g.Wait(); err != nil {
		log.Printf("exiting with error: %v", err)
	}
}
