package processor

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
)

type Config struct {
	Brokers string
	Group   string
	Topic   string

	WindowSec int64
	GapSec    int64

	CheckpointPath string
}

type Processor struct {
	cfg Config

	cons *Consumer
	ckpt Checkpoint

	mw    *MultiWindow
	dedup *Deduper

	// in-memory progress (per partition)
	offsets map[int32]int64

	lastTs   int64
	lastNum  int64
	lastHash string
}

func New(cfg Config) (*Processor, error) {
	if cfg.Topic == "" || cfg.Group == "" || cfg.Brokers == "" {
		return nil, errors.New("brokers/group/topic required")
	}
	if cfg.WindowSec <= 0 {
		cfg.WindowSec = 86400
	}
	if cfg.GapSec <= 0 {
		cfg.GapSec = 1
	}

	cons, err := NewConsumer(cfg.Brokers, cfg.Group, cfg.Topic)
	if err != nil {
		return nil, err
	}
	ck, err := NewFileCheckpoint(cfg.CheckpointPath)
	if err != nil {
		_ = cons.Close()
		return nil, err
	}
	wins := []WindowLike{
		NewWindow("1m", 60),
		NewWindow("5m", 300),
		NewWindow("1h", 3600),
		NewWindow("24h", 86400),
	}
	mw := NewMultiWindow(wins)

	p := &Processor{
		cfg:     cfg,
		cons:    cons,
		ckpt:    ck,
		mw:      mw,
		dedup:   NewDeduper(200_000), // rough hint; tune
		offsets: map[int32]int64{},
	}
	// load checkpoint (optional)
	if v, ok, err := p.ckpt.Load(); err != nil {
		return nil, err
	} else if ok {
		p.offsets = v.Offsets
		p.lastTs = v.LastTs
		p.lastNum = v.LastBlockNum
		p.lastHash = v.LastBlockHash
		log.Printf("[processor] loaded ckpt: last_num=%d last_ts=%d partitions=%d",
			p.lastNum, p.lastTs, len(p.offsets))
	}
	return p, nil
}

func (p *Processor) Close() error { return p.cons.Close() }

func (p *Processor) Run(ctx context.Context) error {
	h := &Handler{
		onBlock: p.handleBlock,
	}

	// consume loop (sarama requires re-run on rebalance)
	for {
		if err := p.cons.group.Consume(ctx, []string{p.cfg.Topic}, h); err != nil {
			log.Printf("[processor] consume err: %v", err)
			time.Sleep(300 * time.Millisecond)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (p *Processor) handleBlock(ctx context.Context, part int32, offset int64, blk model.Block) error {
	// Basic monotonic guard by timestamp (optional; helps catch weird input)
	ts := blk.Header.Timestamp
	if p.lastTs != 0 && ts+p.cfg.GapSec < p.lastTs {
		// Time went backwards "too much": treat as reorg/reset.
		// For now, return error to retry (or you can rebuild state).
		log.Printf("[processor] time went backwards: last=%d now=%d block=%d", p.lastTs, ts, blk.Header.Number)
		// In mock, we can accept or force restart; choose accept by not erroring:
		// return nil
	}

	// advance window evictions based on "nowTs" = blk timestamp
	p.mw.Evict(ts)
	p.dedup.Evict(ts)

	// Convert txs -> events
	for _, tx := range blk.Txs {
		ev := TxEvent{
			TxHash:    tx.Hash.Hex(),
			From:      tx.TxBody.From,
			To:        tx.TxBody.To,
			Timestamp: tx.TxBody.Timestamp,
			BlockNum:  blk.Header.Number,
			BlockHash: blk.Hash.Hex(),
		}

		// Dedup key: tx hash is enough for your mock; if you later allow same tx hash across forks,
		// switch to key = blkHash + ":" + txHash.
		key := ev.TxHash
		expire := ev.Timestamp + p.mw.MaxWindowSec()

		if seen := p.dedup.SeenOrAdd(key, expire, ts); seen {
			continue
		}
		p.mw.Add(ev)

		// TODO: here you can compute flags/metrics and produce toISK outputs to another topic or DB
	}

	// progress bookkeeping
	p.offsets[part] = offset + 1 // next offset
	p.lastTs = ts
	p.lastNum = blk.Header.Number
	p.lastHash = blk.Hash.Hex()

	// checkpoint periodically (cheap file write; you can batch every N blocks)
	if blk.Header.Number%100 == 0 {
		_ = p.ckpt.Save(ProcCkpt{
			Offsets:       p.offsets,
			LastBlockNum:  p.lastNum,
			LastBlockHash: p.lastHash,
			LastTs:        p.lastTs,
		})
	}

	return nil
}
