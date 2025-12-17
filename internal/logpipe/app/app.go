package app

import (
	"context"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/fetcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/scheduler"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/worker"
)

type App struct {
	cfg       config.Config
	fetcher   *fetcher.HTTPFetcher
	scheduler *scheduler.Scheduler
	workers   []*worker.Worker
	writer    *writer.BatchWriter
}

func (a *App) Run(ctx context.Context) error {}
