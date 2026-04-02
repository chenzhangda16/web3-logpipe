package app

import "github.com/chenzhangda16/web3-logpipe/internal/logpipe/bench"

type rowMsg[T any] struct {
	Row T
}

type errMsg struct {
	Err error
}

type fetchRowMsg struct {
	Row bench.FetchJson
}
