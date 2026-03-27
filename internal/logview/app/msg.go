package app

import "github.com/chenzhangda16/web3-logpipe/internal/logpipe/bench"

type procRowMsg struct {
	Row bench.ProcJson
}

type procErrMsg struct {
	Err error
}
