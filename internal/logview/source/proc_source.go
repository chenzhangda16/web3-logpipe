package source

import (
	"context"
	"encoding/json"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/bench"
)

func ReadProcJSON(ctx context.Context, fifoPath string, out chan<- bench.ProcJson) error {
	lines := make(chan []byte, 128)
	errCh := make(chan error, 1)

	go func() {
		errCh <- ReadLines(ctx, fifoPath, lines)
		close(lines)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-errCh:
			if err != nil {
				return err
			}
			return nil

		case line, ok := <-lines:
			if !ok {
				return nil
			}
			var pj bench.ProcJson
			if err := json.Unmarshal(line, &pj); err != nil {
				// 第一版先直接跳过坏行，不中断 viewer
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- pj:
			}
		}
	}
}
