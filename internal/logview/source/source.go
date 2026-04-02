package source

import (
	"context"
	"encoding/json"
)

func ReadJSON[T any](ctx context.Context, fifoPath string, out chan<- T) error {
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

			var v T
			if err := json.Unmarshal(line, &v); err != nil {
				// 保持你原来的策略：跳过坏行
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- v:
			}
		}
	}
}
