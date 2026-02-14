package ready

import (
	"context"
	"errors"
	"log"
	"os"
	"syscall"
	"time"
)

// SignalFifoCtx tries to write `payload` to a named FIFO at `path`.
// - It uses O_NONBLOCK open to avoid goroutine deadlock when no reader exists.
// - It keeps retrying until success, ctx canceled, or timeout.
// - It is safe to call from a goroutine.
// Notes:
// - If the reader isn't open yet, syscall.Open returns ENXIO. We retry.
// - If timeout <= 0, a default timeout is used.
func SignalFifoCtx(ctx context.Context, path string, payload string, timeout time.Duration) {
	if path == "" {
		return
	}
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	if payload == "" {
		payload = "READY\n"
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	tick := time.NewTicker(80 * time.Millisecond)
	defer tick.Stop()

	for {
		fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			f := os.NewFile(uintptr(fd), path)
			_, _ = f.WriteString(payload)
			_ = f.Close()
			return
		}

		// No reader yet
		if errors.Is(err, syscall.ENXIO) {
			select {
			case <-ctx.Done():
				log.Printf("[ready] canceled before fifo ready: path=%s err=%v", path, ctx.Err())
				return
			case <-deadline.C:
				log.Printf("[ready] timeout waiting fifo reader: path=%s timeout=%s", path, timeout)
				return
			case <-tick.C:
				continue
			}
		}

		// Other errors: fail fast
		log.Printf("[ready] fifo open failed: path=%s err=%v", path, err)
		return
	}
}
