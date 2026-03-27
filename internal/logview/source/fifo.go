package source

import (
	"bufio"
	"context"
	"os"
)

func ReadLines(ctx context.Context, path string, out chan<- []byte) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 4*1024*1024)

	for sc.Scan() {
		line := append([]byte(nil), sc.Bytes()...)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- line:
		}
	}
	return sc.Err()
}
