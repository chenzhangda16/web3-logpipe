package obs

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	bootID  atomic.Value // string
	rootDir string
)

func Init(service string) {
	cwd, _ := os.Getwd()
	rootDir = cwd

	bootID.Store(service + "#" + time.Now().Format("20060102_150405.000000"))
	log.Printf("[boot] id=%s pid=%d root=%s",
		bootID.Load().(string), os.Getpid(), rootDir)
}

func P(format string, args ...any) {
	id, _ := bootID.Load().(string)

	_, file, line, ok := runtime.Caller(1)
	loc := "?:0"
	if ok {
		if rel, err := filepath.Rel(rootDir, file); err == nil {
			loc = fmt.Sprintf("%s:%d", rel, line)
		} else {
			loc = fmt.Sprintf("%s:%d", filepath.Base(file), line)
		}
	}

	ts := time.Now().Format("15:04:05.000000")
	log.Printf("[T=%s] %s %s "+format,
		append([]any{id, ts, loc}, args...)...)
}
