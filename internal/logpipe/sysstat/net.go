package sysstat

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type netSample struct {
	rxBytes uint64
	txBytes uint64
	ts      time.Time
}

type NetReader struct {
	mu              sync.Mutex
	iface           string
	capacityBytesPS float64
	prev            netSample
	init            bool
}

func NewNetReader(iface string, capacityBytesPS float64) *NetReader {
	return &NetReader{
		iface:           iface,
		capacityBytesPS: capacityBytesPS,
	}
}

// Read returns rx/tx bytes per second and utilization percent in [0,100].
// The first call establishes a baseline and returns ok=false.
func (r *NetReader) Read() (rxBps, txBps, rxPct, txPct float64, ok bool, err error) {
	cur, err := readProcNetDev(r.iface)
	if err != nil {
		return 0, 0, 0, 0, false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.init {
		r.prev = cur
		r.init = true
		return 0, 0, 0, 0, false, nil
	}

	prev := r.prev
	r.prev = cur

	dt := cur.ts.Sub(prev.ts).Seconds()
	if dt <= 0 {
		return 0, 0, 0, 0, false, nil
	}

	var rxDelta uint64
	var txDelta uint64
	if cur.rxBytes >= prev.rxBytes {
		rxDelta = cur.rxBytes - prev.rxBytes
	}
	if cur.txBytes >= prev.txBytes {
		txDelta = cur.txBytes - prev.txBytes
	}

	rxBps = float64(rxDelta) / dt
	txBps = float64(txDelta) / dt

	if r.capacityBytesPS > 0 {
		rxPct = rxBps * 100 / r.capacityBytesPS
		txPct = txBps * 100 / r.capacityBytesPS

		if rxPct < 0 {
			rxPct = 0
		}
		if rxPct > 100 {
			rxPct = 100
		}
		if txPct < 0 {
			txPct = 0
		}
		if txPct > 100 {
			txPct = 100
		}
	}

	return rxBps, txBps, rxPct, txPct, true, nil
}

func readProcNetDev(iface string) (netSample, error) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return netSample{}, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || !strings.Contains(line, ":") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		name := strings.TrimSpace(parts[0])
		if name != iface {
			continue
		}

		fields := strings.Fields(parts[1])
		// /proc/net/dev:
		// rx: bytes packets errs drop fifo frame compressed multicast
		// tx: bytes packets errs drop fifo colls carrier compressed
		if len(fields) < 16 {
			return netSample{}, fmt.Errorf("sysstat: unexpected /proc/net/dev line for %s: %q", iface, line)
		}

		rxBytes, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return netSample{}, fmt.Errorf("sysstat: parse rx bytes for %s: %w", iface, err)
		}
		txBytes, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			return netSample{}, fmt.Errorf("sysstat: parse tx bytes for %s: %w", iface, err)
		}

		return netSample{
			rxBytes: rxBytes,
			txBytes: txBytes,
			ts:      time.Now(),
		}, nil
	}

	if err := sc.Err(); err != nil {
		return netSample{}, err
	}
	return netSample{}, fmt.Errorf("sysstat: interface %q not found in /proc/net/dev", iface)
}
