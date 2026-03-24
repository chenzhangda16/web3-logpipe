package sysstat

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

type cpuSample struct {
	idle  uint64
	total uint64
}

type CPUReader struct {
	mu   sync.Mutex
	prev cpuSample
	init bool
}

func NewCPUReader() *CPUReader {
	return &CPUReader{}
}

// ReadPct returns CPU usage percent in [0, 100].
// The first call establishes a baseline and returns (0, false, nil).
// Subsequent calls return (pct, true, nil).
func (r *CPUReader) ReadPct() (pct float64, ok bool, err error) {
	cur, err := readProcStatCPU()
	if err != nil {
		return 0, false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.init {
		r.prev = cur
		r.init = true
		return 0, false, nil
	}

	prev := r.prev
	r.prev = cur

	idleDelta := cur.idle - prev.idle
	totalDelta := cur.total - prev.total

	if totalDelta == 0 {
		return 0, false, nil
	}

	usedDelta := totalDelta - idleDelta
	pct = float64(usedDelta) * 100 / float64(totalDelta)

	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return pct, true, nil
}

func readProcStatCPU() (cpuSample, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuSample{}, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return cpuSample{}, err
		}
		return cpuSample{}, fmt.Errorf("sysstat: /proc/stat is empty")
	}

	line := strings.TrimSpace(sc.Text())
	// expected:
	// cpu  user nice system idle iowait irq softirq steal guest guest_nice
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuSample{}, fmt.Errorf("sysstat: unexpected /proc/stat first line: %q", line)
	}

	var nums []uint64
	for i := 1; i < len(fields); i++ {
		v, err := strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			return cpuSample{}, fmt.Errorf("sysstat: parse /proc/stat field %q: %w", fields[i], err)
		}
		nums = append(nums, v)
	}

	var total uint64
	for _, v := range nums {
		total += v
	}

	// idle = idle + iowait when available
	idle := nums[3]
	if len(nums) > 4 {
		idle += nums[4]
	}

	return cpuSample{
		idle:  idle,
		total: total,
	}, nil
}
