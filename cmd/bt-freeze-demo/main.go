package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

type Row struct {
	ID      int
	Node    string
	Status  string
	QPS     int
	Latency int
	Remark  string
}

func parseLine(line string) (Row, bool) {
	parts := strings.SplitN(strings.TrimSpace(line), "|", 6)
	if len(parts) != 6 {
		return Row{}, false
	}

	id, err1 := strconv.Atoi(parts[0])
	qps, err2 := strconv.Atoi(parts[3])
	lat, err3 := strconv.Atoi(parts[4])
	if err1 != nil || err2 != nil || err3 != nil {
		return Row{}, false
	}

	return Row{
		ID:      id,
		Node:    parts[1],
		Status:  parts[2],
		QPS:     qps,
		Latency: lat,
		Remark:  parts[5],
	}, true
}

type Generator struct {
	nextID int
	rng    *rand.Rand
}

func newGenerator() *Generator {
	return &Generator{
		nextID: 1,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (g *Generator) genRow() Row {
	nodes := []string{"main", "pc127", "m2", "pixel", "writer", "fetcher", "processor"}

	node := nodes[g.rng.Intn(len(nodes))]
	qps := 60 + g.rng.Intn(260)
	lat := 5 + g.rng.Intn(180)

	status := "OK"
	remark := "stable"

	switch {
	case lat >= 120 || qps <= 80:
		status = "ERROR"
		remark = "drop/retry pressure"
	case lat >= 60 || qps <= 120:
		status = "WARN"
		remark = "backpressure rising"
	default:
		status = "OK"
		remark = "stable"
	}

	if node == "fetcher" && qps > 240 {
		remark = "burst fetch spike"
	}
	if node == "processor" && lat > 100 {
		remark = "window compute hot"
	}
	if node == "writer" && lat > 80 {
		remark = "sink flush slow"
	}

	row := Row{
		ID:      g.nextID,
		Node:    node,
		Status:  status,
		QPS:     qps,
		Latency: lat,
		Remark:  remark,
	}
	g.nextID++
	return row
}

func formatRow(r Row) string {
	return fmt.Sprintf(
		"ID=%06d  NODE=%-10s  STATUS=%-5s  QPS=%3d  LAT=%3dms  REMARK=%s",
		r.ID, r.Node, r.Status, r.QPS, r.Latency, r.Remark,
	)
}

func streamFromFIFO(path string) {
	for {
		f, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[fifo-open-error] %v\n", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			row, ok := parseLine(line)
			if !ok {
				fmt.Printf("[fifo-raw] %s\n", line)
				continue
			}
			fmt.Printf("[fifo] %s\n", formatRow(row))
		}

		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "[fifo-scan-error] %v\n", err)
		}

		_ = f.Close()
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	fifoPath := flag.String("fifo", "", "path to input fifo")
	autoGen := flag.Bool("auto", true, "enable automatic row generation")
	intervalMs := flag.Int("interval-ms", 120, "auto generation interval in milliseconds")
	flag.Parse()

	if *intervalMs < 10 {
		*intervalMs = 10
	}

	fmt.Println("Append Demo · plain stdout mode · Ctrl+F scrollback experiment")
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println(strings.Repeat("─", 96))

	if *fifoPath != "" {
		go streamFromFIFO(*fifoPath)
	}

	if !*autoGen {
		select {}
	}

	gen := newGenerator()
	ticker := time.NewTicker(time.Duration(*intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		row := gen.genRow()
		fmt.Println(formatRow(row))
	}
}
