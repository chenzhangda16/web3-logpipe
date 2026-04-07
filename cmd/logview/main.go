package main

import (
	"flag"
	"log"

	"github.com/chenzhangda16/web3-logpipe/internal/logview/app"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	var fifo string
	var schema string
	var sample string

	flag.StringVar(&fifo, "fifo", "", "path to input fifo")
	flag.StringVar(&schema, "schema", "proc", "viewer schema: proc|fetch")
	flag.StringVar(&sample, "sample", "", "path to schema sample json")
	flag.Parse()

	if fifo == "" {
		log.Fatal("missing --fifo")
	}

	if err := app.Run(app.Config{
		FIFOPath:   fifo,
		Schema:     schema,
		SamplePath: sample,
	}); err != nil {
		log.Fatal(err)
	}
}
