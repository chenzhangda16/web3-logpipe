package main

import (
	"flag"
	"log"

	"github.com/chenzhangda16/web3-logpipe/internal/logview/app"
)

func main() {
	var fifo string
	var schema string

	flag.StringVar(&fifo, "fifo", "", "path to input fifo")
	flag.StringVar(&schema, "schema", "proc", "viewer schema: proc|fetch")
	flag.Parse()

	if fifo == "" {
		log.Fatal("missing --fifo")
	}

	if err := app.Run(app.Config{
		FIFOPath: fifo,
		Schema:   schema,
	}); err != nil {
		log.Fatal(err)
	}
}
