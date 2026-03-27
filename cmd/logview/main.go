package main

import (
	"log"

	"github.com/chenzhangda16/web3-logpipe/internal/logview/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
