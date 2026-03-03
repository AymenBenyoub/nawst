package main

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/AymenBenyoub/nawst/core"
)

var port = flag.Int("port", 9999, "server port")
var ack = flag.Int("ack", 1, "ack mode: 0=after enqueue, 1=after flush, 2=after fsync")
func main() {
	flag.Parse()

	// Determine cross-platform data directory
	baseDir, err := os.UserConfigDir()
	if err != nil {
		panic(err)
	}


	walDir := filepath.Join(baseDir, "kvst")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		panic(err)
	}

	walPath := filepath.Join(walDir, "ops.wal")

	store := core.NewStore()

	
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		// file doesn't exist, nothing to replay
	} else {
		if err := core.ReplayWal(walPath, store.Apply); err != nil {
			panic(err)
		}
	}

	
	wal, err := core.NewWal(walPath, 1024, core.AckMode(*ack))
	if err != nil {
		panic(err)
	}
	defer wal.Close()

	
	reqCh := make(chan core.Request, 1024)
	eventLoop := &core.EventLoop{
		Store: store,
		Wal:   wal,
		ReqCh: reqCh,
	}
	go eventLoop.Run()

	server := core.NewServer(reqCh)
	if err := server.Start(*port); err != nil {
		panic(err)
	}
}