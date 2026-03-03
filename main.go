// core: single storage node internals

// cluster: cluster management, node discovery, cluster membership and other distributed system related features

// main.go: entry point for the application

package main

import (
	"flag"

	"github.com/AymenBenyoub/nawst/core"
)

var port = flag.Int("port", 9999, "server port")

func main() {
	flag.Parse()
	store := core.NewStore()
	err := core.ReplayWal("ops.wal", store.Apply)
	if err != nil {
		panic(err)
	}
	wal, err := core.NewWal("ops.wal", 1024, core.AckAfterFlush)

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
