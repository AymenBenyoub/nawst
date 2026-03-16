package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AymenBenyoub/nawst/cluster"
	"github.com/AymenBenyoub/nawst/core"
)

func main() {
	var rpc_port = flag.Int("rpc-port", 9999, "grpc server port")
	var ack = flag.Int("ack", 1, "ack mode: 0=after enqueue, 1=after flush, 2=after fsync")
	var gossipPort = flag.Int("gossip-port", 0, "memberlist gossip port (0 selects random port on non-seed node)")
	var seedGossipAddr = flag.String("seed-gossip-addr", "127.0.0.1:7946", "seed node memberlist address")

	var walDir = flag.String("wal-dir", "", "directory for WAL files (default: kvst/node-<rpc-port>)")
	flag.Parse()
	const writerBufferSize = 64 * 1024
	const requestChannelSize = 10000
	// determine cross-platform data directory
	resolvedWalDir := *walDir
	if resolvedWalDir == "" {
		resolvedWalDir = filepath.Join("kvst", fmt.Sprintf("node-%d", *rpc_port))
	}

	baseDir, err := os.UserConfigDir()
	if err != nil {
		panic(err)
	}

	if err := os.MkdirAll(filepath.Join(baseDir, resolvedWalDir), 0755); err != nil {
		panic(err)
	}

	walPath := filepath.Join(baseDir, resolvedWalDir, "ops.wal")

	store := core.NewStore()

	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		// file doesn't exist, nothing to replay
	} else {
		if err := core.ReplayWal(walPath, store.Apply); err != nil {
			panic(err)
		}
	}

	wal, err := core.NewWal(walPath, writerBufferSize, core.AckMode(*ack))
	if err != nil {
		panic(err)
	}
	defer wal.Close()

	reqCh := make(chan core.Request, requestChannelSize)
	eventLoop := &core.EventLoop{
		Store: store,
		Wal:   wal,
		ReqCh: reqCh,
	}
	go eventLoop.Run()

	server := core.NewServer(reqCh)
	nodeID := fmt.Sprintf("node-%d", *rpc_port)
	resolvedGossipPort := *gossipPort
	if resolvedGossipPort == 0 && *rpc_port == 9999 {
		resolvedGossipPort = 7946
	}
	// host, err := os.Hostname()
	// if err != nil {
	// 	panic(err)
	// }
	Node := &cluster.Node{
		ID:                nodeID,
		Addr:              fmt.Sprintf("%s:%d", "0.0.0.0", *rpc_port),
		Ml:                nil,
		EventLoop:         eventLoop,
		Server:            server,
		HealthScore:       0.75,
		GossipBindAddr:    "0.0.0.0",
		GossipBindPort:    resolvedGossipPort,
		GossipAdvertiseIP: "127.0.0.1",
	}
	if err := Node.CreateCluster(); err != nil {
		panic(err)
	}
	if *rpc_port != 9999 {
		if err := Node.JoinCluster(*seedGossipAddr); err != nil {
			panic(err)
		}
	}
	// fmt.Printf("Server running at %s\n", Node.Addr)
	if err := Node.Server.Start(*rpc_port); err != nil {
		panic(err)
	}
}
