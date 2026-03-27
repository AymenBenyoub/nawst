package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
"log"
	"github.com/AymenBenyoub/nawst/cluster"
	"github.com/AymenBenyoub/nawst/core"
)

func main() {
	var rpc_port = flag.Int("rpc-port", 9999, "grpc server port")
	var rpcHost = flag.String("rpc-host", "127.0.0.1", "grpc advertise host/ip used by peers")
	var raftPort = flag.Int("raft-port", 0, "raft tcp port (default rpc-port+1000)")
	var raftBindAddr = flag.String("raft-bind-addr", "0.0.0.0", "raft bind address")
	var raftAdvertiseIP = flag.String("raft-advertise-ip", "127.0.0.1", "raft advertise IP used by peers")
	var ack = flag.Int("ack", 1, "ack mode: 0=after enqueue, 1=after flush, 2=after fsync")
	var gossipBindAddr = flag.String("gossip-bind-addr", "0.0.0.0", "memberlist bind address")
	var gossipPort = flag.Int("gossip-port", 0, "memberlist gossip port (0 selects random port on non-seed node)")
	var gossipAdvertiseIP = flag.String("gossip-advertise-ip", "127.0.0.1", "memberlist advertise IP used by peers")
	var seedGossipAddr = flag.String("seed-gossip-addr", "127.0.0.1:7946", "seed node memberlist address")

	var walDir = flag.String("wal-dir", "", "directory for WAL files (default: kvst/node-<rpc-port>)")
	var replicationFactor = flag.Int("rf", 3, "replication factor for the cluster")
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
		log.Printf("Could not get user config dir, falling back to current directory.")
		baseDir = "."
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
	resolvedRaftPort := *raftPort
	if resolvedRaftPort == 0 {
		resolvedRaftPort = *rpc_port + 1000
	}
	raftAddr := fmt.Sprintf("%s:%d", *raftAdvertiseIP, resolvedRaftPort)
	resolvedGossipPort := *gossipPort
	if resolvedGossipPort == 0 && *rpc_port == 9999 {
		resolvedGossipPort = 7946
	}
	// host, err := os.Hostname()
	// if err != nil {
	// 	panic(err)
	// }
	Node := &cluster.Node{
		ID:        nodeID,
		RPCAddr:   fmt.Sprintf("%s:%d", *rpcHost, *rpc_port),
		RaftAddr:  raftAddr,
		Ml:        nil,
		EventLoop: eventLoop,
		Server:    server,

		GossipBindAddr:    *gossipBindAddr,
		GossipBindPort:    resolvedGossipPort,
		GossipAdvertiseIP: *gossipAdvertiseIP,
	}
	if err := Node.CreateCluster(); err != nil {
		panic(err)
	}
	replicator := cluster.NewReplicator(nodeID, Node.Ml, *replicationFactor)
	raftDataDir := filepath.Join(baseDir, resolvedWalDir, "raft")
	rn, err := cluster.NewRaftNode(
		nodeID,
		fmt.Sprintf("%s:%d", *raftBindAddr, resolvedRaftPort),
		raftDataDir,
		*rpc_port == 9999,
		replicator.ApplyPlacementFromRaft,
	)
	if err != nil {
		panic(err)
	}
	replicator.SetRaft(rn)
	Node.Reconciler = replicator
	Node.StartMembershipWorkers()
	defer Node.StopMembershipWorkers()

	if *rpc_port != 9999 {
		if err := Node.JoinCluster(*seedGossipAddr); err != nil {
			panic(err)
		}
	}

	// Demo metrics and RTT matrix - will be used for placement updates
	demoMetrics := []cluster.NodeMetrics{
		{NodeID: "node-9999", AvgRTT: 1.1, BandwidthMbps: 950, NetUsage: 0.3},
		{NodeID: "node-10000", AvgRTT: 1.5, BandwidthMbps: 900, NetUsage: 0.30},
		{NodeID: "node-10001", AvgRTT: 2.0, BandwidthMbps: 800, NetUsage: 0.4},
		{NodeID: "node-10002", AvgRTT: 28.6, BandwidthMbps: 210, NetUsage: 0.85},
	}
	demoRTT := map[string]map[string]float64{
		"node-9999":  {"node-10000": 15, "node-10001": 11, "node-10002": 10},
		"node-10000": {"node-9999": 12, "node-10001": 15, "node-10002": 62},
		"node-10001": {"node-9999": 10, "node-10000": 16, "node-10002": 18},
		"node-10002": {"node-9999": 50, "node-10000": 20, "node-10001": 16},
	}

	// Store metrics in replicator for dynamic placement updates
	replicator.SetMetrics(demoMetrics, demoRTT)

	// Trigger initial placement update
	fmt.Println("[main] triggering initial placement update...")
	replicator.UpdatePlacement()

	server.Replicator = replicator
	defer replicator.Close()
	// fmt.Printf("Server running at %s\n", Node.Addr)
	if err := Node.Server.Start(*rpc_port); err != nil {
		panic(err)
	}
}
