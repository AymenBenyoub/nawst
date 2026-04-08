package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/AymenBenyoub/nawst/cluster"
	"github.com/AymenBenyoub/nawst/core"
)

func main() {
	var rpc_port = flag.Int("rpc-port", 9999, "grpc server port")
	var rpcHost = flag.String("rpc-host", "127.0.0.1", "grpc advertise host/ip used by peers")

	var raftPort = flag.Int("raft-port", 0, "raft tcp port (default rpc-port+1000)")
	var raftBindAddr = flag.String("raft-bind-addr", "0.0.0.0", "raft bind address")
	var raftAdvertiseIP = flag.String("raft-advertise-ip", "", "raft advertise IP used by peers")

	var ack = flag.Int("ack", 1, "ack mode: 0=after enqueue, 1=after flush, 2=after fsync")

	var gossipBindAddr = flag.String("gossip-bind-addr", "0.0.0.0", "memberlist bind address")
	var gossipPort = flag.Int("gossip-port", 0, "memberlist gossip port (0 selects random port on non-seed node)")
	var gossipAdvertiseIP = flag.String("gossip-advertise-ip", "127.0.0.1", "memberlist advertise IP used by peers")
	var seedGossipAddr = flag.String("seed-gossip-addr", "127.0.0.1:7946", "seed node memberlist address")

	var bandwidthMbps = flag.Int("bandwidth-mbps", 50, "estimated node NIC bandwidth in Mbps (static capacity denominator)")
	var diskPath = flag.String("disk-path", "/", "filesystem path used for disk capacity/usage metrics")
	var metricsInterval = flag.Duration("metrics-interval", 5*time.Second, "interval for collecting and gossiping node metrics")

	var walDir = flag.String("wal-dir", "", "directory for WAL files (default: kvst/node-<rpc-port>)")
	var replicationFactor = flag.Int("rf", 3, "replication factor for the cluster")
	flag.Parse()
	const writerBufferSize = 64 * 1024
	const requestChannelSize = 10000
	if *raftAdvertiseIP == "" {
		*raftAdvertiseIP = *rpcHost
	}
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

	server := core.NewServer(reqCh, store)
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
	replicator.SetTransferApplier(func(cmd core.Command) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp := server.SendInternal(ctx, core.Request{
			Op:           cmd.Op,
			Key:          cmd.Key,
			Value:        cmd.Value,
			VNodeID:      cmd.VNodeID,
			Version:      cmd.Version,
			ResponseChan: make(chan core.Response, 1),
		})
		return resp.Err
	})
	replicator.SetTransferDropper(func(vnodeID uint16) error {
		keys := store.GetKeysForVNode(vnodeID)
		for _, key := range keys {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp := server.SendInternal(ctx, core.Request{
				Op:           core.OpDelete,
				Key:          key,
				VNodeID:      vnodeID,
				Version:      server.NextVersion(),
				ResponseChan: make(chan core.Response, 1),
			})
			cancel()
			if resp.Err != nil {
				return resp.Err
			}
		}
		store.DeleteVNodeData(vnodeID)
		return nil
	})
	raftDataDir := filepath.Join(baseDir, resolvedWalDir, "raft")
	rn, err := cluster.NewRaftNode(
		nodeID,
		fmt.Sprintf("%s:%d", *raftBindAddr, resolvedRaftPort),
		raftAddr,
		raftDataDir,
		*rpc_port == 9999,
		replicator.ApplyPlacementFromRaft,
	)
	if err != nil {
		panic(err)
	}
	replicator.SetRaft(rn)

	collector := cluster.NewMetricsCollector(
		nodeID,
		*bandwidthMbps,
		cluster.DetectDiskPath(*diskPath),
	)
	replicator.SetMetricsCollector(collector)
	replicator.SetGossipBroadcaster(Node.QueueBroadcastMessage)
	Node.SetMetricsHandler(replicator.HandleMetricsMessage)
	replicator.StartMetricsReporter(*metricsInterval)
	defer replicator.StopMetricsReporter()

	Node.Reconciler = replicator
	Node.StartMembershipWorkers()
	defer Node.StopMembershipWorkers()

	if *rpc_port != 9999 {
		time.Sleep(1 * time.Second)
		if err := Node.JoinCluster(*seedGossipAddr); err != nil {
			panic(err)
		}
	}

	// Trigger initial placement update; leader may skip until first metrics round is available.
	fmt.Println("[main] triggering initial placement update...")
	replicator.UpdatePlacement()

	server.Replicator = replicator
	defer replicator.Close()
	// fmt.Printf("Server running at %s\n", Node.Addr)
	if err := Node.Server.Start(*rpc_port); err != nil {
		panic(err)
	}
}
