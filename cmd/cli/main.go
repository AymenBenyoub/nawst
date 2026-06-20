package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/AymenBenyoub/nawst/cluster"
	pb "github.com/AymenBenyoub/nawst/core/proto"
)

var (
	addr                  = flag.String("addr", "localhost:9999", "server address (host:port)")
	rpcTimeout            = flag.Duration("rpc-timeout", 5*time.Second, "timeout for each RPC operation")
	tlsEnable             = flag.Bool("tls-enable", false, "enable TLS for gRPC client connections")
	tlsCAFile             = flag.String("tls-ca-cert-file", "", "path to CA certificate PEM for server verification")
	tlsServerName         = flag.String("tls-server-name", "", "TLS server name for certificate verification")
	tlsInsecureSkipVerify = flag.Bool("tls-insecure-skip-verify", false, "skip TLS cert hostname/chain verification (not recommended)")
)

func main() {
	flag.Parse()

	router := cluster.NewPlacementRouter(*addr)
	if err := router.ConfigureTLS(cluster.ClientTLSConfig{
		Enabled:            *tlsEnable,
		CACertFile:         *tlsCAFile,
		ServerName:         *tlsServerName,
		InsecureSkipVerify: *tlsInsecureSkipVerify,
	}); err != nil {
		log.Fatalf("invalid TLS configuration: %v", err)
	}
	refreshCtx, cancel := context.WithTimeout(context.Background(), *rpcTimeout)
	refreshErr := router.Refresh(refreshCtx)
	cancel()
	if refreshErr != nil {
		log.Printf("placement refresh failed; commands will fail until placement is available: %v", refreshErr)
	}
	defer router.Close()

	fmt.Printf("KV CLI started. Target: %s\nType 'help' for commands.\n", *addr)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("kvcli> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		// Logic for shared context per request
		ctx, cancel := context.WithTimeout(context.Background(), *rpcTimeout)

		switch cmd {
		case "quit", "exit":
			cancel()
			return
		case "help":
			fmt.Println("Commands: get KEY, put KEY VALUE, putfile KEY PATH, delete KEY, status, quit")
		case "status":
			status, err := router.Status(ctx)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println(formatClusterStatus(status))
			}
		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get KEY")
			} else {
				val, err := router.Get(ctx, parts[1])
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println(string(val))
				}
			}
		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put KEY VALUE")
			} else {
				if err := router.Put(ctx, parts[1], []byte(parts[2])); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("OK")
				}
			}
		case "putfile":
			if len(parts) < 3 {
				fmt.Println("usage: putfile KEY PATH")
			} else {
				data, err := os.ReadFile(parts[2])
				if err != nil {
					fmt.Printf("File error: %v\n", err)
				} else {
					if err := router.Put(ctx, parts[1], data); err != nil {
						fmt.Printf("Error: %v\n", err)
					} else {
						fmt.Println("OK")
					}
				}
			}
		case "delete":
			if len(parts) < 2 {
				fmt.Println("usage: delete KEY")
			} else {
				if err := router.Delete(ctx, parts[1]); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("OK")
				}
			}
		default:
			fmt.Println("Unknown command. Type 'help'.")
		}
		cancel()
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		log.Fatalf("input error: %v", err)
	}
}

func formatClusterStatus(status *pb.ClusterStatus) string {
	if status == nil {
		return "Cluster status unavailable"
	}
	state := status.GetState()
	if state == nil {
		state = &pb.ClusterState{}
	}

	nodes := state.GetNodes()
	vnodes := state.GetVnodes()
	primaryCounts := make(map[string]int, len(nodes))
	replicaTargets := make(map[string]map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node == nil || node.GetId() == "" {
			continue
		}
		if _, ok := replicaTargets[node.GetId()]; !ok {
			replicaTargets[node.GetId()] = make(map[string]struct{})
		}
	}
	for _, vnode := range vnodes {
		if vnode == nil {
			continue
		}
		primary := vnode.GetPrimary()
		if primary != "" {
			primaryCounts[primary]++
			if _, ok := replicaTargets[primary]; !ok {
				replicaTargets[primary] = make(map[string]struct{})
			}
			for _, replica := range vnode.GetReplicas() {
				if replica != "" && replica != primary {
					replicaTargets[primary][replica] = struct{}{}
				}
			}
		}
	}

	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node != nil && node.GetId() != "" {
			ids = append(ids, node.GetId())
		}
	}
	sort.Strings(ids)

	lines := []string{
		"Cluster status",
		fmt.Sprintf("Leader: %s", formatLeader(status.GetLeader())),
		fmt.Sprintf("Nodes: %d", len(nodes)),
		fmt.Sprintf("Placement epoch: %d", state.GetEpoch()),
		"Placement:",
	}
	for _, nodeID := range ids {
		replicas := sortedReplicaTargets(replicaTargets[nodeID])
		if len(replicas) == 0 {
			replicas = "none"
		}
		lines = append(lines, fmt.Sprintf("  %s: %d vnodes -> replicas: %s", nodeID, primaryCounts[nodeID], replicas))
	}
	return strings.Join(lines, "\n")
}

func formatLeader(leader string) string {
	leader = strings.TrimSpace(leader)
	if leader == "" {
		return "unknown"
	}
	return leader
}

func sortedReplicaTargets(targets map[string]struct{}) string {
	if len(targets) == 0 {
		return ""
	}
	ids := make([]string, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return strings.Join(ids, ", ")
}
