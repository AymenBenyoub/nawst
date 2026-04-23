package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/AymenBenyoub/nawst/cluster"
)

var (
	addr       = flag.String("addr", "localhost:9999", "server address (host:port)")
	rpcTimeout = flag.Duration("rpc-timeout", 5*time.Second, "timeout for each RPC operation")
)

func main() {
	flag.Parse()

	router := cluster.NewPlacementRouter(*addr)
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
			fmt.Println("Commands: get KEY, put KEY VALUE, putfile KEY PATH, delete KEY, quit")
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
