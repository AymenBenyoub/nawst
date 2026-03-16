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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/AymenBenyoub/nawst/core/proto"
)

var (
	addr       = flag.String("addr", "localhost:9999", "server address (host:port)")
	rpcTimeout = flag.Duration("rpc-timeout", 5*time.Second, "timeout for each RPC operation")
)

func main() {
	flag.Parse()

	// NewClient is non-blocking. It won't fail even if the server is offline.
	conn, err := grpc.NewClient(
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVClient(conn)
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
				resp, err := client.Get(ctx, &pb.GetRequest{Key: parts[1]})
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println(string(resp.GetValue()))
				}
			}
		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put KEY VALUE")
			} else {
				_, err := client.Put(ctx, &pb.PutRequest{Key: parts[1], Value: []byte(parts[2])})
				if err != nil {
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
					_, err = client.Put(ctx, &pb.PutRequest{Key: parts[1], Value: data})
					if err != nil {
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
				_, err := client.Delete(ctx, &pb.DeleteRequest{Key: parts[1]})
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("OK")
				}
			}
		default:
			fmt.Println("Unknown command. Type 'help'.")
		}
		cancel() // Clean up context after each command
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		log.Fatalf("input error: %v", err)
	}
}
