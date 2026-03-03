package main

import (
	"bufio"
	"context"
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

func main() {
	// default server address
	addr := "localhost:9999"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatalf("failed to connect to server %s: %v", addr, err)
	}
	defer conn.Close()

	client := pb.NewKVClient(conn)

	fmt.Println("Connected to", addr)
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break // EOF
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "quit", "exit":
			return
		case "help":
			fmt.Println("Commands: get KEY, put KEY VALUE, putfile KEY PATH, delete KEY, quit")
		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get KEY")
				continue
			}
			key := parts[1]
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			c()
			if err != nil {
				fmt.Println("Get error:", err)
				continue
			}
			fmt.Println(string(resp.GetValue()))
		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put KEY VALUE")
				continue
			}
			key := parts[1]
			val := []byte(parts[2])
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: val})
			c()
			if err != nil {
				fmt.Println("Put error:", err)
				continue
			}
			fmt.Println("OK")
		case "putfile":
			if len(parts) < 3 {
				fmt.Println("usage: putfile KEY PATH")
				continue
			}
			key := parts[1]
			path := parts[2]
			data, err := os.ReadFile(path)
			if err != nil {
				fmt.Println("file read error:", err)
				continue
			}
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = client.Put(ctx, &pb.PutRequest{Key: key, Value: data})
			c()
			if err != nil {
				fmt.Println("Put error:", err)
				continue
			}
			fmt.Println("OK")
		case "delete":
			if len(parts) < 2 {
				fmt.Println("usage: delete KEY")
				continue
			}
			key := parts[1]
			ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
			c()
			if err != nil {
				fmt.Println("Delete error:", err)
				continue
			}
			fmt.Println("OK")
		default:
			fmt.Println("unknown command; type help")
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		log.Fatalf("input error: %v", err)
	}
}