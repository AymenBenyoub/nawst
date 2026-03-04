package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr       = flag.String("addr", "localhost:9999", "server address")
	clients    = flag.Int("clients", 50, "number of concurrent clients")
	requests   = flag.Int("requests", 10000, "total requests per client")
	valueSize  = flag.Int("value-size", 128, "size of value in bytes")
	printEvery = flag.Duration("print-every", 2*time.Second, "stats print interval")
)

func main() {
	flag.Parse()

	log.Printf("Target: %s\n", *addr)
	log.Printf("Clients: %d\n", *clients)
	log.Printf("Requests/client: %d\n", *requests)

	var totalOps uint64
	var totalLatency int64

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(*clients)

	for i := 0; i < *clients; i++ {
		go func(id int) {
			defer wg.Done()

			conn, err := grpc.NewClient(
				*addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				log.Fatalf("Client %d failed to connect: %v", id, err)
			}
			defer conn.Close()

			client := pb.NewKVClient(conn)

			for j := 0; j < *requests; j++ {
				key := fmt.Sprintf("k-%d-%d", id, j)
				value := randomBytes(*valueSize)

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

				t0 := time.Now()
				_, err := client.Put(ctx, &pb.PutRequest{
					Key:   key,
					Value: value,
				})
				cancel()

				if err != nil {
					log.Printf("Client %d request failed: %v\n", id, err)
					return
				}

				lat := time.Since(t0).Microseconds()

				atomic.AddUint64(&totalOps, 1)
				atomic.AddInt64(&totalLatency, lat)
			}
		}(i)
	}

	// Periodic stats
	go func() {
		ticker := time.NewTicker(*printEvery)
		defer ticker.Stop()

		for range ticker.C {
			ops := atomic.LoadUint64(&totalOps)
			elapsed := time.Since(start).Seconds()

			if elapsed == 0 {
				continue
			}

			fmt.Printf("Ops: %d | Throughput: %.2f ops/sec\n",
				ops,
				float64(ops)/elapsed,
			)
		}
	}()

	wg.Wait()

	duration := time.Since(start)
	ops := atomic.LoadUint64(&totalOps)
	latSum := atomic.LoadInt64(&totalLatency)

	fmt.Println("------ FINAL ------")
	fmt.Printf("Total Ops: %d\n", ops)
	fmt.Printf("Total Time: %v\n", duration)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(ops)/duration.Seconds())
	fmt.Printf("Avg Latency: %.2f ms\n", float64(latSum)/float64(ops)/1000)
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
