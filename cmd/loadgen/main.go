package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AymenBenyoub/nawst/core/proto"
)

type LoadConfig struct {
	Addr      string
	Clients   int
	Requests  int
	KeySize   int
	ValueSize int
	Timeout   time.Duration
}

type Metrics struct {
	PutsOK   int64
	GetOK    int64
	DeleteOK int64
	Failures int64
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

func runClient(wg *sync.WaitGroup, cfg LoadConfig, id int, metrics *Metrics) {
	defer wg.Done()

	_, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	conn, err := grpc.NewClient(cfg.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Client %d] failed to connect: %v", id, err)
		return
	}
	defer conn.Close()

	client := proto.NewKVClient(conn)

	for i := 0; i < cfg.Requests; i++ {
		key := fmt.Sprintf("c%d:k%d", id, i)
		value := randomBytes(cfg.ValueSize)

		// PUT
		putCtx, putCancel := context.WithTimeout(context.Background(), cfg.Timeout)
		_, err := client.Put(putCtx, &proto.PutRequest{Key: key, Value: value})
		putCancel()
		if err != nil {
			log.Printf("[Client %d] PUT failed: %v", id, err)
			atomic.AddInt64(&metrics.Failures, 1)
			continue
		}
		atomic.AddInt64(&metrics.PutsOK, 1)

		// GET
		getCtx, getCancel := context.WithTimeout(context.Background(), cfg.Timeout)
		resp, err := client.Get(getCtx, &proto.GetRequest{Key: key})
		getCancel()
		if err != nil {
			log.Printf("[Client %d] GET failed: %v", id, err)
			atomic.AddInt64(&metrics.Failures, 1)
			continue
		}
		atomic.AddInt64(&metrics.GetOK, 1)

		if string(resp.Value) != string(value) {
			log.Printf("[Client %d] Data mismatch for key %s", id, key)
			atomic.AddInt64(&metrics.Failures, 1)
			continue
		}

		// DELETE
		delCtx, delCancel := context.WithTimeout(context.Background(), cfg.Timeout)
		_, err = client.Delete(delCtx, &proto.DeleteRequest{Key: key})
		delCancel()
		if err != nil {
			log.Printf("[Client %d] DELETE failed: %v", id, err)
			atomic.AddInt64(&metrics.Failures, 1)
			continue
		}
		atomic.AddInt64(&metrics.DeleteOK, 1)
	}
}

func main() {
	cfg := LoadConfig{}

	flag.StringVar(&cfg.Addr, "addr", "localhost:9999", "server host:port")
	flag.IntVar(&cfg.Clients, "clients", 20, "number of concurrent clients")
	flag.IntVar(&cfg.Requests, "requests", 200, "number of requests per client")
	flag.IntVar(&cfg.KeySize, "keysize", 16, "key size in bytes")
	flag.IntVar(&cfg.ValueSize, "valuesize", 256, "value size in bytes")
	flag.DurationVar(&cfg.Timeout, "timeout", 30*time.Second, "per-request timeout")
	flag.Parse()

	metrics := &Metrics{}
	var wg sync.WaitGroup
	start := time.Now()

	// Start ticker for periodic progress updates
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-ticker.C:
				putsOK := atomic.LoadInt64(&metrics.PutsOK)
				getsOK := atomic.LoadInt64(&metrics.GetOK)
				deletesOK := atomic.LoadInt64(&metrics.DeleteOK)
				failures := atomic.LoadInt64(&metrics.Failures)
				totalOps := putsOK + getsOK + deletesOK
				elapsed := time.Since(start).Seconds()
				throughput := float64(totalOps) / elapsed
				fmt.Printf("[Progress] Elapsed: %.1fs | PUTs: %d | GETs: %d | DELETEs: %d | Failures: %d | Total Ops: %d | Throughput: %.2f ops/sec\n",
					elapsed, putsOK, getsOK, deletesOK, failures, totalOps, throughput)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go runClient(&wg, cfg, i, metrics)
	}

	wg.Wait()
	done <- true

	// Final summary
	putsOK := atomic.LoadInt64(&metrics.PutsOK)
	getsOK := atomic.LoadInt64(&metrics.GetOK)
	deletesOK := atomic.LoadInt64(&metrics.DeleteOK)
	failures := atomic.LoadInt64(&metrics.Failures)
	totalOps := putsOK + getsOK + deletesOK
	elapsed := time.Since(start)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("LOAD TEST SUMMARY")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Total Duration: %s\n", elapsed)
	fmt.Printf("Total Operations: %d\n", totalOps)
	fmt.Printf("  - PUTs:    %d\n", putsOK)
	fmt.Printf("  - GETs:    %d\n", getsOK)
	fmt.Printf("  - DELETEs: %d\n", deletesOK)
	fmt.Printf("Failed Operations: %d\n", failures)
	fmt.Printf("Success Rate: %.2f%%\n", float64(totalOps)*100/float64(totalOps+failures))
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(totalOps)/elapsed.Seconds())
	fmt.Printf("Avg Latency: %.2f ms\n", elapsed.Seconds()*1000/float64(totalOps))
	fmt.Println(strings.Repeat("=", 80))
}
