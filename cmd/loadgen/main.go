package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
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
	ValueSize int
	Timeout   time.Duration
}

type Metrics struct {
	PutsOK   int64
	GetsOK   int64
	DeleteOK int64
	Failures int64
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func runClient(wg *sync.WaitGroup, client proto.KVClient, cfg LoadConfig, id int, metrics *Metrics) {
	defer wg.Done()

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
		if string(resp.Value) != string(value) {
			log.Printf("[Client %d] Data mismatch for key %s", id, key)
			atomic.AddInt64(&metrics.Failures, 1)
			continue
		}
		atomic.AddInt64(&metrics.GetsOK, 1)

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

func runPutOnlyClient(wg *sync.WaitGroup, client proto.KVClient, cfg LoadConfig, id int, metrics *Metrics) {
	defer wg.Done()

	value := randomBytes(cfg.ValueSize) // reuse same value, we're testing throughput not correctness

	for i := 0; i < cfg.Requests; i++ {
		key := fmt.Sprintf("c%d:k%d", id, i)

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		_, err := client.Put(ctx, &proto.PutRequest{Key: key, Value: value})
		cancel()
		if err != nil {
			atomic.AddInt64(&metrics.Failures, 1)
			continue
		}
		atomic.AddInt64(&metrics.PutsOK, 1)
	}
}
func runStreamClientMixed(wg *sync.WaitGroup, client proto.KVClient, cfg LoadConfig, id int, metrics *Metrics) {
    defer wg.Done()

    ctx := context.Background()
    stream, err := client.StreamKV(ctx)
    if err != nil {
        log.Printf("[Client %d] Stream init failed: %v", id, err)
        return
    }

    var inFlight int64
    doneSending := make(chan struct{})

    // 1. Response tallying goroutine
    go func() {
        for {
            resp, err := stream.Recv()
            if err == io.EOF {
                break
            }
            if err != nil {
                log.Printf("[Client %d] Stream recv error: %v", id, err)
                break
            }

            switch resp.Op {
            case proto.Op_PUT:
                atomic.AddInt64(&metrics.PutsOK, 1)
            case proto.Op_GET:
                atomic.AddInt64(&metrics.GetsOK, 1)
            case proto.Op_DELETE:
                atomic.AddInt64(&metrics.DeleteOK, 1)
            }

            atomic.AddInt64(&inFlight, -1)
        }
    }()

    // Cleaned up send function without the useless context timeout
    sendReq := func(req *proto.StreamReq) bool {
        if err := stream.Send(req); err != nil {
            atomic.AddInt64(&metrics.Failures, 1)
            return false
        }
        atomic.AddInt64(&inFlight, 1)
        return true
    }

    value := randomBytes(cfg.ValueSize)

    // 2. Send PUTs
    for i := 0; i < cfg.Requests; i++ {
        key := fmt.Sprintf("c%d:k%d", id, i)
        sendReq(&proto.StreamReq{Op: proto.Op_PUT, Key: key, Value: value})
    }

    // 3. Send GETs
    for i := 0; i < cfg.Requests; i++ {
        key := fmt.Sprintf("c%d:k%d", id, i)
        sendReq(&proto.StreamReq{Op: proto.Op_GET, Key: key})
    }

    // 4. Send DELETEs
    for i := 0; i < cfg.Requests; i++ {
        key := fmt.Sprintf("c%d:k%d", id, i)
        sendReq(&proto.StreamReq{Op: proto.Op_DELETE, Key: key})
    }

    close(doneSending)
    stream.CloseSend() // tell server we are done sending

    // 5. Wait for all ACKs
    for atomic.LoadInt64(&inFlight) > 0 {
        time.Sleep(1 * time.Millisecond)
    }
}
func main() {
	cfg := LoadConfig{}
	putOnly := false
	stream := false
	flag.StringVar(&cfg.Addr, "addr", "localhost:9999", "server host:port")
	flag.IntVar(&cfg.Clients, "clients", 200, "number of concurrent clients")
	flag.IntVar(&cfg.Requests, "requests", 4000, "number of requests per client")
	flag.IntVar(&cfg.ValueSize, "valuesize", 256, "value size in bytes")
	flag.DurationVar(&cfg.Timeout, "timeout", 30*time.Second, "per-request timeout")
	flag.BoolVar(&putOnly, "putonly", false, "hammer puts only, no get/delete interleaving")
	flag.BoolVar(&stream, "stream", false, "use streaming API instead of unary RPCs")
	flag.Parse()

	const numConns = 50
	clients := make([]proto.KVClient, cfg.Clients)
	conns := make([]*grpc.ClientConn, numConns)
	for i := 0; i < numConns; i++ {
		conn, err := grpc.NewClient(cfg.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}
		conns[i] = conn
		defer conn.Close()
	}
	for i := 0; i < cfg.Clients; i++ {
		clients[i] = proto.NewKVClient(conns[i%numConns])
	}

	metrics := &Metrics{}
	var wg sync.WaitGroup
	start := time.Now()

	// progress ticker
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				putsOK := atomic.LoadInt64(&metrics.PutsOK)
				getsOK := atomic.LoadInt64(&metrics.GetsOK)
				deletesOK := atomic.LoadInt64(&metrics.DeleteOK)
				failures := atomic.LoadInt64(&metrics.Failures)
				totalOps := putsOK + getsOK + deletesOK
				elapsed := time.Since(start).Seconds()
				fmt.Printf("[Progress] %.1fs | PUTs: %d | GETs: %d | DELs: %d | Failures: %d | %.2f ops/sec\n",
					elapsed, putsOK, getsOK, deletesOK, failures, float64(totalOps)/elapsed)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		if putOnly {
			go runPutOnlyClient(&wg, clients[i], cfg, i, metrics)
		} else if stream {
			go runStreamClientMixed(&wg, clients[i], cfg, i, metrics)
		} else {
			go runClient(&wg, clients[i], cfg, i, metrics)
		}
	}

	wg.Wait()
	close(done)

	putsOK := atomic.LoadInt64(&metrics.PutsOK)
	getsOK := atomic.LoadInt64(&metrics.GetsOK)
	deletesOK := atomic.LoadInt64(&metrics.DeleteOK)
	failures := atomic.LoadInt64(&metrics.Failures)
	totalOps := putsOK + getsOK + deletesOK
	elapsed := time.Since(start)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("LOAD TEST SUMMARY")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Duration:          %s\n", elapsed)
	fmt.Printf("Clients:           %d\n", cfg.Clients)
	fmt.Printf("Requests/client:   %d\n", cfg.Requests)
	fmt.Printf("Value size:        %d bytes\n", cfg.ValueSize)
	fmt.Printf("Total Ops:         %d (PUTs: %d | GETs: %d | DELs: %d)\n", totalOps, putsOK, getsOK, deletesOK)
	fmt.Printf("Failures:          %d\n", failures)
	if totalOps+failures > 0 {
		fmt.Printf("Success Rate:      %.2f%%\n", float64(totalOps)*100/float64(totalOps+failures))
	}
	fmt.Printf("Throughput:        %.2f ops/sec\n", float64(totalOps)/elapsed.Seconds())
	if totalOps > 0 {
		fmt.Printf("Avg Latency:       %.2f ms\n", elapsed.Seconds()*1000/float64(totalOps))
	}
	fmt.Println(strings.Repeat("=", 80))
}
