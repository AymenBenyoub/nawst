package core

import (
	"fmt"
	"os"
	"runtime"
	
	"testing"
	"time"
	"crypto/rand"
)

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("failed to generate random bytes: %v", err))
	}
	return b
}

// Helper to create a WAL with small batch size for benchmarks to avoid blocking
func newTestWal(path string, ack AckMode) *Wal {
	w, err := NewWal(path, 4096, ack)
	if err != nil {
		panic(err)
	}
	return w
}

func TestWalAppendNoHang(t *testing.T) {
	path := "test_wal_nohang.wal"
	defer os.Remove(path)

	w := newTestWal(path, AckAfterFlush)
	defer w.Close()

	key := string(randomBytes(32))
	val := randomBytes(128)

	done, err := w.Append(Command{Op: OpPut, Key: key, Value: val})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("append did not complete in time")
	}
}

func BenchmarkWALThroughputSafe(b *testing.B) {
	ackModes := []AckMode{AckAfterEnqueue, AckAfterFlush, AckAfterFsync}

	for _, mode := range ackModes {
		b.Run(fmt.Sprintf("Mode=%v", mode), func(b *testing.B) {
			path := fmt.Sprintf("bench_wal_safe_%d.wal", mode)
			defer os.Remove(path)

			w := newTestWal(path, mode)
			defer w.Close()

			key := string(randomBytes(32))
			val := randomBytes(128)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				done, err := w.Append(Command{Op: OpPut, Key: key, Value: val})
				if err != nil {
					b.Fatal(err)
				}
				if mode != AckAfterEnqueue {
					// wait for ack only if flush/fsync is required
					<-done
				}
			}
		})
	}
}

func BenchmarkWALRecoverySafe(b *testing.B) {
	path := "bench_recovery_safe.wal"
	defer os.Remove(path)

	// Pre-fill WAL
	w := newTestWal(path, AckAfterEnqueue)
	defer w.Close()

	key := string(randomBytes(32))
	val := randomBytes(128)

	const nEntries = 10000
	for i := 0; i < nEntries; i++ {
		done, _ := w.Append(Command{Op: OpPut, Key: key, Value: val})
		<-done
	}
	w.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store := NewStore()
		if err := ReplayWal(path, store.Apply); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALMemoryUsage(b *testing.B) {
	path := "bench_mem_safe.wal"
	defer os.Remove(path)

	w := newTestWal(path, AckAfterFlush)
	defer w.Close()

	key := string(randomBytes(32))
	val := randomBytes(128)

	const nEntries = 50000
	for i := 0; i < nEntries; i++ {
		done, _ := w.Append(Command{Op: OpPut, Key: key, Value: val})
		<-done
	}
	w.Close()

	store := NewStore()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)
	start := time.Now()

	if err := ReplayWal(path, store.Apply); err != nil {
		b.Fatal(err)
	}

	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)
	duration := time.Since(start)

	b.ReportMetric(float64(duration.Milliseconds()), "ms/recovery")
	b.ReportMetric(float64(memEnd.Alloc-memStart.Alloc)/1024/1024, "MB/recovery")
}