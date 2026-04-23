package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AymenBenyoub/nawst/cluster"
)

type LoadConfig struct {
	Addr            string
	Clients         int
	Conns           int
	Timeout         time.Duration
	ValueSize       int
	Keyspace        int
	PrefillKeys     int
	Warmup          time.Duration
	Duration        time.Duration
	ProgressEvery   time.Duration
	Distribution    string
	ZipfS           float64
	ZipfV           float64
	ReadPct         int
	WritePct        int
	DeletePct       int
	TargetOpsPerSec int
	Seed            int64
	PutOnly         bool
	UseStream       bool
}

type operation int

const (
	opPut operation = iota
	opGet
	opDelete
)

func (o operation) String() string {
	switch o {
	case opPut:
		return "PUT"
	case opGet:
		return "GET"
	default:
		return "DELETE"
	}
}

type opMix struct {
	putEnd int
	getEnd int
}

func (m opMix) pick(r *rand.Rand) operation {
	v := r.Intn(100)
	if v < m.putEnd {
		return opPut
	}
	if v < m.getEnd {
		return opGet
	}
	return opDelete
}

type keyPicker interface {
	Next(*rand.Rand) int
}

type uniformPicker struct {
	keyspace int
}

func (p *uniformPicker) Next(r *rand.Rand) int {
	return r.Intn(p.keyspace)
}

type latestPicker struct {
	keyspace int
}

func (p *latestPicker) Next(r *rand.Rand) int {
	if p.keyspace <= 1 {
		return 0
	}
	// Heavy bias toward recent keys.
	v := math.Pow(r.Float64(), 4.0)
	idx := int((1.0 - v) * float64(p.keyspace-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= p.keyspace {
		idx = p.keyspace - 1
	}
	return idx
}

type zipfPicker struct {
	zipf *rand.Zipf
	max  uint64
}

func (p *zipfPicker) Next(_ *rand.Rand) int {
	return int(p.zipf.Uint64() % (p.max + 1))
}

type opStats struct {
	ok          uint64
	fail        uint64
	latencySum  float64
	latencyHist []uint64
}

type recorder struct {
	mu      sync.Mutex
	bounds  []float64
	perOp   map[operation]*opStats
	started time.Time
}

func newRecorder() *recorder {
	bounds := []float64{0.25, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000}
	mk := func() *opStats {
		return &opStats{latencyHist: make([]uint64, len(bounds)+1)}
	}
	return &recorder{
		bounds: bounds,
		perOp: map[operation]*opStats{
			opPut:    mk(),
			opGet:    mk(),
			opDelete: mk(),
		},
		started: time.Now(),
	}
}

func (r *recorder) observe(op operation, d time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := r.perOp[op]
	if err != nil {
		s.fail++
		return
	}
	s.ok++
	ms := float64(d.Microseconds()) / 1000.0
	s.latencySum += ms

	bucket := len(r.bounds)
	for i := range r.bounds {
		if ms <= r.bounds[i] {
			bucket = i
			break
		}
	}
	s.latencyHist[bucket]++
}

type snapshotOp struct {
	ok         uint64
	fail       uint64
	latencySum float64
	hist       []uint64
}

type snapshot struct {
	elapsed time.Duration
	perOp   map[operation]snapshotOp
}

func (r *recorder) snapshot() snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := snapshot{
		elapsed: time.Since(r.started),
		perOp:   make(map[operation]snapshotOp, len(r.perOp)),
	}
	for op, s := range r.perOp {
		h := make([]uint64, len(s.latencyHist))
		copy(h, s.latencyHist)
		out.perOp[op] = snapshotOp{ok: s.ok, fail: s.fail, latencySum: s.latencySum, hist: h}
	}
	return out
}

func (s snapshot) totalOK() uint64 {
	return s.perOp[opPut].ok + s.perOp[opGet].ok + s.perOp[opDelete].ok
}

func (s snapshot) totalFail() uint64 {
	return s.perOp[opPut].fail + s.perOp[opGet].fail + s.perOp[opDelete].fail
}

func randomValue(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Intn(26) + 97)
	}
	return b
}

func pickKey(cfg LoadConfig, kp keyPicker, r *rand.Rand) string {
	return fmt.Sprintf("k%08d", kp.Next(r)%cfg.Keyspace)
}

func doOp(ctx context.Context, router *cluster.PlacementRouter, op operation, key string, value []byte) error {
	switch op {
	case opPut:
		return router.Put(ctx, key, value)
	case opGet:
		_, err := router.Get(ctx, key)
		return err
	default:
		return router.Delete(ctx, key)
	}
}

func qFromHist(hist []uint64, bounds []float64, q float64) float64 {
	var total uint64
	for _, c := range hist {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(total) * q))
	if target == 0 {
		target = 1
	}
	var cumulative uint64
	for i, c := range hist {
		cumulative += c
		if cumulative >= target {
			if i >= len(bounds) {
				return bounds[len(bounds)-1]
			}
			return bounds[i]
		}
	}
	return bounds[len(bounds)-1]
}

func printReport(title string, snap snapshot, bounds []float64) {
	totalOK := snap.totalOK()
	totalFail := snap.totalFail()
	total := totalOK + totalFail
	elapsedSec := snap.elapsed.Seconds()
	if elapsedSec <= 0 {
		elapsedSec = 1
	}

	fmt.Println("\n" + strings.Repeat("=", 92))
	fmt.Println(title)
	fmt.Println(strings.Repeat("=", 92))
	fmt.Printf("Elapsed:            %s\n", snap.elapsed)
	fmt.Printf("Total Success:      %d\n", totalOK)
	fmt.Printf("Total Failures:     %d\n", totalFail)
	if total > 0 {
		fmt.Printf("Success Rate:       %.2f%%\n", 100.0*float64(totalOK)/float64(total))
	}
	fmt.Printf("Throughput:         %.2f ops/s\n", float64(totalOK)/elapsedSec)

	for _, op := range []operation{opPut, opGet, opDelete} {
		s := snap.perOp[op]
		opTotal := s.ok + s.fail
		if opTotal == 0 {
			continue
		}
		avg := 0.0
		if s.ok > 0 {
			avg = s.latencySum / float64(s.ok)
		}
		p50 := qFromHist(s.hist, bounds, 0.50)
		p95 := qFromHist(s.hist, bounds, 0.95)
		p99 := qFromHist(s.hist, bounds, 0.99)
		fmt.Printf("%s: ok=%d fail=%d avg=%.2fms p50~%.2fms p95~%.2fms p99~%.2fms\n",
			op.String(), s.ok, s.fail, avg, p50, p95, p99)
	}
	fmt.Println(strings.Repeat("=", 92))
}

func buildMix(cfg LoadConfig) (opMix, error) {
	if cfg.PutOnly {
		return opMix{putEnd: 100, getEnd: 100}, nil
	}
	if cfg.ReadPct < 0 || cfg.WritePct < 0 || cfg.DeletePct < 0 {
		return opMix{}, fmt.Errorf("percentages must be >= 0")
	}
	if cfg.ReadPct+cfg.WritePct+cfg.DeletePct != 100 {
		return opMix{}, fmt.Errorf("read+write+delete must equal 100")
	}
	return opMix{putEnd: cfg.WritePct, getEnd: cfg.WritePct + cfg.ReadPct}, nil
}

func buildPicker(cfg LoadConfig, r *rand.Rand) (keyPicker, error) {
	switch strings.ToLower(cfg.Distribution) {
	case "uniform":
		return &uniformPicker{keyspace: cfg.Keyspace}, nil
	case "latest":
		return &latestPicker{keyspace: cfg.Keyspace}, nil
	case "zipf":
		if cfg.Keyspace <= 1 {
			return &uniformPicker{keyspace: cfg.Keyspace}, nil
		}
		z := rand.NewZipf(r, cfg.ZipfS, cfg.ZipfV, uint64(cfg.Keyspace-1))
		if z == nil {
			return nil, fmt.Errorf("invalid zipf params: s=%.4f v=%.4f", cfg.ZipfS, cfg.ZipfV)
		}
		return &zipfPicker{zipf: z, max: uint64(cfg.Keyspace - 1)}, nil
	default:
		return nil, fmt.Errorf("unknown distribution %q (use uniform|zipf|latest)", cfg.Distribution)
	}
}

func prefill(cfg LoadConfig, router *cluster.PlacementRouter) {
	if cfg.PrefillKeys <= 0 {
		return
	}
	start := time.Now()
	var okCount uint64
	var failCount uint64
	workers := cfg.Clients
	if workers > cfg.PrefillKeys {
		workers = cfg.PrefillKeys
	}
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for wid := 0; wid < workers; wid++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(cfg.Seed + int64(id)*7919 + 13))
			v := randomValue(r, cfg.ValueSize)
			for k := id; k < cfg.PrefillKeys; k += workers {
				ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
				err := router.Put(ctx, fmt.Sprintf("k%08d", k%cfg.Keyspace), v)
				cancel()
				if err != nil {
					atomic.AddUint64(&failCount, 1)
					continue
				}
				atomic.AddUint64(&okCount, 1)
			}
		}(wid)
	}
	wg.Wait()
	elapsed := time.Since(start)
	ops := float64(atomic.LoadUint64(&okCount)) / max(elapsed.Seconds(), 1e-9)
	fmt.Printf("[prefill] requested=%d ok=%d fail=%d elapsed=%s throughput=%.2f ops/s\n",
		cfg.PrefillKeys,
		atomic.LoadUint64(&okCount),
		atomic.LoadUint64(&failCount),
		elapsed,
		ops,
	)
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func runPhase(name string, cfg LoadConfig, router *cluster.PlacementRouter, measure bool) *recorder {
	rec := newRecorder()
	mix, err := buildMix(cfg)
	if err != nil {
		log.Fatalf("invalid op mix: %v", err)
	}

	stopAt := time.Now().Add(cfg.Duration)
	perWorkerPace := time.Duration(0)
	if cfg.TargetOpsPerSec > 0 {
		perWorker := float64(cfg.TargetOpsPerSec) / float64(cfg.Clients)
		if perWorker > 0 {
			perWorkerPace = time.Duration(float64(time.Second) / perWorker)
		}
	}

	var wg sync.WaitGroup
	for wid := 0; wid < cfg.Clients; wid++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(cfg.Seed + int64(id)*104729 + int64(time.Now().UnixNano())))
			picker, err := buildPicker(cfg, r)
			if err != nil {
				log.Printf("worker %d picker error: %v", id, err)
				return
			}
			value := randomValue(r, cfg.ValueSize)

			for time.Now().Before(stopAt) {
				op := mix.pick(r)
				if cfg.UseStream {
					op = opPut
				}
				key := pickKey(cfg, picker, r)

				started := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
				err := doOp(ctx, router, op, key, value)
				cancel()

				if measure {
					rec.observe(op, time.Since(started), err)
				}

				if perWorkerPace > 0 {
					sleep := perWorkerPace - time.Since(started)
					if sleep > 0 {
						time.Sleep(sleep)
					}
				}
			}
		}(wid)
	}

	progressTicker := time.NewTicker(cfg.ProgressEvery)
	defer progressTicker.Stop()
	phaseStart := time.Now()

	for time.Now().Before(stopAt) {
		<-progressTicker.C
		if !measure {
			fmt.Printf("[%s] running... elapsed=%s\n", name, time.Since(phaseStart).Truncate(time.Second))
			continue
		}
		s := rec.snapshot()
		throughput := float64(s.totalOK()) / max(s.elapsed.Seconds(), 1e-9)
		fmt.Printf("[%s] elapsed=%s ok=%d fail=%d throughput=%.2f ops/s\n",
			name,
			time.Since(phaseStart).Truncate(time.Second),
			s.totalOK(),
			s.totalFail(),
			throughput,
		)
	}

	wg.Wait()
	return rec
}

func parseFlags() LoadConfig {
	cfg := LoadConfig{}
	flag.StringVar(&cfg.Addr, "addr", "localhost:9999", "server host:port")
	flag.IntVar(&cfg.Clients, "clients", 128, "number of concurrent workers")
	flag.IntVar(&cfg.Conns, "conns", 32, "number of grpc connections shared by workers")
	flag.DurationVar(&cfg.Timeout, "timeout", 3*time.Second, "per-operation timeout")
	flag.IntVar(&cfg.ValueSize, "valuesize", 512, "value payload size in bytes")
	flag.IntVar(&cfg.Keyspace, "keyspace", 300000, "logical keyspace size")
	flag.IntVar(&cfg.PrefillKeys, "prefill", 120000, "initial key count to prefill before workload")
	flag.DurationVar(&cfg.Warmup, "warmup", 30*time.Second, "warmup duration")
	flag.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "measured workload duration")
	flag.DurationVar(&cfg.ProgressEvery, "progress", 5*time.Second, "progress print interval")
	flag.StringVar(&cfg.Distribution, "dist", "zipf", "key distribution: uniform|zipf|latest")
	flag.Float64Var(&cfg.ZipfS, "zipf-s", 1.10, "zipf skew parameter s (>1)")
	flag.Float64Var(&cfg.ZipfV, "zipf-v", 1.0, "zipf parameter v (>=1)")
	flag.IntVar(&cfg.ReadPct, "read-pct", 80, "read percentage")
	flag.IntVar(&cfg.WritePct, "write-pct", 15, "write percentage")
	flag.IntVar(&cfg.DeletePct, "delete-pct", 5, "delete percentage")
	flag.IntVar(&cfg.TargetOpsPerSec, "target-ops", 0, "approx global op rate cap (0 = max throughput)")
	flag.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano(), "random seed")
	flag.BoolVar(&cfg.PutOnly, "putonly", false, "legacy mode: force 100% PUT")
	flag.BoolVar(&cfg.UseStream, "stream", false, "legacy stream mode (not recommended for distributed-path measurements)")
	flag.Parse()

	if cfg.Clients <= 0 {
		cfg.Clients = 1
	}
	if cfg.Conns <= 0 {
		cfg.Conns = 1
	}
	if cfg.ValueSize <= 0 {
		cfg.ValueSize = 64
	}
	if cfg.Keyspace <= 0 {
		cfg.Keyspace = 1
	}
	if cfg.PrefillKeys < 0 {
		cfg.PrefillKeys = 0
	}
	if cfg.PrefillKeys > cfg.Keyspace {
		cfg.PrefillKeys = cfg.Keyspace
	}
	if cfg.ProgressEvery <= 0 {
		cfg.ProgressEvery = 5 * time.Second
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 30 * time.Second
	}
	if cfg.ZipfS <= 1.0 {
		cfg.ZipfS = 1.01
	}
	if cfg.ZipfV < 1.0 {
		cfg.ZipfV = 1.0
	}

	return cfg
}

func main() {
	cfg := parseFlags()

	router := cluster.NewPlacementRouter(cfg.Addr)
	refreshCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	if err := router.Refresh(refreshCtx); err != nil {
		log.Printf("placement refresh failed; workload will error until placement is available: %v", err)
	}
	cancel()
	defer router.Close()

	fmt.Println(strings.Repeat("=", 92))
	fmt.Println("NAWST REAL WORKLOAD DRIVER")
	fmt.Println(strings.Repeat("=", 92))
	fmt.Printf("addr=%s clients=%d timeout=%s value=%dB keyspace=%d prefill=%d\n",
		cfg.Addr, cfg.Clients, cfg.Timeout, cfg.ValueSize, cfg.Keyspace, cfg.PrefillKeys)
	fmt.Printf("dist=%s zipf(s=%.3f,v=%.3f) mix(read/write/delete)=%d/%d/%d target-ops=%d\n",
		cfg.Distribution, cfg.ZipfS, cfg.ZipfV, cfg.ReadPct, cfg.WritePct, cfg.DeletePct, cfg.TargetOpsPerSec)
	fmt.Printf("warmup=%s measure=%s progress=%s\n", cfg.Warmup, cfg.Duration, cfg.ProgressEvery)

	prefill(cfg, router)

	if cfg.Warmup > 0 {
		warmCfg := cfg
		warmCfg.Duration = cfg.Warmup
		_ = runPhase("warmup", warmCfg, router, false)
	}

	measured := runPhase("measured", cfg, router, true)
	printReport("MEASURED WORKLOAD SUMMARY", measured.snapshot(), measured.bounds)
}
