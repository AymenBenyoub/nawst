package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"math"
	stdnet "net"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	gnet "github.com/shirou/gopsutil/v4/net"
)

// MetricsCollector gathers local node metrics for gossip reporting.
type MetricsCollector struct {
	nodeID        string
	cpuCores      int
	memGB         int
	bandwidthMbps int
	diskGB        int
	diskPath      string

	mu            sync.RWMutex
	lastRTT       map[string]float64 // peer -> latency in ms
	rttCount      map[string]int     // peer -> sample count for averaging
	lastNetSample *netSample
}

type netSample struct {
	rxBytes uint64
	txBytes uint64
	at      time.Time
}

func NewMetricsCollector(nodeID string, bandwidthMbps int, diskPath string) *MetricsCollector {
	if bandwidthMbps <= 0 {
		bandwidthMbps = 100
	}
	if diskPath == "" {
		diskPath = "/"
	}

	staticCores := detectCPUCores()
	staticMemGB := detectMemGB()
	staticDiskGB := detectDiskGB(diskPath)

	return &MetricsCollector{
		nodeID:        nodeID,
		cpuCores:      staticCores,
		memGB:         staticMemGB,
		bandwidthMbps: bandwidthMbps,
		diskGB:        staticDiskGB,
		diskPath:      diskPath,
		lastRTT:       make(map[string]float64),
		rttCount:      make(map[string]int),
	}
}

func detectCPUCores() int {
	c, err := cpu.Counts(false)
	if err != nil || c <= 0 {
		return 1
	}
	return c
}

func detectMemGB() int {
	v, err := mem.VirtualMemory()
	if err != nil || v == nil || v.Total == 0 {
		return 1
	}
	gb := int(v.Total / (1024 * 1024 * 1024))
	if gb <= 0 {
		return 1
	}
	return gb
}

func detectDiskGB(path string) int {
	u, err := disk.Usage(path)
	if err != nil || u == nil || u.Total == 0 {
		return 1
	}
	gb := int(u.Total / (1024 * 1024 * 1024))
	if gb <= 0 {
		return 1
	}
	return gb
}

// RecordRTT records a latency sample to a peer (in milliseconds).
func (mc *MetricsCollector) RecordRTT(peerID string, latencyMS float64) {
	if peerID == "" || latencyMS <= 0 || math.IsNaN(latencyMS) || math.IsInf(latencyMS, 0) {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Exponential moving average: new_avg = 0.8*old_avg + 0.2*new_sample
	if old, exists := mc.lastRTT[peerID]; exists {
		mc.lastRTT[peerID] = 0.8*old + 0.2*latencyMS
	} else {
		mc.lastRTT[peerID] = latencyMS
	}
	mc.rttCount[peerID]++
}

func (mc *MetricsCollector) SnapshotRTT() map[string]float64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	out := make(map[string]float64, len(mc.lastRTT))
	maps.Copy(out, mc.lastRTT)
	return out
}

func (mc *MetricsCollector) ProbePeerRTT(peerID, rpcAddr string, timeout time.Duration) {
	if peerID == "" || rpcAddr == "" {
		return
	}
	start := time.Now()
	conn, err := stdnet.DialTimeout("tcp", rpcAddr, timeout)
	if err != nil {
		return
	}
	_ = conn.Close()
	mc.RecordRTT(peerID, float64(time.Since(start).Milliseconds()))
}

// GetCurrentMetrics returns the current node's metrics snapshot.
func (mc *MetricsCollector) GetCurrentMetrics() NodeMetrics {
	// Calculate average RTT across all peers.
	var avgRTT float64 = 1.0
	rtt := mc.SnapshotRTT()
	if len(rtt) > 0 {
		var sum float64
		for _, v := range rtt {
			sum += v
		}
		avgRTT = sum / float64(len(rtt))
		if avgRTT <= 0 {
			avgRTT = 1.0
		}
	}

	cpuUsage := readCPUUsage()
	memUsage := readMemUsage()
	diskUsage := readDiskUsage(mc.diskPath)
	netUsage := mc.readNetUsage()

	m := NodeMetrics{
		NodeID:        mc.nodeID,
		AvgRTT:        avgRTT,
		CPUCores:      mc.cpuCores,
		MemGB:         mc.memGB,
		BandwidthMbps: mc.bandwidthMbps,
		DiskGB:        mc.diskGB,
		CPUUsage:      cpuUsage,
		MemUsage:      memUsage,
		NetUsage:      netUsage,
		DiskUsage:     diskUsage,
	}

	return m
}

func readCPUUsage() float64 {
	p, err := cpu.Percent(200*time.Millisecond, false)
	if err != nil || len(p) == 0 {
		return 0
	}
	v := p[0] / 100.0
	return clamp01(v)
}

func readMemUsage() float64 {
	v, err := mem.VirtualMemory()
	if err != nil || v == nil {
		return 0
	}
	return clamp01(v.UsedPercent / 100.0)
}

func readDiskUsage(path string) float64 {
	u, err := disk.Usage(path)
	if err != nil || u == nil {
		return 0
	}
	return clamp01(u.UsedPercent / 100.0)
}

func (mc *MetricsCollector) readNetUsage() float64 {
	counters, err := gnet.IOCounters(false)
	if err != nil || len(counters) == 0 {
		return 0
	}
	now := time.Now()
	cur := counters[0]

	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.lastNetSample == nil {
		mc.lastNetSample = &netSample{rxBytes: cur.BytesRecv, txBytes: cur.BytesSent, at: now}
		return 0
	}

	dt := now.Sub(mc.lastNetSample.at).Seconds()
	if dt <= 0 {
		return 0
	}

	deltaRx := diffUint64(cur.BytesRecv, mc.lastNetSample.rxBytes)
	deltaTx := diffUint64(cur.BytesSent, mc.lastNetSample.txBytes)
	bitsPerSecond := float64(deltaRx+deltaTx) * 8.0 / dt

	mc.lastNetSample = &netSample{rxBytes: cur.BytesRecv, txBytes: cur.BytesSent, at: now}

	capacity := float64(mc.bandwidthMbps) * 1_000_000.0
	if capacity <= 0 {
		return 0
	}
	return clamp01(bitsPerSecond / capacity)
}

func diffUint64(cur, prev uint64) uint64 {
	if cur >= prev {
		return cur - prev
	}
	return 0
}

func clamp01(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// GossipMetricsMessage is the payload sent via memberlist gossip.
type GossipMetricsMessage struct {
	Type    string             `json:"type"`
	NodeID  string             `json:"node_id"`
	Metrics NodeMetrics        `json:"metrics"`
	RTTData map[string]float64 `json:"rtt_data"` // peer -> avg RTT
	Time    int64              `json:"timestamp"`
}

// EncodeGossipMetricsMessage creates a gossip payload.
func EncodeGossipMetricsMessage(metrics NodeMetrics, rttData map[string]float64) ([]byte, error) {
	msg := GossipMetricsMessage{
		Type:    "metrics",
		NodeID:  metrics.NodeID,
		Metrics: metrics,
		RTTData: rttData,
		Time:    time.Now().UnixMilli(),
	}
	return json.Marshal(msg)
}

// DecodeGossipMetricsMessage parses a gossip payload.
func DecodeGossipMetricsMessage(data []byte) (*GossipMetricsMessage, error) {
	var msg GossipMetricsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if msg.Type != "metrics" {
		return nil, fmt.Errorf("unknown gossip message type: %s", msg.Type)
	}
	return &msg, nil
}

// MemStatsSnapshot captures Go runtime memory stats.
func MemStatsSnapshot() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

// LogResourceUsage logs current resource usage for debugging.
func LogResourceUsage(nodeID string, metrics NodeMetrics) {
	log.Printf("[metrics] %s: RTT=%.1fms CPU=%d cores MemGB=%d BW=%d Mbps Disk=%d GB | CPU=%.1f%% Mem=%.1f%% Net=%.1f%% Disk=%.1f%%",
		nodeID,
		metrics.AvgRTT,
		metrics.CPUCores,
		metrics.MemGB,
		metrics.BandwidthMbps,
		metrics.DiskGB,
		metrics.CPUUsage*100,
		metrics.MemUsage*100,
		metrics.NetUsage*100,
		metrics.DiskUsage*100,
	)
}

func DetectDiskPath(path string) string {
	if path == "" {
		return "/"
	}
	if filepath.IsAbs(path) {
		return path
	}
	return "/"
}
