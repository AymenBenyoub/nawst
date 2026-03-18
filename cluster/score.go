package cluster

import "math"

// here we calculate the dynamic score of each node using the score formula:
// Score_i =
//     (w1 * (RTT_i / RTT_avg)) +
//     (w2 * (CPU_i / RS_cpu_i)) +
//     (w3 * (MEM_i / RS_mem_i)) +
//     (w4 * (NetUsage_i / RS_net_i)) +
//     (w5 * (P(Disk_i) / RS_disk_i))

// where:

// w1, w2, w3, w4, w5 are the weights for each metric, which can be tuned based on the importance of each factor in the cluster's performance.

// and RS is relative score, which is how the node's STATIC metric compares against the cluster average for that metric. (ie, not cpu usage, but cpu cores, not mem usage, but mem capacity, etc)

// P(Disk_i) = 1 / (1.01 - DiskUsage_i) is a penalty function that sharply increases as disk usage (static, not io usage) approaches 100%, to prevent assigning more load to nearly full disks.

// for now we'll only use rtt and bandwidth in the score calculation.

// as for replica assignment, we'll only use rtt AND bandwidth. (definitevely)
// // Cost(p, r) =(alpha * (RTT_p_r / RTT_links_avg)) + (beta  * (BW_links_avg / BW_p_r))

type NodeMetrics struct {
	NodeID        string
	AvgRTT        float64 // in millieseconds
	CPUCores      int
	MemGB         int
	BandwidthMbps int
	DiskGB        int
	CPUUsage      float64 // 0 - 1
	MemUsage      float64 // 0-1
	NetUsage      float64 // 0-1
	DiskUsage     float64 // 0-1
}

const (
	WeightRTT = 0.65
	WeightBW  = 0.35 //only these two for now
)

func CalculateScores(metrics []NodeMetrics) []NodeInfo {
	if len(metrics) == 0 {
		return nil
	}

	var totalRTT float64
	var totalBW float64
	for _, m := range metrics {
		rtt := m.AvgRTT
		if rtt <= 0 || math.IsNaN(rtt) || math.IsInf(rtt, 0) {
			rtt = 1.0
		}
		totalRTT += rtt

		bw := float64(m.BandwidthMbps)
		if bw <= 0 || math.IsNaN(bw) || math.IsInf(bw, 0) {
			bw = 1.0
		}
		totalBW += bw
	}
	ClusterAvgRTT := totalRTT / float64(len(metrics))
	if ClusterAvgRTT <= 0 || math.IsNaN(ClusterAvgRTT) || math.IsInf(ClusterAvgRTT, 0) {
		ClusterAvgRTT = 1.0
	}

	avgBW := totalBW / float64(len(metrics))
	if avgBW <= 0 || math.IsNaN(avgBW) || math.IsInf(avgBW, 0) {
		avgBW = 1.0
	}

	BandwidthRC := make(map[string]float64) // relative capacity for bandwidth, higher is better
	for _, m := range metrics {
		bw := float64(m.BandwidthMbps)
		if bw <= 0 || math.IsNaN(bw) || math.IsInf(bw, 0) {
			bw = 1.0
		}
		BandwidthRC[m.NodeID] = bw / avgBW
	}
	scores := make([]NodeInfo, len(metrics))
	for i, m := range metrics {
		scores[i].ID = m.NodeID

		rtt := m.AvgRTT
		if rtt <= 0 || math.IsNaN(rtt) || math.IsInf(rtt, 0) {
			rtt = ClusterAvgRTT
		}

		netUsage := m.NetUsage
		if math.IsNaN(netUsage) || math.IsInf(netUsage, 0) {
			netUsage = 0.5
		}
		if netUsage < 0 {
			netUsage = 0
		}
		if netUsage > 0.99 {
			netUsage = 0.99
		}

		netPenalty := 1.0 / (1.01 - netUsage)
		rc := BandwidthRC[m.NodeID]
		if rc <= 0 || math.IsNaN(rc) || math.IsInf(rc, 0) {
			rc = 1.0
		}

		scores[i].Score = WeightRTT*(rtt/ClusterAvgRTT) + WeightBW*(netPenalty/rc)
	}
	return scores
}
