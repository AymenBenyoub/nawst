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
	WeightRTT  = 0.30
	WeightCPU  = 0.20
	WeightMEM  = 0.20
	WeightNET  = 0.20
	WeightDisk = 0.10
)

func CalculateScores(metrics []NodeMetrics) []NodeInfo {
	if len(metrics) == 0 {
		return nil
	}

	const eps = 1e-9

	var totalRTT float64
	var totalCPUCap float64
	var totalMemCap float64
	var totalNetCap float64
	var totalDiskCap float64
	for _, m := range metrics {
		rtt := m.AvgRTT
		if rtt <= 0 || math.IsNaN(rtt) || math.IsInf(rtt, 0) {
			rtt = 1.0
		}
		totalRTT += rtt

		cpuCap := float64(maxInt(m.CPUCores, 1))
		memCap := float64(maxInt(m.MemGB, 1))
		netCap := float64(maxInt(m.BandwidthMbps, 1))
		diskCap := float64(maxInt(m.DiskGB, 1))

		totalCPUCap += cpuCap
		totalMemCap += memCap
		totalNetCap += netCap
		totalDiskCap += diskCap
	}

	clusterAvgRTT := totalRTT / float64(len(metrics))
	if clusterAvgRTT <= 0 || math.IsNaN(clusterAvgRTT) || math.IsInf(clusterAvgRTT, 0) {
		clusterAvgRTT = 1.0
	}

	avgCPUCap := totalCPUCap / float64(len(metrics))
	avgMemCap := totalMemCap / float64(len(metrics))
	avgNetCap := totalNetCap / float64(len(metrics))
	avgDiskCap := totalDiskCap / float64(len(metrics))

	if avgCPUCap <= 0 || math.IsNaN(avgCPUCap) || math.IsInf(avgCPUCap, 0) {
		avgCPUCap = 1.0
	}
	if avgMemCap <= 0 || math.IsNaN(avgMemCap) || math.IsInf(avgMemCap, 0) {
		avgMemCap = 1.0
	}
	if avgNetCap <= 0 || math.IsNaN(avgNetCap) || math.IsInf(avgNetCap, 0) {
		avgNetCap = 1.0
	}
	if avgDiskCap <= 0 || math.IsNaN(avgDiskCap) || math.IsInf(avgDiskCap, 0) {
		avgDiskCap = 1.0
	}

	scores := make([]NodeInfo, len(metrics))
	for i, m := range metrics {
		scores[i].ID = m.NodeID

		rtt := m.AvgRTT
		if rtt <= 0 || math.IsNaN(rtt) || math.IsInf(rtt, 0) {
			rtt = clusterAvgRTT
		}

		cpuUsage := clampMetric(m.CPUUsage)
		memUsage := clampMetric(m.MemUsage)
		netUsage := clampMetric(m.NetUsage)
		diskUsage := clampMetric(m.DiskUsage)

		rsCPU := (float64(maxInt(m.CPUCores, 1)) / avgCPUCap)
		rsMEM := (float64(maxInt(m.MemGB, 1)) / avgMemCap)
		rsNET := (float64(maxInt(m.BandwidthMbps, 1)) / avgNetCap)
		rsDisk := (float64(maxInt(m.DiskGB, 1)) / avgDiskCap)

		if rsCPU <= 0 || math.IsNaN(rsCPU) || math.IsInf(rsCPU, 0) {
			rsCPU = 1.0
		}
		if rsMEM <= 0 || math.IsNaN(rsMEM) || math.IsInf(rsMEM, 0) {
			rsMEM = 1.0
		}
		if rsNET <= 0 || math.IsNaN(rsNET) || math.IsInf(rsNET, 0) {
			rsNET = 1.0
		}
		if rsDisk <= 0 || math.IsNaN(rsDisk) || math.IsInf(rsDisk, 0) {
			rsDisk = 1.0
		}

		diskPenalty := 1.0 / (1.01 - math.Min(diskUsage, 0.99))

		scores[i].Score =
			WeightRTT*(rtt/(clusterAvgRTT+eps)) +
				WeightCPU*(cpuUsage/rsCPU) +
				WeightMEM*(memUsage/rsMEM) +
				WeightNET*(netUsage/rsNET) +
				WeightDisk*(diskPenalty/rsDisk)
	}
	return scores
}

func maxInt(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}

func clampMetric(v float64) float64 {
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
