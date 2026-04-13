package observability

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	placementEpochCurrent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_placement_epoch_current",
		Help: "Current applied placement epoch on this node.",
	})

	placementApplyTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_placement_apply_total",
		Help: "Total number of placement applications.",
	})

	placementApplyDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nawst_placement_apply_duration_seconds",
		Help:    "Duration of placement application/classification logic.",
		Buckets: prometheus.DefBuckets,
	})

	migrationVNodesGainedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_migration_vnodes_gained_total",
		Help: "Total vnodes gained as primary across placements.",
	})

	migrationVNodesLostTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_migration_vnodes_lost_total",
		Help: "Total vnodes lost as primary across placements.",
	})

	migrationVNodesReplicaAddedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_migration_vnodes_replica_added_total",
		Help: "Total vnodes where this node was newly added as replica.",
	})

	migrationVNodesReplicaChangedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_migration_vnodes_replica_changed_total",
		Help: "Total vnodes where replica set changed while this node remained primary.",
	})

	migrationActiveVNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_migration_active_vnodes",
		Help: "Number of vnode migrations currently in progress on this node.",
	})

	transferAttemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nawst_transfer_attempts_total",
		Help: "Total vnode transfer attempts by result.",
	}, []string{"result"})

	transferDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nawst_transfer_duration_seconds",
		Help:    "Duration of vnode transfer attempts.",
		Buckets: prometheus.DefBuckets,
	})

	transferEntriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_transfer_entries_total",
		Help: "Total entries received during vnode transfers.",
	})

	transferBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_transfer_bytes_total",
		Help: "Total key/value bytes received during vnode transfers.",
	})

	getMigrationFallbackTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_get_migration_fallback_total",
		Help: "Total GET requests served via migration-source fallback.",
	})

	staleWriteIgnoredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nawst_stale_write_ignored_total",
		Help: "Total stale writes ignored by version checks.",
	})

	storeApplyTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nawst_store_apply_total",
		Help: "Store apply operations by op and result.",
	}, []string{"op", "result"})

	storeGetTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nawst_store_get_total",
		Help: "Store get outcomes.",
	}, []string{"result"})

	storeGetDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nawst_store_get_duration_seconds",
		Help:    "Store get latency.",
		Buckets: prometheus.DefBuckets,
	})

	storeSnapshotEntries = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nawst_store_snapshot_entries",
		Help:    "Snapshot entries per vnode snapshot.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
	})

	storeKeys = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_store_keys",
		Help: "Current number of active keys in store.",
	})

	storeTombstones = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_store_tombstones",
		Help: "Current number of tombstoned keys.",
	})

	storeValueBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_store_value_bytes",
		Help: "Approximate bytes held by active values in store.",
	})

	storeVnodesWithData = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_store_vnodes_with_data",
		Help: "Number of vnodes with at least one active key.",
	})

	storeVnodesWithTombstones = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_store_vnodes_with_tombstones",
		Help: "Number of vnodes with tombstones.",
	})

	rpcRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nawst_rpc_requests_total",
		Help: "RPC requests by method and result.",
	}, []string{"method", "result"})

	rpcDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nawst_rpc_request_duration_seconds",
		Help:    "RPC method latency by method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	replicationQuorumTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nawst_replication_quorum_total",
		Help: "Replication quorum outcomes.",
	}, []string{"result"})

	replicationAcks = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nawst_replication_acks",
		Help:    "Number of replication acknowledgments observed per quorum attempt.",
		Buckets: []float64{1, 2, 3, 4, 5, 7, 9},
	})

	replicationTargets = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nawst_replication_targets",
		Help:    "Number of remote replication targets selected per request.",
		Buckets: []float64{0, 1, 2, 3, 4, 5, 7, 9},
	})

	localNodeCPUUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_cpu_usage_ratio",
		Help: "Local CPU usage ratio [0..1] from capacity collector.",
	})
	localNodeMemUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_mem_usage_ratio",
		Help: "Local memory usage ratio [0..1] from capacity collector.",
	})
	localNodeNetUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_net_usage_ratio",
		Help: "Local network usage ratio [0..1] versus configured bandwidth.",
	})
	localNodeDiskUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_disk_usage_ratio",
		Help: "Local disk usage ratio [0..1] from capacity collector.",
	})
	localNodeAvgRTTMs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_avg_rtt_ms",
		Help: "Local average RTT in ms.",
	})
	localNodeCPUCores = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_cpu_cores",
		Help: "Static local CPU cores.",
	})
	localNodeMemGB = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_mem_gb",
		Help: "Static local memory capacity in GB.",
	})
	localNodeBandwidthMbps = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_bandwidth_mbps",
		Help: "Configured local bandwidth capacity in Mbps.",
	})
	localNodeDiskGB = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_local_disk_gb",
		Help: "Static local disk capacity in GB.",
	})

	clusterMembers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nawst_cluster_members",
		Help: "Current memberlist member count observed by this node.",
	})
)

func StartMetricsServer(bindAddr string, port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf("%s:%d", bindAddr, port)
	go func() {
		log.Printf("[obs] metrics endpoint listening on %s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("[obs] metrics endpoint stopped: %v", err)
		}
	}()
}

func ObservePlacementApply(epoch uint64, gained, lost, changedReplicas, addedReplicas int, d time.Duration) {
	placementEpochCurrent.Set(float64(epoch))
	placementApplyTotal.Inc()
	placementApplyDurationSeconds.Observe(d.Seconds())
	migrationVNodesGainedTotal.Add(float64(gained))
	migrationVNodesLostTotal.Add(float64(lost))
	migrationVNodesReplicaChangedTotal.Add(float64(changedReplicas))
	migrationVNodesReplicaAddedTotal.Add(float64(addedReplicas))
}

func IncMigrationActive() {
	migrationActiveVNodes.Inc()
}

func DecMigrationActive() {
	migrationActiveVNodes.Dec()
}

func ObserveTransfer(result string, d time.Duration, entries int, bytes int) {
	if result == "" {
		result = "unknown"
	}
	transferAttemptsTotal.WithLabelValues(result).Inc()
	transferDurationSeconds.Observe(d.Seconds())
	if entries > 0 {
		transferEntriesTotal.Add(float64(entries))
	}
	if bytes > 0 {
		transferBytesTotal.Add(float64(bytes))
	}
}

func IncGetMigrationFallback() {
	getMigrationFallbackTotal.Inc()
}

func IncStaleWriteIgnored() {
	staleWriteIgnoredTotal.Inc()
}

func ObserveStoreApply(op, result string) {
	if op == "" {
		op = "unknown"
	}
	if result == "" {
		result = "unknown"
	}
	storeApplyTotal.WithLabelValues(op, result).Inc()
}

func ObserveStoreGet(result string, d time.Duration) {
	if result == "" {
		result = "unknown"
	}
	storeGetTotal.WithLabelValues(result).Inc()
	storeGetDurationSeconds.Observe(d.Seconds())
}

func ObserveStoreSnapshot(entries int) {
	if entries >= 0 {
		storeSnapshotEntries.Observe(float64(entries))
	}
}

func SetStoreState(keys, tombstones int, valueBytes int64, vnodesWithData, vnodesWithTombstones int) {
	storeKeys.Set(float64(keys))
	storeTombstones.Set(float64(tombstones))
	storeValueBytes.Set(float64(valueBytes))
	storeVnodesWithData.Set(float64(vnodesWithData))
	storeVnodesWithTombstones.Set(float64(vnodesWithTombstones))
}

func ObserveRPC(method, result string, d time.Duration) {
	if method == "" {
		method = "unknown"
	}
	if result == "" {
		result = "unknown"
	}
	rpcRequestsTotal.WithLabelValues(method, result).Inc()
	rpcDurationSeconds.WithLabelValues(method).Observe(d.Seconds())
}

func ObserveReplicationQuorum(result string, acks, targets int) {
	if result == "" {
		result = "unknown"
	}
	replicationQuorumTotal.WithLabelValues(result).Inc()
	replicationAcks.Observe(float64(acks))
	replicationTargets.Observe(float64(targets))
}

func ObserveLocalCapacity(cpuUsage, memUsage, netUsage, diskUsage, avgRTT float64, cpuCores, memGB, bandwidthMbps, diskGB int) {
	localNodeCPUUsage.Set(cpuUsage)
	localNodeMemUsage.Set(memUsage)
	localNodeNetUsage.Set(netUsage)
	localNodeDiskUsage.Set(diskUsage)
	localNodeAvgRTTMs.Set(avgRTT)
	localNodeCPUCores.Set(float64(cpuCores))
	localNodeMemGB.Set(float64(memGB))
	localNodeBandwidthMbps.Set(float64(bandwidthMbps))
	localNodeDiskGB.Set(float64(diskGB))
}

func SetClusterMembers(n int) {
	if n < 0 {
		n = 0
	}
	clusterMembers.Set(float64(n))
}
