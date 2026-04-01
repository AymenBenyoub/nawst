# Context Notes (Verified)

This file is a corrected technical context snapshot for the current `cluster` branch.
It replaces earlier draft notes that mixed fixed and unfixed items.

## Current Design (As Implemented)

### Metrics and placement trigger model
- Metrics are collected and gossiped in `Replicator.publishLocalMetrics()`.
- Incoming gossip updates are merged in `Replicator.HandleMetricsMessage()`.
- Placement recomputation is NOT triggered per-message anymore.
- Placement recomputation currently runs from `StartMetricsReporter()` via a periodic `evalTicker` (`3 * interval`) and calls `UpdatePlacement()` only on leader.

### Hysteresis in placement updates
- `UpdatePlacement()` computes a proposed placement, then compares against the previous one.
- It measures drift using two counters:
  - primary changes (`movedPrimaryCount`)
  - replica-list changes (`movedReplicaCount`)
- It skips commit if BOTH are below threshold:
  - `primaryThreshold = 5% of VNodeCount`
  - `replicaThreshold = 20% of (VNodeCount * (rf-1))`
- Epoch increment is controlled in `replication.go` (not in `placement.go`).

### Raft membership reconciliation
- Reconcile logic is in `Replicator.ReconcileRaftWithMembership()`.
- New nodes seen in gossip are added to raft immediately via `AddVoter()`.
- Missing nodes are put on `deadRow` with first-missing timestamp.
- Nodes are removed from raft only after grace period expires.
- `deadRow` map is initialized in `NewReplicator()` (no nil-map panic now).
- `raft.Nonvoter` is the correct constant name in current hashicorp/raft API.

## What Was Wrong In Previous Draft Notes

The following previous statements are now stale and should be considered resolved:
- "deadRow map is nil" -> fixed.
- "hysteresis OR bug" -> now uses AND comparison.
- "placement logs from placement.go cause false commit logs" -> logging moved to commit path in `replication.go`.

## Resolved In Current Branch

### 1) Placement schedulers are explicit and stopped cleanly
- `StartMetricsReporter()` now runs:
  - publish ticker
  - short-term eval ticker
  - long-term eval ticker
- All tickers are stopped with deferred `Stop()` calls in the goroutine.

### 2) Metadata parsing policy is now strict
- `parseMeta()` enforces comma-delimited metadata only (`nodeID,rpcAddr[,raftAddr]`).
- Colon fallback was removed to avoid ambiguous parsing.

### 3) Ownership comparison is strict
- `CheckOwnership()` now uses exact ID comparison (`owner == r.ID`).

### 4) Membership reconcile loop is debounced
- `cluster/membership.go` coalesces bursty membership events with a debounce timer before calling reconcile.

### 5) Stream drain no longer busy-waits
- `core/rpc.go` `StreamKV` now uses `sync.WaitGroup` and channels for drain coordination.
- The previous atomic+sleep polling loop was removed.

### 6) Replication path now uses cached node RPC addresses
- `Replicator` maintains `nodeRPCAddrs` (nodeID -> rpcAddr).
- Cache is refreshed during metrics publish and raft membership reconcile.
- `getOrCreateClientByNodeID()` now tries:
  1) existing client map
  2) cached rpc address
  3) memberlist scan fallback

### 7) Empty placement target no longer hard-fails immediately
- `ReplicateToAll()` now falls back to all non-self members if placement yields no remote targets.

## Current Optional Improvements

1. Reconcile-triggered long-term placement
- `ReconcileRaftWithMembership()` still calls `UpdatePlacement()` at the end.
- This is valid, but if too noisy in large clusters, it can be rate-limited further.

2. Metadata strictness rollout
- Strict comma parsing is now active; older nodes that still emit legacy metadata formats will fail fast.
- Keep startup docs aligned so mixed-version clusters are avoided.

## Optional Architecture Upgrade (Already Partially Adopted)

Short-term and long-term evaluation split is present:
- short-term: role swaps (fast cadence)
- long-term: full primary+replica reassignment (slow cadence)

Tune cadence/thresholds per workload if needed.

## Operational Reminder

Many observed "raft membership broken" incidents were startup flag issues, not reconcile logic bugs.
Validate these per node:
- `-raft-bind-addr`
- `-raft-advertise-ip`
- `-gossip-bind-addr`
- `-gossip-advertise-ip`
- seed gossip address for non-seed nodes
