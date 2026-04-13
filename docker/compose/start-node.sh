#!/usr/bin/env bash
set -euo pipefail

RPC_PORT="${RPC_PORT:?RPC_PORT is required}"
NODE_IP="${NODE_IP:?NODE_IP is required}"
GOSSIP_PORT="${GOSSIP_PORT:?GOSSIP_PORT is required}"
SEED_GOSSIP_ADDR="${SEED_GOSSIP_ADDR:?SEED_GOSSIP_ADDR is required}"
RAFT_BIND_ADDR="${RAFT_BIND_ADDR:-0.0.0.0}"
RAFT_ADVERTISE_IP="${RAFT_ADVERTISE_IP:-${NODE_IP}}"
BANDWIDTH_MBPS="${BANDWIDTH_MBPS:-100}"
METRICS_INTERVAL="${METRICS_INTERVAL:-4s}"
METRICS_BIND_ADDR="${METRICS_BIND_ADDR:-0.0.0.0}"
METRICS_PORT="${METRICS_PORT:-$((RPC_PORT + 2000))}"
RF="${RF:-3}"
ACK="${ACK:-1}"
ENABLE_TC="${ENABLE_TC:-0}"
DISK_PATH="${DISK_PATH:-/data}"

if [[ ! -x /workspace/kvserver ]]; then
  echo "[boot] missing executable /workspace/kvserver"
  echo "[boot] build it first with: CGO_ENABLED=0 go build -o kvserver ./cmd/server/main.go"
  exit 1
fi

# Fail fast when container runs an old host binary after source edits.
for src in /workspace/cmd/server/main.go /workspace/cluster/raft.go /workspace/cluster/replication.go /workspace/cluster/membership.go; do
  if [[ -f "${src}" && /workspace/kvserver -ot "${src}" ]]; then
    echo "[boot] stale /workspace/kvserver detected (older than ${src})"
    echo "[boot] rebuild on host: CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags='-w -s' -o kvserver ./cmd/server/main.go"
    exit 1
  fi
done

DATA_DIR="/data/node-${RPC_PORT}"
mkdir -p "${DATA_DIR}"

apply_tc_matrix() {
  local rules="${TC_RULES:-}"
  if [[ -z "${rules}" ]]; then
    echo "[tc] no TC_RULES configured for node-${RPC_PORT}; running without shaping"
    return
  fi

  tc qdisc replace dev eth0 root handle 1: prio bands 16

  local prio=2
  IFS=';' read -r -a entries <<< "${rules}"
  for entry in "${entries[@]}"; do
    [[ -z "${entry}" ]] && continue

    local dst=""
    local rate=""
    local delay=""
    IFS=',' read -r dst rate delay <<< "${entry}"

    if [[ -z "${dst}" || -z "${rate}" || -z "${delay}" ]]; then
      echo "[tc] skipping malformed rule: ${entry}"
      continue
    fi

    tc qdisc replace dev eth0 parent "1:${prio}" handle "${prio}0:" netem delay "${delay}" rate "${rate}"
    tc filter replace dev eth0 protocol ip parent 1:0 prio "${prio}" u32 match ip dst "${dst}/32" flowid "1:${prio}"

    echo "[tc] node-${RPC_PORT}: dst=${dst} rate=${rate} delay=${delay}"
    prio=$((prio + 1))
  done
}

if [[ "${ENABLE_TC}" == "1" ]]; then
  apply_tc_matrix
else
  echo "[tc] disabled by ENABLE_TC=${ENABLE_TC}"
fi

if [[ "${RPC_PORT}" != "9999" ]]; then
  seed_ip="${SEED_GOSSIP_ADDR%:*}"
  until ping -c 1 -W 1 "${seed_ip}" >/dev/null 2>&1; do
    echo "[boot] waiting for seed ${seed_ip}"
    sleep 1
  done
fi

echo "[boot] starting node-${RPC_PORT} (${NODE_IP})"
echo "[boot] flags: rpc-port=${RPC_PORT} rpc-host=${NODE_IP} gossip-bind=${NODE_IP} gossip-port=${GOSSIP_PORT} gossip-adv=${NODE_IP} seed=${SEED_GOSSIP_ADDR} raft-bind=${RAFT_BIND_ADDR} raft-adv=${RAFT_ADVERTISE_IP} rf=${RF} ack=${ACK} bandwidth-mbps=${BANDWIDTH_MBPS} metrics-interval=${METRICS_INTERVAL} metrics-bind-addr=${METRICS_BIND_ADDR} metrics-port=${METRICS_PORT} disk-path=${DISK_PATH}"
exec /workspace/kvserver \
  -rpc-port "${RPC_PORT}" \
  -rpc-host "${NODE_IP}" \
  -gossip-bind-addr "${NODE_IP}" \
  -gossip-port "${GOSSIP_PORT}" \
  -gossip-advertise-ip "${NODE_IP}" \
  -seed-gossip-addr "${SEED_GOSSIP_ADDR}" \
  -raft-bind-addr "${RAFT_BIND_ADDR}" \
  -raft-advertise-ip "${RAFT_ADVERTISE_IP}" \
  -bandwidth-mbps "${BANDWIDTH_MBPS}" \
  -disk-path "${DISK_PATH}" \
  -metrics-interval "${METRICS_INTERVAL}" \
  -metrics-bind-addr "${METRICS_BIND_ADDR}" \
  -metrics-port "${METRICS_PORT}" \
  -ack "${ACK}" \
  -rf "${RF}" \
  -wal-dir "${DATA_DIR}/wal"
