# Production Deployment Guide

This guide describes a hardened deployment mode for NAWST on LAN or multi-site networks.

## What Changed

The codebase now supports TLS for:
- gRPC server listener (client-facing and inter-node RPC)
- Inter-node replication clients
- CLI and load generator clients

New flags are available on:
- `cmd/server/main.go`
- `cmd/cli/main.go`
- `cmd/loadgen/main.go`

## 1) Build Binaries

From repo root:

```bash
go build -o kvserver ./cmd/server
go build -o kvcli ./cmd/cli
go build -o workload ./cmd/loadgen
```

## 2) Create Certificates (Example)

For real deployments, use your PKI and host-specific certificates.
The commands below are for quick bring-up.

```bash
mkdir -p certs

# CA
openssl genrsa -out certs/ca.key 4096
openssl req -x509 -new -nodes -key certs/ca.key -sha256 -days 3650 \
  -subj "/CN=nawst-ca" -out certs/ca.crt

# Server cert/key for one node (repeat per host with proper CN/SAN)
openssl genrsa -out certs/server.key 2048
openssl req -new -key certs/server.key -subj "/CN=node1.local" -out certs/server.csr

cat > certs/server.ext <<EOF
subjectAltName=DNS:node1.local,IP:127.0.0.1
extendedKeyUsage=serverAuth
EOF

openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key \
  -CAcreateserial -out certs/server.crt -days 365 -sha256 -extfile certs/server.ext
```

For multi-node, generate one cert per node with each node's DNS/IP in SAN.

## 3) Start Seed Node (TLS Enabled)

```bash
./kvserver \
  -rpc-port 9999 \
  -rpc-host 10.0.0.10 \
  -rpc-tls-enable \
  -rpc-tls-cert-file certs/server.crt \
  -rpc-tls-key-file certs/server.key \
  -rpc-tls-ca-cert-file certs/ca.crt \
  -rpc-tls-server-name node1.local \
  -gossip-bind-addr 0.0.0.0 \
  -gossip-advertise-ip 10.0.0.10 \
  -seed-gossip-addr 10.0.0.10:7946 \
  -rf 3
```

## 4) Start Additional Nodes

Run on each host with unique ports/addresses and node certificate files:

```bash
./kvserver \
  -rpc-port 10000 \
  -rpc-host 10.0.0.11 \
  -rpc-tls-enable \
  -rpc-tls-cert-file certs/node2.crt \
  -rpc-tls-key-file certs/node2.key \
  -rpc-tls-ca-cert-file certs/ca.crt \
  -rpc-tls-server-name node2.local \
  -gossip-bind-addr 0.0.0.0 \
  -gossip-advertise-ip 10.0.0.11 \
  -seed-gossip-addr 10.0.0.10:7946 \
   -raft-bind-addr 0.0.0.0 \
  -raft-advertise-ip 10.0.0.11: \
  -bandwidth-mbps XX \
  -rf 3
```

## 5) Run CLI Securely

```bash
./kvcli \
  -addr 10.0.0.10:9999 \
  -tls-enable \
  -tls-ca-cert-file certs/ca.crt \
  -tls-server-name node1.local
```

## 6) Run Workload Securely

```bash
./workload \
  -addr 10.0.0.10:9999 \
  -clients 120 \
  -conns 32 \
  -timeout 5s \
  -tls-enable \
  -tls-ca-cert-file certs/ca.crt \
  -tls-server-name node1.local \
  -dist zipf -zipf-s 1.10 -zipf-v 1.0 \
  -read-pct 80 -write-pct 15 -delete-pct 5 \
  -warmup 30s -duration 8m -progress 5s
```

## WAN / Multi-Location Notes

- Use DNS names and certificates that match each node's advertised endpoint.
- Keep `-rpc-host` and `-gossip-advertise-ip` reachable from all peers.
- Avoid `-rpc-tls-insecure-skip-verify` outside short-lived testing.
- Use `-ack 2` for strongest durability if latency budget allows.
- Increase `-timeout` for high-latency inter-region traffic.

## Recommended Baseline for Real Environments

- TLS enabled everywhere
- RF=3, quorum writes enabled (default behavior)
- `ack=1` (balanced) or `ack=2` (strong durability)
- Dedicated persistent storage path for WAL (`-wal-dir`)
- Observability endpoint restricted to trusted network
