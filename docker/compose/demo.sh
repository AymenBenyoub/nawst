#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

echo "[demo] building server + cli host binaries"
CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags="-w -s" -o kvserver ./cmd/server/main.go
CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags="-w -s" -o kvcli ./cmd/cli/main.go

echo "[demo] starting docker compose with live aggregated logs"
docker compose up --build --remove-orphans
