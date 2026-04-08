#!/usr/bin/env bash
set -euo pipefail

SESSION="${1:-nawst-local}"
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$ROOT_DIR"



if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "[tmux-live-local] tmux session '$SESSION' already exists, attaching"
  exec tmux attach -t "$SESSION"
fi

echo "[tmux-live-local] creating session '$SESSION'"

# Run node 1 (seed)
tmux new-session -d -s "$SESSION" "cd '$ROOT_DIR' && ./kvserver -rpc-port 9999 -raft-bind-addr 127.0.0.1 -raft-advertise-ip 127.0.0.1 -gossip-port 7946 -seed-gossip-addr 127.0.0.1:7946 -bandwidth-mbps 200 -metrics-interval 4s -rf 3"
# Run node 2
tmux split-window -h -t "$SESSION":0 "cd '$ROOT_DIR' && sleep 1 && ./kvserver -rpc-port 10000 -raft-bind-addr 127.0.0.1 -raft-advertise-ip 127.0.0.1 -gossip-port 7947 -seed-gossip-addr 127.0.0.1:7946 -bandwidth-mbps 150 -metrics-interval 4s -rf 3"
# Run node 3
tmux split-window -v -t "$SESSION":0.0 "cd '$ROOT_DIR' && sleep 2 && ./kvserver -rpc-port 10001 -raft-bind-addr 127.0.0.1 -raft-advertise-ip 127.0.0.1 -gossip-port 7948 -seed-gossip-addr 127.0.0.1:7946 -bandwidth-mbps 100 -metrics-interval 4s -rf 3"
# Run node 4
tmux split-window -v -t "$SESSION":0.1 "cd '$ROOT_DIR' && sleep 3 && ./kvserver -rpc-port 10002 -raft-bind-addr 127.0.0.1 -raft-advertise-ip 127.0.0.1 -gossip-port 7949 -seed-gossip-addr 127.0.0.1:7946 -bandwidth-mbps 60 -metrics-interval 4s -rf 3"

# Extra window for quick client access
tmux new-window -t "$SESSION" -n client "cd '$ROOT_DIR' && ./kvcli -addr 127.0.0.1:9999; echo; echo [client] kvcli exited; exec sh"

tmux select-layout -t "$SESSION":0 tiled
tmux set-option -t "$SESSION" remain-on-exit on
tmux select-window -t "$SESSION":0

echo "[tmux-live-local] attaching"
