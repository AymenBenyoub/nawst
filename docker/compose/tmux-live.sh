#!/usr/bin/env bash
set -euo pipefail

SESSION="${1:-nawst}"
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

cd "$ROOT_DIR"

echo "[tmux-live] ensuring compose stack is running"
docker compose up -d --build --remove-orphans

echo "[tmux-live] waiting for server containers to be running"
for svc in node9999 node10000 node10001 node10002; do
  ok=0
  for _ in $(seq 1 30); do
    if docker compose ps --status running --services | grep -qx "$svc"; then
      ok=1
      break
    fi
    sleep 1
  done
  if [[ "$ok" != "1" ]]; then
    echo "[tmux-live] service '$svc' did not reach running state"
    echo "[tmux-live] recent logs:"
    docker compose logs --tail=40 "$svc" || true
    exit 1
  fi
done

if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "[tmux-live] tmux session '$SESSION' already exists, attaching"
  exec tmux attach -t "$SESSION"
fi

echo "[tmux-live] creating session '$SESSION'"
tmux new-session -d -s "$SESSION" "cd '$ROOT_DIR' && docker compose logs --tail=50 -f node9999"
tmux split-window -h -t "$SESSION":0 "cd '$ROOT_DIR' && docker compose logs --tail=50 -f node10000"
tmux split-window -v -t "$SESSION":0.0 "cd '$ROOT_DIR' && docker compose logs --tail=50 -f node10001"
tmux split-window -v -t "$SESSION":0.1 "cd '$ROOT_DIR' && docker compose logs --tail=50 -f node10002"

# Extra window for quick client access (opens kvcli directly)
tmux new-window -t "$SESSION" -n client "cd '$ROOT_DIR' && docker compose exec client /workspace/kvcli -addr node9999:9999"

tmux select-layout -t "$SESSION":0 tiled
tmux set-option -t "$SESSION" remain-on-exit on
tmux select-window -t "$SESSION":0

echo "[tmux-live] attaching"
exec tmux attach -t "$SESSION"
