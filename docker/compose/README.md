Docker Compose Demo (Live Logs + tc Link Shaping)

What this stack gives you
- Live aggregated logs via docker compose up
- 4 kvserver nodes + 1 client
- Per-destination tc shaping on every node (bandwidth + latency)
- Correct gossip and raft advertise addresses for container networking

Files
- docker-compose.yml
- docker/compose/Dockerfile
- docker/compose/start-node.sh
- docker/compose/demo.sh

Quick start
1) From repo root run:
   bash docker/compose/demo.sh

2) Open another terminal and run client commands:
   docker compose exec client /workspace/kvcli -addr 10.44.0.9:9999 put foo bar
   docker compose exec client /workspace/kvcli -addr 10.44.0.9:9999 get foo

3) Stop demo:
   Ctrl+C in the compose terminal, then:
   docker compose down -v

How tc is configured
- Each node has TC_RULES in docker-compose.yml in this format:
  dstIP,rate,delay;dstIP,rate,delay;...

- Example:
  10.44.0.10,120mbit,8ms;10.44.0.11,60mbit,20ms;10.44.0.12,20mbit,40ms

- start-node.sh applies those rules on eth0 using:
  tc qdisc ... netem delay <delay> rate <rate>
  tc filter ... match ip dst <dstIP>

Tuning tips
- Theoretical per-node NIC capacity used by scoring is BANDWIDTH_MBPS.
- Pairwise path constraints are set by TC_RULES.
- For harsher effects, reduce rate and increase delay per path.

Realistic local-LAN profile (recommended for perf + realism)
- Run with the override file:
  docker compose -f docker-compose.yml -f docker-compose.real.yml up -d --build
- What this profile does:
  - Keeps RF=3 and quorum semantics.
  - Enables tc with low-latency, high-bandwidth LAN-like links (sub-ms delays, multi-gigabit rates).
  - Slows metrics/placement polling pressure (10s interval) to reduce epoch churn from host-noise.
  - Applies per-container CPU and memory limits to reduce scheduler interference and simulate isolated nodes.

Durability mode
- ACK is now passed through start-node.sh to kvserver:
  - ACK=0: ack after enqueue (fastest, weakest)
  - ACK=1: ack after flush (balanced default)
  - ACK=2: ack after fsync (strongest durability, slower)
