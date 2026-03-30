#!/bin/bash
update_node() {
  local node=$1
  local ip=$2
  local port=$3
  local gossip=$4
  local speed=$5
  
  cat << INNER_EOF > lab/kathara_cluster/${node}.startup
#!/bin/sh
set -e

ip addr add ${ip}/24 dev eth0
ip link set eth0 up

# Simulate variable network link capacities
tc qdisc add dev eth0 root tbf rate ${speed}mbit burst 32kbit limit 32000

chmod +x /shared/bin/kvserver
mkdir -p /tmp/kvst

exec /shared/bin/kvserver \\
  -rpc-port ${port} \\
  -rpc-host ${ip%%/*} \\
  -gossip-bind-addr 0.0.0.0 \\
  -gossip-port ${gossip} \\
  -gossip-advertise-ip ${ip%%/*} \\
  -seed-gossip-addr 10.0.0.9:7946 \\
  -rf 3 \\
  -bandwidth-mbps ${speed}

echo "${node} started: rpc=${ip%%/*}:${port} gossip=${ip%%/*}:${gossip} at ${speed}Mbps"
INNER_EOF
}

update_node n9999 10.0.0.9 9999 7946 100
update_node n10000 10.0.0.10 10000 7947 50
update_node n10001 10.0.0.11 10001 7948 20
update_node n10002 10.0.0.12 10002 7949 5

chmod +x update_lab.sh
./update_lab.sh
rm update_lab.sh
