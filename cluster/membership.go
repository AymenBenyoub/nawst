package cluster

import (
	"fmt"
	"log"
	"time"

	"github.com/AymenBenyoub/nawst/core"
	"github.com/hashicorp/memberlist"
)

type Node struct {
	ID                  string
	RPCAddr             string
	Ml                  *memberlist.Memberlist
	EventLoop           *core.EventLoop
	Server              *core.Server
	OnMembershipChanged func()
	HealthScore         float32
	GossipBindAddr      string
	GossipBindPort      int
	GossipAdvertiseIP   string
}

type nodeEventDelegate struct {
	node *Node
}

func (d *nodeEventDelegate) NotifyJoin(n *memberlist.Node) {
	if d == nil || d.node == nil {
		return
	}
	log.Printf("membership event: join name=%s addr=%s", n.Name, n.Address())
	if d.node.OnMembershipChanged != nil {
		go d.node.OnMembershipChanged()
	}
}

func (d *nodeEventDelegate) NotifyLeave(n *memberlist.Node) {
	if d == nil || d.node == nil {
		return
	}
	log.Printf("membership event: leave name=%s addr=%s", n.Name, n.Address())
	if d.node.OnMembershipChanged != nil {
		go d.node.OnMembershipChanged()
	}
}

func (d *nodeEventDelegate) NotifyUpdate(n *memberlist.Node) {
	if d == nil || d.node == nil {
		return
	}
	log.Printf("membership event: update name=%s addr=%s", n.Name, n.Address())
	if d.node.OnMembershipChanged != nil {
		go d.node.OnMembershipChanged()
	}
}

// memberlist.Delgate interface implementation, for now i only need
// NodeMeta to share grpc address for replication, others may be used
// later on.
func (n *Node) NodeMeta(limit int) []byte {
	return fmt.Appendf(nil, "%s:%s", n.ID, n.RPCAddr)
}
func (n *Node) NotifyMsg(b []byte) {

}
func (n *Node) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}
func (n *Node) LocalState(join bool) []byte {
	return nil
}
func (n *Node) MergeRemoteState(buf []byte, join bool) {

}

func (n *Node) CreateCluster() error {
	cfg := memberlist.DefaultLocalConfig()
	// Relax timeouts for local testing environment to avoid flakiness
	cfg.ProbeTimeout = 2 * time.Second
	cfg.ProbeInterval = 2 * time.Second
	cfg.GossipInterval = 500 * time.Millisecond
	cfg.Name = n.ID
	bindAddr := n.GossipBindAddr
	if bindAddr == "" {
		bindAddr = "127.0.0.1"
	}

	advertiseIP := n.GossipAdvertiseIP
	if advertiseIP == "" {
		advertiseIP = "127.0.0.1"
	}
	cfg.BindAddr = bindAddr
	cfg.AdvertiseAddr = advertiseIP
	cfg.BindPort = n.GossipBindPort

	cfg.Delegate = n
	cfg.Events = &nodeEventDelegate{node: n}
	cfg.AdvertisePort = n.GossipBindPort
	log.Printf("memberlist: creation config bind=%s:%d advertise=%s:%d", cfg.BindAddr, cfg.BindPort, cfg.AdvertiseAddr, cfg.AdvertisePort)
	ml, err := memberlist.Create(cfg)
	if err != nil {
		return err
	}
	n.Ml = ml
	return nil

}
func (n *Node) JoinCluster(seedNodeAddr string) error {
	if n.Ml == nil {
		return fmt.Errorf("memberlist is not initialized")
	}
	if seedNodeAddr == "" {
		return fmt.Errorf("seed node address is required")
	}
	_, err := n.Ml.Join([]string{seedNodeAddr})

	return err
}
func (n *Node) LeaveCluster() error {
	if n.Ml == nil {
		return fmt.Errorf("memberlist is not initialized")
	}
	err := n.Ml.Leave(5 * time.Second)

	return err
}
