package cluster

import (
	"fmt"
	"time"

	"github.com/AymenBenyoub/nawst/core"
	"github.com/hashicorp/memberlist"
)

type Node struct {
	ID                string
	RPCAddr           string
	Ml                *memberlist.Memberlist
	EventLoop         *core.EventLoop
	Server            *core.Server
	HealthScore       float32
	GossipBindAddr    string
	GossipBindPort    int
	GossipAdvertiseIP string
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
	cfg.Name = n.ID
	bindAddr := n.GossipBindAddr
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}

	advertiseIP := n.GossipAdvertiseIP
	if advertiseIP == "" {
		advertiseIP = "127.0.0.1"
	}
	cfg.BindAddr = bindAddr
	cfg.AdvertiseAddr = advertiseIP
	cfg.BindPort = n.GossipBindPort

	cfg.Delegate = n
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
