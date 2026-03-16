package cluster

import (
	"fmt"
	"time"

	"github.com/AymenBenyoub/nawst/core"
	"github.com/hashicorp/memberlist"
)

type Node struct {
	ID                string
	Addr              string
	Ml                *memberlist.Memberlist
	EventLoop         *core.EventLoop
	Server            *core.Server
	HealthScore       float32
	GossipBindAddr    string
	GossipBindPort    int
	GossipAdvertiseIP string
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
	fmt.Printf("Node joined cluster: %s , %s\n", n.Ml.LocalNode().String(), n.Addr)
	fmt.Printf("Current cluster members: %v\n", n.Ml.Members())
	return err
}
func (n *Node) LeaveCluster() error {
	if n.Ml == nil {
		return fmt.Errorf("memberlist is not initialized")
	}
	err := n.Ml.Leave(5 * time.Second)

	fmt.Printf("Node left cluster: %s\n", n.Ml.LocalNode().String())
	return err
}
