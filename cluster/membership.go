package cluster

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/AymenBenyoub/nawst/core"
	"github.com/hashicorp/memberlist"
)

type Node struct {
	ID        string
	RPCAddr   string
	RaftAddr  string
	Ml        *memberlist.Memberlist
	EventLoop *core.EventLoop
	Server    *core.Server

	OnMembershipChanged func()
	HealthScore         float32
	GossipBindAddr      string
	GossipBindPort      int
	GossipAdvertiseIP   string

	//control fields
	Reconciler       Reconciler
	MembershipEvents chan MembershipEvent
	reconcileCh      chan struct{}
	stopCh           chan struct{}
	wg               sync.WaitGroup
	broadcasts       *memberlist.TransmitLimitedQueue
	metricsHandler   func(*GossipMetricsMessage)
}

type Reconciler interface {
	IsLeader() bool
	ReconcileRaftWithMembership(members []*memberlist.Node) error
}
type MembershipEventType int

const (
	MembershipJoin MembershipEventType = iota
	MembershipLeave
	MembershipUpdate
)

type MembershipEvent struct {
	Type   MembershipEventType
	NodeID string
	Addr   string
	At     time.Time
}
type nodeEventDelegate struct {
	node *Node
}

func (n *Node) StartControlChannels() {
	if n.MembershipEvents == nil {
		n.MembershipEvents = make(chan MembershipEvent, 100)
	}
	if n.reconcileCh == nil {
		n.reconcileCh = make(chan struct{}, 1)
	}
	if n.stopCh == nil {
		n.stopCh = make(chan struct{})
	}
}

type messageBroadcast struct {
	msg []byte
}

func (b *messageBroadcast) Invalidates(other memberlist.Broadcast) bool {
	ob, ok := other.(*messageBroadcast)
	if !ok {
		return false
	}
	return bytes.Equal(b.msg, ob.msg)
}

func (b *messageBroadcast) Message() []byte {
	return b.msg
}

func (b *messageBroadcast) Finished() {}

func (n *Node) EnqueueMembershipEvent(event MembershipEvent) {
	select {
	case n.MembershipEvents <- event:
	default:
		return
	}
}

func (n *Node) SetMetricsHandler(handler func(*GossipMetricsMessage)) {
	n.metricsHandler = handler
}

func (n *Node) QueueBroadcastMessage(msg []byte) error {
	if len(msg) == 0 {
		return fmt.Errorf("empty broadcast message")
	}
	if n.broadcasts == nil {
		return fmt.Errorf("broadcast queue is not initialized")
	}
	n.broadcasts.QueueBroadcast(&messageBroadcast{msg: append([]byte(nil), msg...)})
	return nil
}

func (n *Node) signalReconcile() {
	select {
	case n.reconcileCh <- struct{}{}:
	default:
	}
}
func (n *Node) StartMembershipWorkers() {
	n.StartControlChannels()
	n.wg.Add(2)
	go n.membershipLoop()
	go n.reconcileLoop()
}
func (n *Node) StopMembershipWorkers() {
	close(n.stopCh)
	n.wg.Wait()
}
func (n *Node) membershipLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case event := <-n.MembershipEvents:
			_ = event
			n.signalReconcile()
		}
	}
}
func (n *Node) reconcileLoop() {
	defer n.wg.Done()
	const reconcileDebounce = 200 * time.Millisecond

	var debounceTimer *time.Timer
	var debounceC <-chan time.Time

	resetDebounce := func() {
		if debounceTimer == nil {
			debounceTimer = time.NewTimer(reconcileDebounce)
			debounceC = debounceTimer.C
			return
		}
		if !debounceTimer.Stop() {
			select {
			case <-debounceTimer.C:
			default:
			}
		}
		debounceTimer.Reset(reconcileDebounce)
		debounceC = debounceTimer.C
	}

	for {
		select {
		case <-n.stopCh:
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return
		case <-n.reconcileCh:
			resetDebounce()
		case <-debounceC:
			debounceC = nil
			if n.Ml == nil || n.Reconciler == nil {
				continue
			}
			if !n.Reconciler.IsLeader() {
				continue
			}
			members := n.Ml.Members()
			_ = n.Reconciler.ReconcileRaftWithMembership(members)

		}
	}
}
func (d *nodeEventDelegate) NotifyJoin(n *memberlist.Node) {
	if d == nil || d.node == nil {
		return
	}
	d.node.EnqueueMembershipEvent(MembershipEvent{
		Type:   MembershipJoin,
		NodeID: n.Name,
		Addr:   n.Address(),
		At:     time.Now(),
	})
}

func (d *nodeEventDelegate) NotifyLeave(n *memberlist.Node) {
	if d == nil || d.node == nil {
		return
	}
	d.node.EnqueueMembershipEvent(MembershipEvent{
		Type:   MembershipLeave,
		NodeID: n.Name,
		Addr:   n.Address(),
		At:     time.Now(),
	})
}

func (d *nodeEventDelegate) NotifyUpdate(n *memberlist.Node) {
	if d == nil || d.node == nil {
		return
	}
	d.node.EnqueueMembershipEvent(MembershipEvent{
		Type:   MembershipUpdate,
		NodeID: n.Name,
		Addr:   n.Address(),
		At:     time.Now(),
	})
}

// memberlist.Delgate interface implementation, for now i only need
// NodeMeta to share grpc address for replication, others may be used
// later on.
func (n *Node) NodeMeta(limit int) []byte {
	if n.RaftAddr == "" {
		return fmt.Appendf(nil, "%s,%s", n.ID, n.RPCAddr)
	}
	return fmt.Appendf(nil, "%s,%s,%s", n.ID, n.RPCAddr, n.RaftAddr)
}
func (n *Node) NotifyMsg(b []byte) {
	msg, err := DecodeGossipMetricsMessage(b)
	if err != nil {
		return
	}
	if n.metricsHandler != nil {
		n.metricsHandler(msg)
	}
}
func (n *Node) GetBroadcasts(overhead, limit int) [][]byte {
	if n.broadcasts == nil {
		return nil
	}
	return n.broadcasts.GetBroadcasts(overhead, limit)
}
func (n *Node) LocalState(join bool) []byte {
	return nil
}
func (n *Node) MergeRemoteState(buf []byte, join bool) {

}

func (n *Node) CreateCluster() error {
	cfg := memberlist.DefaultLANConfig()
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
	cfg.Logger = log.New(io.Discard, "", 0)
	ml, err := memberlist.Create(cfg)
	if err != nil {
		return err
	}
	n.Ml = ml
	n.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			if n.Ml == nil {
				return 1
			}
			return n.Ml.NumMembers()
		},
		RetransmitMult: 3,
	}
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
