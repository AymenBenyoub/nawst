package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"github.com/hashicorp/memberlist"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Replicator struct {
	ID string
	Ml *memberlist.Memberlist

	mu    sync.Mutex
	peers map[string]pb.KVClient
	conns map[string]*grpc.ClientConn

	ReplicationFactor int
}

func NewReplicator(id string, ml *memberlist.Memberlist, rf int) *Replicator {
	return &Replicator{
		ID:                id,
		Ml:                ml,
		peers:             make(map[string]pb.KVClient),
		conns:             make(map[string]*grpc.ClientConn),
		ReplicationFactor: rf,
	}
}

func parseMeta(meta []byte) (nodeID string, rpcAddr string, err error) {
	raw := strings.TrimSpace(string(meta))
	if raw == "" {
		return "", "", errors.New("empty member metadata")
	}

	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid member metadata format: %q", raw)
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid member metadata fields: %q", raw)
	}
	return parts[0], parts[1], nil
}

func (r *Replicator) getOrCreateClient(member *memberlist.Node) (pb.KVClient, error) {
	if member == nil {
		return nil, errors.New("nil member")
	}

	peerID, rpcAddr, err := parseMeta(member.Meta)
	if err != nil {
		return nil, fmt.Errorf("parse metadata for %s: %w", member.Name, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if c, ok := r.peers[peerID]; ok {
		return c, nil
	}

	conn, err := grpc.NewClient(
		rpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s (%s): %w", peerID, rpcAddr, err)
	}

	client := pb.NewKVClient(conn)
	r.peers[peerID] = client
	r.conns[peerID] = conn
	return client, nil
}

func (r *Replicator) ReplicateToAll(ctx context.Context, op pb.Op, key string, value []byte) error {
	if r.Ml == nil {
		return errors.New("memberlist is nil")
	}

	members := r.Ml.Members()
	if len(members) == 0 {
		return nil
	}

	// local primary write already succeeded before this function is called
	acks := 1

	effectiveRF := min(max(r.ReplicationFactor, 1), len(members))

	required := effectiveRF/2 + 1
	if acks >= required {
		return nil
	}

	type target struct {
		id     string
		client pb.KVClient
	}

	targets := make([]target, 0, len(members)-1)
	var firstErr error

	for _, member := range members {
		if len(targets) >= effectiveRF-1 {
			break
		}
		peerID, _, err := parseMeta(member.Meta)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("member %s metadata: %w", member.Name, err)
			}
			continue
		}
		if peerID == r.ID {
			continue
		}

		client, err := r.getOrCreateClient(member)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		targets = append(targets, target{id: peerID, client: client})
	}

	remaining := len(targets)
	if acks+remaining < required {
		if firstErr != nil {
			return fmt.Errorf("quorum impossible: need %d acks, have %d local + %d remotes: %w", required, acks, remaining, firstErr)
		}
		return fmt.Errorf("quorum impossible: need %d acks, have %d local + %d remotes", required, acks, remaining)
	}

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan error, len(targets))
	var wg sync.WaitGroup

	for _, t := range targets {
		wg.Add(1)
		go func(pid string, c pb.KVClient) {
			defer wg.Done()

			cctx := callCtx
			if _, hasDeadline := callCtx.Deadline(); !hasDeadline {
				var cCancel context.CancelFunc
				cctx, cCancel = context.WithTimeout(callCtx, 2*time.Second)
				defer cCancel()
			}
			req := &pb.ReplicationRequest{
				Op:    op,
				Key:   key,
				Value: value,
			}

			_, err := c.Replicate(cctx, req)
			if err == nil {
				resultCh <- nil
				return
			}

			retryErr := retryReplication(pid, c, req, cctx)
			if retryErr != nil {
				resultCh <- fmt.Errorf("replicate to %s failed after retries: %w", pid, retryErr)
				return
			}

			resultCh <- nil
		}(t.id, t.client)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for err := range resultCh {
		remaining--

		if err == nil {
			acks++
			if acks >= required {
				cancel()
				return nil
			}
		} else if firstErr == nil {
			firstErr = err
		}

		if acks+remaining < required {
			cancel()
			if firstErr != nil {
				return fmt.Errorf("write quorum not reached: got %d/%d acks: %w", acks, required, firstErr)
			}
			return fmt.Errorf("write quorum not reached: got %d/%d acks", acks, required)
		}
	}

	if acks >= required {
		return nil
	}
	if firstErr != nil {
		return fmt.Errorf("write quorum not reached: got %d/%d acks: %w", acks, required, firstErr)
	}
	return fmt.Errorf("write quorum not reached: got %d/%d acks", acks, required)
}

func (r *Replicator) ReplicatePut(ctx context.Context, key string, value []byte) error {
	return r.ReplicateToAll(ctx, pb.Op_PUT, key, value)
}

func (r *Replicator) ReplicateDelete(ctx context.Context, key string) error {
	return r.ReplicateToAll(ctx, pb.Op_DELETE, key, nil)
}
func retryReplication(pid string, c pb.KVClient, req *pb.ReplicationRequest, cctx context.Context) error {
	const maxAttempts = 3
	backoff := 500 * time.Millisecond

	var lastErr error

	for attempt := range maxAttempts {
		select {
		case <-cctx.Done():
			return fmt.Errorf("context cancelled while retrying replication to %s: %w", pid, cctx.Err())
		default:
		}

		_, err := c.Replicate(cctx, req)
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt < maxAttempts-1 {
			select {
			case <-cctx.Done():
				return fmt.Errorf("context cancelled while retrying replication to %s: %w", pid, cctx.Err())
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}

	return fmt.Errorf("retry replication to %s exhausted: %w", pid, lastErr)
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error
	for peerID, conn := range r.conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close connection to %s: %w", peerID, err)
		}
	}
	r.peers = make(map[string]pb.KVClient)
	r.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}

// Compile-time check that this file imports emptypb only when generated stubs are in sync.
var _ = emptypb.Empty{}
