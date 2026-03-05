package replication

import (
	"context"
	"slices"
	"time"

	"google.golang.org/grpc"

	"go.linka.cloud/badger/v4"
)

func (r *Replication) IsLeader() bool {
	return r.Role() == badger.ReplicationRolePrimary
}

// lock must be held by caller.
func (r *Replication) currentLeader() string {
	if r.leaderID == 0 {
		return ""
	}
	if r.leaderID == r.cfg.NodeID {
		return r.cfg.GRPCAddr
	}
	if p, ok := r.peers[r.leaderID]; ok && p.GRPCAddr != "" {
		return p.GRPCAddr
	}
	if m, ok := r.knownMembers[r.leaderID]; ok {
		return m.GRPCAddr
	}
	return ""
}

func (r *Replication) CurrentLeader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentLeader()
}

func (r *Replication) LeaderConn() (grpc.ClientConnInterface, bool) {
	r.mu.RLock()
	leaderID := r.leaderID
	if leaderID == 0 || leaderID == r.cfg.NodeID {
		r.mu.RUnlock()
		return nil, false
	}
	addr := ""
	if p, ok := r.peers[leaderID]; ok && p.GRPCAddr != "" {
		addr = p.GRPCAddr
	} else if m, ok := r.knownMembers[leaderID]; ok {
		addr = m.GRPCAddr
	}
	timeout := r.cfg.ElectionTimeout
	r.mu.RUnlock()

	if addr == "" {
		return nil, false
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	pc, err := r.peerClientFor(ctx, peerInfo{ID: leaderID, GRPCAddr: addr, Alive: true})
	if err != nil {
		return nil, false
	}
	return pc.conn, true
}

func (r *Replication) Subscribe() <-chan string {
	ch := make(chan string, 1)
	r.mu.Lock()
	id := r.leaderSubsSeq + 1
	r.leaderSubsSeq = id
	r.leaderSubs[id] = ch
	r.mu.Unlock()
	return ch
}

func (r *Replication) closeLeaderSubscribers() {
	r.mu.Lock()
	subs := make([]chan string, 0, len(r.leaderSubs))
	for id, ch := range r.leaderSubs {
		subs = append(subs, ch)
		delete(r.leaderSubs, id)
	}
	r.mu.Unlock()
	for _, ch := range subs {
		close(ch)
	}
}

// lock must be held by caller.
func (r *Replication) captureLeaderChange() ([]chan string, string, bool) {
	leader := r.currentLeader()
	if leader == r.lastLeaderAddr {
		return nil, "", false
	}
	r.lastLeaderAddr = leader
	if len(r.leaderSubs) == 0 {
		return nil, leader, false
	}
	subs := make([]chan string, 0, len(r.leaderSubs))
	for _, ch := range r.leaderSubs {
		subs = append(subs, ch)
	}
	return subs, leader, true
}

func publishLeaderChange(subs []chan string, leader string) {
	if len(subs) == 0 {
		return
	}
	for _, ch := range slices.Clip(subs) {
		select {
		case ch <- leader:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- leader:
			default:
			}
		}
	}
}
