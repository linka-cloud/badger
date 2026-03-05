package replication

import (
	"context"
	"log"
	"strconv"
	"time"

	"go.linka.cloud/badger/v4"
)

func (r *Replication) loop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.tick()
		case <-ctx.Done():
			return
		}
	}
}

func (r *Replication) tick() {
	now := time.Now()
	r.mu.RLock()
	role := r.role
	leaderID := r.leaderID
	lastHB := r.lastHeartbeat
	r.mu.RUnlock()
	if role == badger.ReplicationRolePrimary {
		r.sendHeartbeats()
		r.maybeReconcileReplicas(now)
		return
	}
	if !r.canParticipateInElection(now) {
		return
	}
	if leaderID == 0 || now.Sub(lastHB) >= r.cfg.LeaderTimeout {
		go r.startElection()
	}
}

func (r *Replication) maybeReconcileReplicas(now time.Time) {
	r.mu.Lock()
	if r.role != badger.ReplicationRolePrimary || r.reconcileRunning {
		r.mu.Unlock()
		return
	}
	interval := r.cfg.LeaderTimeout
	if interval <= 0 {
		interval = time.Second
	}
	if minInterval := 5 * time.Second; interval < minInterval {
		interval = minInterval
	}
	if !r.lastReconcile.IsZero() && now.Sub(r.lastReconcile) < interval {
		r.mu.Unlock()
		return
	}
	r.reconcileRunning = true
	epoch := r.epoch
	r.mu.Unlock()

	go func() {
		r.synchronizeReplicas(epoch)
		r.mu.Lock()
		r.reconcileRunning = false
		r.lastReconcile = time.Now()
		r.mu.Unlock()
	}()
}

func (r *Replication) canParticipateInElection(now time.Time) bool {
	if now.Sub(r.startupAt) < r.cfg.StartupGrace {
		return false
	}
	r.mu.RLock()
	known := make([]persistedMember, 0, len(r.knownMembers))
	for _, m := range r.knownMembers {
		if m.ID == r.cfg.NodeID {
			continue
		}
		known = append(known, m)
	}
	alive := map[uint64]bool{}
	for _, p := range r.peers {
		if p.Alive {
			alive[p.ID] = true
		}
	}
	r.mu.RUnlock()

	if len(known) == 0 {
		return true
	}
	seen := 0
	for _, m := range known {
		if alive[m.ID] {
			seen++
		}
	}
	ratio := float64(seen) / float64(len(known))
	if ratio < r.cfg.RequiredSeenRatio {
		return false
	}
	for _, m := range known {
		if m.ID <= r.cfg.NodeID || alive[m.ID] {
			continue
		}
		if now.Sub(time.Unix(m.LastSeenUnix, 0)) < r.cfg.KnownUnreachableAfter {
			return false
		}
	}
	return true
}

func (r *Replication) startElection() {
	started := time.Now()
	r.mu.Lock()
	if r.electionRunning {
		r.mu.Unlock()
		return
	}
	r.electionRunning = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.electionRunning = false
		r.mu.Unlock()
	}()

	self, err := r.localNodeState()
	if err != nil {
		log.Printf("election: read local cursor failed: %v", err)
		return
	}
	log.Printf("election start: node=%d epoch=%d cursor=(%d,%d,%d)", r.cfg.NodeID, self.Epoch, self.Cursor.Fid, self.Cursor.Offset, self.Cursor.Len)

	r.mu.RLock()
	epoch := r.epoch
	peers := r.alivePeers()
	r.mu.RUnlock()

	higherPriority := make([]peerInfo, 0, len(peers))
	for _, p := range peers {
		ctx, cancel := context.WithTimeout(r.ctx, r.cfg.ElectionTimeout)
		resp, err := r.callGetNodeState(ctx, p)
		cancel()
		if err != nil || resp.GetError() != "" {
			continue
		}
		state := nodeStateFromPB(resp)
		if isStatePreferred(state, self) {
			higherPriority = append(higherPriority, p)
		}
	}

	if len(higherPriority) == 0 {
		log.Printf("election no higher-priority candidate: node=%d epoch=%d duration=%s", r.cfg.NodeID, epoch, time.Since(started).Truncate(time.Millisecond))
		r.becomeLeader(epoch + 1)
		return
	}

	ack := false
	for _, p := range higherPriority {
		ctx, cancel := context.WithTimeout(r.ctx, r.cfg.ElectionTimeout)
		resp, err := r.callElection(ctx, p, self)
		cancel()
		if err == nil && resp.GetAck() {
			ack = true
			break
		}
	}
	if !ack {
		log.Printf("election fallback self-elect: node=%d epoch=%d higher=%d duration=%s", r.cfg.NodeID, epoch, len(higherPriority), time.Since(started).Truncate(time.Millisecond))
		r.becomeLeader(epoch + 1)
		return
	}

	select {
	case <-r.coordinatorPulse:
		log.Printf("election got coordinator: node=%d duration=%s", r.cfg.NodeID, time.Since(started).Truncate(time.Millisecond))
		return
	case <-time.After(r.cfg.ElectionTimeout):
		log.Printf("election timeout waiting coordinator: node=%d duration=%s", r.cfg.NodeID, time.Since(started).Truncate(time.Millisecond))
		go r.startElection()
	case <-r.ctx.Done():
		return
	}
}

func (r *Replication) becomeLeader(epoch uint64) {
	r.mu.Lock()
	if epoch <= r.epoch {
		epoch = r.epoch + 1
	}
	r.epoch = epoch
	r.leaderID = r.cfg.NodeID
	r.role = badger.ReplicationRolePrimary
	r.lastHeartbeat = time.Now()
	r.lastReconcile = time.Now()
	r.reconcileRunning = true
	subs, leader, changed := r.captureLeaderChange()
	state := r.snapshotState()
	peers := r.alivePeers()
	r.mu.Unlock()
	if changed {
		publishLeaderChange(subs, leader)
	}
	if err := r.saveState(state); err != nil {
		log.Printf("persist state: %v", err)
	}
	log.Printf("leader elected: node=%d epoch=%d", r.cfg.NodeID, epoch)
	for _, p := range peers {
		ctx, cancel := context.WithTimeout(r.ctx, r.cfg.ElectionTimeout)
		_, _ = r.callCoordinator(ctx, p, r.cfg.NodeID, epoch)
		cancel()
	}
	go func() {
		r.synchronizeReplicas(epoch)
		r.mu.Lock()
		r.reconcileRunning = false
		r.lastReconcile = time.Now()
		r.mu.Unlock()
	}()
}

// lock must be held by caller.
func (r *Replication) snapshotState() persistedState {
	st := persistedState{
		NodeID:            r.cfg.NodeID,
		Epoch:             r.epoch,
		LastKnownLeaderID: r.leaderID,
		KnownMembers:      map[string]persistedMember{},
	}
	for id, m := range r.knownMembers {
		st.KnownMembers[strconv.FormatUint(id, 10)] = m
	}
	return st
}

func (r *Replication) sendHeartbeats() {
	r.mu.RLock()
	epoch := r.epoch
	peers := r.alivePeers()
	r.mu.RUnlock()
	for _, p := range peers {
		ctx, cancel := context.WithTimeout(r.ctx, r.cfg.ElectionTimeout)
		resp, err := r.callHeartbeat(ctx, p, r.cfg.NodeID, epoch)
		cancel()
		if err == nil && !resp.GetAccepted() && resp.GetResponderEpoch() > epoch {
			r.stepDown(resp.GetResponderEpoch(), 0)
		}
	}
}

func (r *Replication) stepDown(epoch uint64, leaderID uint64) {
	r.mu.Lock()
	if epoch < r.epoch {
		r.mu.Unlock()
		return
	}
	r.epoch = epoch
	r.leaderID = leaderID
	r.role = badger.ReplicationRoleReplica
	r.lastHeartbeat = time.Now()
	r.reconcileRunning = false
	subs, leader, changed := r.captureLeaderChange()
	state := r.snapshotState()
	r.mu.Unlock()
	if changed {
		publishLeaderChange(subs, leader)
	}
	if err := r.saveState(state); err != nil {
		log.Printf("persist state: %v", err)
	}
}

func (r *Replication) currentEpoch() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.epoch
}

func (r *Replication) CurrentEpoch() uint64 {
	return r.currentEpoch()
}
