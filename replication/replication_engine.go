package replication

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.linka.cloud/badger/v4"
)

func (r *Replication) Replicate(ctx context.Context, frame *badger.WALFrame) error {
	if frame == nil {
		return nil
	}
	r.mu.Lock()
	if r.role != badger.ReplicationRolePrimary {
		r.mu.Unlock()
		return nil
	}
	r.pending = append(r.pending, frame.CloneVT())
	flush := len(r.pending) >= r.cfg.BatchSize || frame.Type != badger.WALFrameTxnIntent
	if !flush {
		r.mu.Unlock()
		return nil
	}
	batch := r.pending
	r.pending = nil
	epoch := r.epoch
	leaderID := r.cfg.NodeID
	flushSync := frame.Type != badger.WALFrameTxnIntent
	r.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	task := replicationTask{batch: batch, leaderID: leaderID, epoch: epoch, sync: flushSync}
	if flushSync {
		task.done = make(chan error, 1)
	}

	select {
	case r.replCh <- task:
	case <-r.ctx.Done():
		return r.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}

	if !flushSync {
		return nil
	}

	select {
	case err := <-task.done:
		return err
	case <-r.ctx.Done():
		return r.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Replication) replicationLoop(ctx context.Context) {
	for {
		select {
		case task := <-r.replCh:
			err := r.executeReplicationTask(task)
			if r.cfg.testHooks != nil && r.cfg.testHooks.onAfterReplicationBatch != nil {
				r.cfg.testHooks.onAfterReplicationBatch(task, err)
			}
			if task.sync {
				task.done <- err
				continue
			}
			if err != nil {
				log.Printf("replication async flush failed: leader=%d epoch=%d batch=%d err=%v", task.leaderID, task.epoch, len(task.batch), err)
			}
		case <-r.ctx.Done():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (r *Replication) executeReplicationTask(task replicationTask) error {
	r.mu.RLock()
	allPeers := r.alivePeers()
	r.mu.RUnlock()
	peers := make([]peerInfo, 0, len(allPeers))
	for _, p := range allPeers {
		if r.isPeerRecovering(p.ID) {
			continue
		}
		peers = append(peers, p)
	}
	started := time.Now()

	if len(peers) == 0 {
		return nil
	}
	required := r.requiredAcks(len(peers))
	if required == 0 {
		return nil
	}

	ackCh := make(chan bool, len(peers))
	replCtx, cancelAll := context.WithCancel(r.ctx)
	defer cancelAll()
	for _, p := range peers {
		p := p
		go func() {
			timeout := r.cfg.ApplyTimeout
			if task.sync {
				timeout = r.cfg.ApplyTimeout * 20
			}
			rpcCtx, cancel := context.WithTimeout(replCtx, timeout)
			defer cancel()
			err := r.applyFramesToPeer(rpcCtx, p, task.leaderID, task.epoch, task.batch)
			if err != nil && r.shouldTriggerRecovery(err) {
				r.triggerPeerRecovery(task.epoch, p, err)
			}
			ackCh <- err == nil
		}()
	}
	acks := 0
	remaining := len(peers)
	for remaining > 0 {
		if acks >= required {
			d := time.Since(started)
			if d > 100*time.Millisecond {
				log.Printf("replication flush slow: leader=%d epoch=%d batch=%d peers=%d required=%d acked=%d sync=%t duration=%s", task.leaderID, task.epoch, len(task.batch), len(peers), required, acks, task.sync, d.Truncate(time.Millisecond))
			}
			cancelAll()
			return nil
		}
		if acks+remaining < required {
			break
		}
		ok := <-ackCh
		remaining--
		if ok {
			acks++
		}
	}
	if acks < required {
		log.Printf("replication flush quorum failed: leader=%d epoch=%d batch=%d peers=%d required=%d acked=%d sync=%t duration=%s", task.leaderID, task.epoch, len(task.batch), len(peers), required, acks, task.sync, time.Since(started).Truncate(time.Millisecond))
		return fmt.Errorf("quorum not met: required=%d got=%d", required, acks)
	}
	if d := time.Since(started); d > 100*time.Millisecond {
		log.Printf("replication flush slow: leader=%d epoch=%d batch=%d peers=%d required=%d acked=%d sync=%t duration=%s", task.leaderID, task.epoch, len(task.batch), len(peers), required, acks, task.sync, d.Truncate(time.Millisecond))
	}
	return nil
}

func (r *Replication) requiredAcks(peerCount int) int {
	if r.cfg.QuorumAcks > 0 {
		if r.cfg.QuorumAcks > peerCount {
			return peerCount
		}
		return r.cfg.QuorumAcks
	}
	cluster := peerCount + 1
	majority := cluster/2 + 1
	required := majority - 1
	if required < 0 {
		return 0
	}
	return required
}
