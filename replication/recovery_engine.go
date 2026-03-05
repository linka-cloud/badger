package replication

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"go.linka.cloud/badger/v4"
	badgerpb "go.linka.cloud/badger/v4/pb"

	replicationpb "go.linka.cloud/badger/replication/pb"
)

func (r *Replication) shouldTriggerRecovery(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "wal offset mismatch") || strings.Contains(s, "cursor")
}

func (r *Replication) isPeerRecovering(peerID uint64) bool {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	_, ok := r.recoveringPeers[peerID]
	return ok
}

func (r *Replication) beginPeerRecovery(peerID uint64) bool {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	if _, ok := r.recoveringPeers[peerID]; ok {
		return false
	}
	r.recoveringPeers[peerID] = struct{}{}
	return true
}

func (r *Replication) endPeerRecovery(peerID uint64) {
	r.recoveryMu.Lock()
	delete(r.recoveringPeers, peerID)
	r.recoveryMu.Unlock()
}

func (r *Replication) triggerPeerRecovery(epoch uint64, p peerInfo, cause error) {
	if !r.beginPeerRecovery(p.ID) {
		return
	}

	go func() {
		log.Printf("recovery trigger: leader=%d peer=%d epoch=%d cause=%v", r.cfg.NodeID, p.ID, epoch, cause)
		if err := r.syncPeerWithRetry(epoch, p); err != nil {
			log.Printf("recovery trigger failed: leader=%d peer=%d epoch=%d err=%v", r.cfg.NodeID, p.ID, epoch, err)
		} else {
			log.Printf("recovery trigger done: leader=%d peer=%d epoch=%d", r.cfg.NodeID, p.ID, epoch)
		}
		r.endPeerRecovery(p.ID)
	}()
}

func (r *Replication) synchronizeReplicas(epoch uint64) {
	r.mu.RLock()
	peers := r.alivePeers()
	r.mu.RUnlock()
	for _, p := range peers {
		if !r.beginPeerRecovery(p.ID) {
			continue
		}
		err := r.syncPeerWithRetry(epoch, p)
		r.endPeerRecovery(p.ID)
		if err != nil {
			log.Printf("sync peer %d failed: %v", p.ID, err)
		}
	}
}

func (r *Replication) SynchronizeReplicas() {
	r.synchronizeReplicas(r.currentEpoch())
}

func (r *Replication) syncPeerWithRetry(epoch uint64, p peerInfo) error {
	r.recoveryRunMu.RLock()
	defer r.recoveryRunMu.RUnlock()

	var lastErr error
	for attempt := 1; attempt <= 6; attempt++ {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		if err := r.syncPeer(epoch, p); err != nil {
			lastErr = err
			if attempt < 6 {
				d := time.Duration(attempt) * 250 * time.Millisecond
				t := time.NewTimer(d)
				select {
				case <-t.C:
				case <-r.ctx.Done():
					if !t.Stop() {
						<-t.C
					}
					return r.ctx.Err()
				}
				continue
			}
			break
		}
		return nil
	}
	return lastErr
}

func (r *Replication) syncPeer(epoch uint64, p peerInfo) error {
	if r.cfg.testHooks != nil && r.cfg.testHooks.onSyncPeer != nil {
		return r.cfg.testHooks.onSyncPeer(epoch, p)
	}
	syncStarted := time.Now()
	log.Printf("recovery sync start: leader=%d peer=%d epoch=%d", r.cfg.NodeID, p.ID, epoch)
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.ApplyTimeout)
	resp, err := r.callGetCursor(ctx, p, r.cfg.NodeID, epoch)
	cancel()
	if err != nil {
		return err
	}
	if !resp.GetAccepted() {
		return fmt.Errorf("cursor request rejected: %s", resp.GetError())
	}
	startCursor := badger.WALCursor{}
	if c := resp.GetCursor(); c != nil {
		startCursor = badger.WALCursor{Fid: c.Fid, Offset: c.Offset, Len: c.Len}
	}
	for {
		err := r.backfillFromCursor(epoch, p, startCursor)
		if err == nil {
			log.Printf("recovery sync done: leader=%d peer=%d epoch=%d duration=%s", r.cfg.NodeID, p.ID, epoch, time.Since(syncStarted).Truncate(time.Millisecond))
			return nil
		}
		if !errors.Is(err, badger.ErrWALCursorCompacted) {
			log.Printf("recovery sync failed: leader=%d peer=%d epoch=%d duration=%s err=%v", r.cfg.NodeID, p.ID, epoch, time.Since(syncStarted).Truncate(time.Millisecond), err)
			return err
		}
		log.Printf("recovery compacted cursor detected: leader=%d peer=%d epoch=%d duration=%s", r.cfg.NodeID, p.ID, epoch, time.Since(syncStarted).Truncate(time.Millisecond))
		cur, recErr := r.recoverPeerFromSnapshot(epoch, p)
		if recErr != nil {
			log.Printf("recovery snapshot failed: leader=%d peer=%d epoch=%d duration=%s err=%v", r.cfg.NodeID, p.ID, epoch, time.Since(syncStarted).Truncate(time.Millisecond), recErr)
			return recErr
		}
		log.Printf("recovery snapshot installed: leader=%d peer=%d epoch=%d cursor=(%d,%d,%d) duration=%s", r.cfg.NodeID, p.ID, epoch, cur.Fid, cur.Offset, cur.Len, time.Since(syncStarted).Truncate(time.Millisecond))
		startCursor = cur
	}
}

func (r *Replication) applyFramesToPeer(ctx context.Context, p peerInfo, leaderID, epoch uint64, frames []*badger.WALFrame) error {
	if r.cfg.testHooks != nil && r.cfg.testHooks.onApplyFramesToPeer != nil {
		return r.cfg.testHooks.onApplyFramesToPeer(ctx, p, leaderID, epoch, frames)
	}
	pbFrames := make([]*badgerpb.WALFrame, 0, len(frames))
	for _, f := range frames {
		if f == nil {
			continue
		}
		pbFrames = append(pbFrames, f)
	}
	resp, err := r.applyFramesStream(ctx, p, &replicationpb.ApplyFramesRequest{
		LeaderId: leaderID,
		Epoch:    epoch,
		Frames:   pbFrames,
	})
	if err != nil {
		r.closePeerStream(p.ID)
		if r.shouldTriggerRecovery(err) {
			r.triggerPeerRecovery(epoch, p, err)
		}
		return err
	}
	if !resp.GetAccepted() {
		r.closePeerStream(p.ID)
		err := fmt.Errorf("apply rejected by %d: %s", p.ID, resp.GetError())
		if r.shouldTriggerRecovery(err) {
			r.triggerPeerRecovery(epoch, p, err)
		}
		return err
	}
	return nil
}
