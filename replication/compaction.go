package replication

import (
	"context"
	"errors"
	"log"
	"time"

	"go.linka.cloud/badger/v4"
)

func (r *Replication) compactionLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.CompactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if r.Role() != badger.ReplicationRolePrimary {
				continue
			}
			r.runWALGC()
		case <-r.ctx.Done():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (r *Replication) runWALGC() {
	ratio := r.cfg.WALGCRatio
	if ratio <= 0 {
		ratio = 0.01
	}
	maxRuns := r.cfg.WALGCMaxRuns
	if maxRuns <= 0 {
		maxRuns = 8
	}
	start := time.Now()
	epoch := r.currentEpoch()
	log.Printf("gc cycle start: leader=%d epoch=%d ratio=%.4f max_runs=%d", r.cfg.NodeID, epoch, ratio, maxRuns)
	runs := 0
	for i := 0; i < maxRuns; i++ {
		err := r.db.RunWALGC(ratio)
		if errors.Is(err, badger.ErrNoRewrite) {
			if runs == 0 {
				log.Printf("gc cycle no-op: leader=%d epoch=%d duration=%s", r.cfg.NodeID, epoch, time.Since(start).Truncate(time.Millisecond))
			}
			break
		}
		if err != nil {
			log.Printf("gc cycle stopped: leader=%d epoch=%d runs=%d duration=%s err=%v", r.cfg.NodeID, epoch, runs, time.Since(start).Truncate(time.Millisecond), err)
			break
		}
		runs++
		log.Printf("gc run complete: leader=%d epoch=%d run=%d/%d elapsed=%s", r.cfg.NodeID, epoch, runs, maxRuns, time.Since(start).Truncate(time.Millisecond))
	}
	if runs > 0 {
		log.Printf("gc cycle complete: leader=%d epoch=%d runs=%d ratio=%.4f duration=%s", r.cfg.NodeID, epoch, runs, ratio, time.Since(start).Truncate(time.Millisecond))
	}
}

func (r *Replication) RunWALGC() {
	r.runWALGC()
}
