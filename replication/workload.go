package replication

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"go.linka.cloud/badger/v4"
)

func (r *Replication) runWorkload(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-r.ctx.Done():
			return
		case <-ctx.Done():
			return
		}
		if r.Role() != badger.ReplicationRolePrimary {
			continue
		}
		start := time.Now()
		err := r.runWriteWorkload()
		if err != nil {
			log.Printf("workload write failed: %v", err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("workload writes committed by leader %d: writes=%d duration=%s", r.cfg.NodeID, r.cfg.WorkloadWrites, time.Since(start).Truncate(time.Millisecond))
		if r.cfg.WorkloadDeletes > 0 {
			delStart := time.Now()
			if err := r.runDeleteWorkload(); err != nil {
				log.Printf("workload delete failed: %v", err)
				time.Sleep(time.Second)
				continue
			}
			log.Printf("workload deletes committed by leader %d: deletes=%d duration=%s", r.cfg.NodeID, r.cfg.WorkloadDeletes, time.Since(delStart).Truncate(time.Millisecond))
		}
		if r.Role() == badger.ReplicationRolePrimary {
			r.runWALGC()
		}
		return
	}
}

func (r *Replication) runWriteWorkload() error {
	const progressEvery = 100000
	return r.db.Update(func(txn *badger.Txn) error {
		s := time.Now()
		key := make([]byte, 0, 48)
		val := make([]byte, 0, 32)
		for i := 0; i < r.cfg.WorkloadWrites; i++ {
			key = key[:0]
			key = append(key, 'w', 'l', '-')
			key = strconv.AppendUint(key, r.cfg.NodeID, 10)
			key = append(key, '-')
			key = strconv.AppendInt(key, int64(i), 10)

			val = val[:0]
			val = append(val, 'v', '-')
			val = strconv.AppendInt(val, int64(i), 10)

			if err := txn.Set(key, val); err != nil {
				return err
			}
			if i := i + 1; i%progressEvery == 0 || i == r.cfg.WorkloadWrites {
				log.Printf("workload write progress: %d/%d duration=%s", i, r.cfg.WorkloadWrites, time.Since(s).Truncate(time.Millisecond))
				s = time.Now()
			}
		}
		return nil
	})
}

func (r *Replication) runDeleteWorkload() error {
	const progressEvery = 100000
	if r.cfg.WorkloadDeletes > r.cfg.WorkloadWrites {
		return fmt.Errorf("workload-deletes (%d) cannot exceed workload-writes (%d)", r.cfg.WorkloadDeletes, r.cfg.WorkloadWrites)
	}
	return r.db.Update(func(txn *badger.Txn) error {
		s := time.Now()
		key := make([]byte, 0, 48)
		for i := 0; i < r.cfg.WorkloadDeletes; i++ {
			key = key[:0]
			key = append(key, 'w', 'l', '-')
			key = strconv.AppendUint(key, r.cfg.NodeID, 10)
			key = append(key, '-')
			key = strconv.AppendInt(key, int64(i), 10)
			if err := txn.Delete(key); err != nil {
				return err
			}
			if i := i + 1; i%progressEvery == 0 || i == r.cfg.WorkloadDeletes {
				log.Printf("workload delete progress: %d/%d duration=%s", i, r.cfg.WorkloadDeletes, time.Since(s).Truncate(time.Millisecond))
				s = time.Now()
			}
		}
		return nil
	})
}
