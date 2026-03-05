package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"go.linka.cloud/badger/v4"

	"go.linka.cloud/badger/replication/pb"
)

func (r *Replication) backfillFromCursor(epoch uint64, p peerInfo, start badger.WALCursor) error {
	batch := make([]*badger.WALFrame, 0, r.cfg.BatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ctx, cancel := context.WithTimeout(r.ctx, r.cfg.ApplyTimeout)
		err := r.applyFramesToPeer(ctx, p, r.cfg.NodeID, epoch, batch)
		cancel()
		if err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	err := r.db.StreamWAL(r.ctx, badger.WALStreamOptions{StartCursor: start}, func(f *badger.WALFrame) error {
		batch = append(batch, f.CloneVT())
		if len(batch) >= r.cfg.BatchSize || f.Type == badger.WALFrameTxnCommit || f.Type == badger.WALFrameCheckpoint {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func (r *Replication) recoverPeerFromSnapshot(epoch uint64, p peerInfo) (badger.WALCursor, error) {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	started := time.Now()
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.ApplyTimeout*8)
	defer cancel()
	pc, err := r.peerClientFor(ctx, p)
	if err != nil {
		return badger.WALCursor{}, err
	}
	stream, err := pc.client.InstallSnapshot(ctx)
	if err != nil {
		return badger.WALCursor{}, err
	}

	_, applied := r.db.WALLSN()
	chunkSize := r.cfg.SnapshotChunkSize
	if chunkSize <= 0 {
		chunkSize = 1 << 20
	}
	if r.cfg.testHooks != nil && r.cfg.testHooks.onBeforeSnapshotReset != nil {
		if err := r.cfg.testHooks.onBeforeSnapshotReset(epoch, p); err != nil {
			return badger.WALCursor{}, err
		}
	}

	if err := stream.Send(&replicationpb.InstallSnapshotRequest{LeaderId: r.cfg.NodeID, Epoch: epoch, Reset_: true}); err != nil {
		return badger.WALCursor{}, err
	}

	pr, pw := io.Pipe()
	backupErrCh := make(chan error, 1)
	var backedUp uint64
	backupDone := false
	go func() {
		<-ctx.Done()
		_ = pr.CloseWithError(ctx.Err())
		_ = pw.CloseWithError(ctx.Err())
	}()
	defer func() {
		if backupDone {
			return
		}
		_ = pr.CloseWithError(errors.New("snapshot stream aborted"))
		_ = pw.CloseWithError(errors.New("snapshot stream aborted"))
		select {
		case <-backupErrCh:
		default:
		}
	}()
	go func() {
		n, berr := r.db.Backup(pw, 0)
		backedUp = n
		if berr != nil {
			_ = pw.CloseWithError(berr)
			backupErrCh <- berr
			return
		}
		backupErrCh <- pw.Close()
	}()

	buf := make([]byte, chunkSize)
	for {
		n, readErr := pr.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if r.cfg.testHooks != nil && r.cfg.testHooks.onBeforeSnapshotSend != nil {
				if err := r.cfg.testHooks.onBeforeSnapshotSend(epoch, p, chunk); err != nil {
					return badger.WALCursor{}, err
				}
			}
			if err := stream.Send(&replicationpb.InstallSnapshotRequest{LeaderId: r.cfg.NodeID, Epoch: epoch, Chunk: chunk}); err != nil {
				return badger.WALCursor{}, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return badger.WALCursor{}, readErr
		}
	}
	if berr := <-backupErrCh; berr != nil {
		return badger.WALCursor{}, berr
	}
	backupDone = true

	if r.cfg.testHooks != nil && r.cfg.testHooks.onBeforeSnapshotDone != nil {
		if err := r.cfg.testHooks.onBeforeSnapshotDone(epoch, p, applied); err != nil {
			return badger.WALCursor{}, err
		}
	}
	if err := stream.Send(&replicationpb.InstallSnapshotRequest{LeaderId: r.cfg.NodeID, Epoch: epoch, Done: true, Cursor: pbCursorFromWALCursor(applied)}); err != nil {
		return badger.WALCursor{}, err
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return badger.WALCursor{}, err
	}
	if !resp.GetAccepted() {
		return badger.WALCursor{}, fmt.Errorf("snapshot install rejected by %d: %s", p.ID, resp.GetError())
	}
	if c := resp.GetCursor(); c != nil {
		log.Printf("recovery snapshot transfer done: leader=%d peer=%d epoch=%d entries=%d duration=%s", r.cfg.NodeID, p.ID, epoch, backedUp, time.Since(started).Truncate(time.Millisecond))
		return badger.WALCursor{Fid: c.Fid, Offset: c.Offset, Len: c.Len}, nil
	}
	log.Printf("recovery snapshot transfer done: leader=%d peer=%d epoch=%d entries=%d duration=%s", r.cfg.NodeID, p.ID, epoch, backedUp, time.Since(started).Truncate(time.Millisecond))
	return applied, nil
}
