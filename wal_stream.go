package badger

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

const WALFrameVersion uint32 = 1

type WALFrameType = pb.WALFrame_Type

const (
	WALFrameUnknown    = pb.WALFrame_UNKNOWN
	WALFrameEntry      = pb.WALFrame_ENTRY
	WALFrameTxnIntent  = pb.WALFrame_TXN_INTENT
	WALFrameTxnCommit  = pb.WALFrame_TXN_COMMIT
	WALFrameCheckpoint = pb.WALFrame_CHECKPOINT
)

type WALFrame = pb.WALFrame

// WALStreamOptions controls optional stream behavior.
type WALStreamOptions struct {
	// StartCursor is the inclusive cursor to start from. Zero means from the beginning.
	StartCursor WALCursor
	// Continuous keeps streaming frames as they arrive.
	// Default false performs a one-shot scan and returns.
	Continuous bool
	// StopCursor bounds streaming to this cursor (inclusive). Zero means unbounded.
	StopCursor WALCursor
}

// StreamWAL streams WAL frames with optional one-shot/continuous behavior and
// optional start/stop cursors.
func (db *DB) StreamWAL(ctx context.Context, opts WALStreamOptions, fn func(*WALFrame) error) error {
	if !db.opt.EnableWAL {
		return nil
	}
	if db.IsClosed() {
		return ErrDBClosed
	}
	if fn == nil {
		return ErrInvalidRequest
	}
	if ctx == nil {
		ctx = context.Background()
	}
	start := opts.StartCursor
	minRetain := db.wal.minRetainCursorSnapshot()
	if !minRetain.IsZero() && (start.IsZero() || start.Less(minRetain)) {
		return fmt.Errorf("%w: requested=(fid=%d offset=%d len=%d) min_retain=(fid=%d offset=%d len=%d)",
			ErrWALCursorCompacted,
			start.Fid, start.Offset, start.Len,
			minRetain.Fid, minRetain.Offset, minRetain.Len,
		)
	}
	return db.wal.stream(ctx, valuePointerFromWALCursor(start), valuePointerFromWALCursor(opts.StopCursor), opts.Continuous, fn)
}

func walFrameType(e Entry) WALFrameType {
	if isWalCheckpointEntry(e) {
		return WALFrameCheckpoint
	}
	if e.meta&bitFinTxn > 0 {
		return WALFrameTxnCommit
	}
	if e.meta&bitTxnIntent > 0 {
		return WALFrameTxnIntent
	}
	return WALFrameEntry
}

func walCursorToPB(c WALCursor) *pb.WALCursor {
	if c.IsZero() {
		return nil
	}
	return &pb.WALCursor{Fid: c.Fid, Offset: c.Offset, Len: c.Len}
}

func walCursorFromPB(c *pb.WALCursor) WALCursor {
	if c == nil {
		return WALCursor{}
	}
	return WALCursor{Fid: c.Fid, Offset: c.Offset, Len: c.Len}
}

func (l *wal) stream(ctx context.Context, start valuePointer, stop valuePointer, continuous bool, fn func(*WALFrame) error) error {
	if !l.enabled() || fn == nil {
		return nil
	}
	if !stop.IsZero() && stop.Less(start) {
		return nil
	}
	subID, subCh, tail := l.addStreamSubscriber()
	defer l.removeStreamSubscriber(subID)

	scanEnd := tail
	if !stop.IsZero() && tail.Less(stop) {
		scanEnd = tail
	} else if !stop.IsZero() {
		scanEnd = stop
	}
	next, err := l.scanFrom(ctx, start, scanEnd, fn)
	if err != nil {
		return err
	}
	start = next
	if !stop.IsZero() && !start.Less(stop) {
		return nil
	}
	if !continuous {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame, ok := <-subCh:
			if !ok {
				if l.db.IsClosed() {
					return ErrDBClosed
				}
				return ErrRejected
			}
			if frame == nil {
				continue
			}
			vp := valuePointerFromWALCursor(walCursorFromPB(frame.Lsn))
			if vp.IsZero() || vp.Less(start) {
				continue
			}
			if !stop.IsZero() && stop.Less(vp) {
				return nil
			}
			if err := fn(frame); err != nil {
				return err
			}
			start = valuePointer{Fid: vp.Fid, Offset: vp.Offset + vp.Len}
			if !stop.IsZero() && !start.Less(stop) {
				return nil
			}
		}
	}
}

func (l *wal) scanFrom(ctx context.Context, start valuePointer, end valuePointer, fn func(*WALFrame) error) (valuePointer, error) {
	l.mu.RLock()
	fids := l.sortedFids()
	files := make([]*logFile, 0, len(fids))
	for _, fid := range fids {
		if start.Fid > 0 && fid < start.Fid {
			continue
		}
		files = append(files, l.files[fid])
	}
	l.mu.RUnlock()

	next := start
	for _, lf := range files {
		if err := ctx.Err(); err != nil {
			return next, err
		}
		if lf == nil {
			continue
		}
		offset := uint32(vlogHeaderSize)
		if start.Fid > 0 && lf.fid == start.Fid && start.Offset > 0 {
			offset = start.Offset
		}
		lf.lock.RLock()
		read := safeRead{
			k:  make([]byte, 10),
			v:  make([]byte, 10),
			lf: lf,
		}
		read.recordOffset = offset
		for {
			if err := ctx.Err(); err != nil {
				lf.lock.RUnlock()
				return next, err
			}
			e, err := read.Entry(io.Reader(lf.NewReader(int(read.recordOffset))))
			if err == io.EOF || err == io.ErrUnexpectedEOF || err == errTruncate {
				break
			}
			if err != nil {
				lf.lock.RUnlock()
				return next, err
			}
			if e == nil || e.isZero() {
				break
			}
			vp := valuePointer{Fid: lf.fid, Offset: e.offset}
			vp.Len = uint32(int(e.hlen) + len(e.Key) + len(e.Value) + crc32.Size)
			read.recordOffset += vp.Len
			if !end.IsZero() && end.Less(vp) {
				lf.lock.RUnlock()
				return next, nil
			}
			next = valuePointer{Fid: lf.fid, Offset: read.recordOffset}
			frame := newWALFrameFromEntry(*e, vp)
			if frame == nil {
				continue
			}
			if frame.Type == WALFrameCheckpoint {
				mr := l.minRetainCursorSnapshot()
				if !mr.IsZero() {
					frame.MinRetainCursor = walCursorToPB(mr)
				}
			}
			if err := fn(frame); err != nil {
				lf.lock.RUnlock()
				return next, err
			}
		}
		lf.lock.RUnlock()
	}
	return next, nil
}

func newWALFrameFromEntry(e Entry, vp valuePointer) *WALFrame {
	return &WALFrame{
		Version:   WALFrameVersion,
		Type:      walFrameType(e),
		Lsn:       walCursorToPB(walCursorFromValuePointer(vp)),
		Key:       y.SafeCopy(nil, e.Key),
		Value:     y.SafeCopy(nil, e.Value),
		Meta:      []byte{e.meta},
		UserMeta:  []byte{e.UserMeta},
		ExpiresAt: e.ExpiresAt,
	}
}
