package badger

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"sync"

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

// WALStreamOptions controls source-side WAL frame streaming.
type WALStreamOptions struct {
	// DurableOnly limits output to the durable cursor snapshot captured at stream start.
	DurableOnly bool
}

// ReplicationAck is a transport-agnostic acknowledgment of applied source WAL.
type ReplicationAck struct {
	ReplicaID string
	Cursor    WALCursor
}

// FrameStream defines a transport-neutral frame stream contract.
type FrameStream interface {
	StreamFrom(ctx context.Context, from WALCursor, emit func(*WALFrame) error) error
}

// AckStream defines a transport-neutral ack stream contract.
type AckStream interface {
	StreamAcks(ctx context.Context, emit func(ReplicationAck) error) error
}

// AckSink defines a transport-neutral ack publishing contract.
type AckSink interface {
	PublishAck(ctx context.Context, ack ReplicationAck) error
}

// CursorCheckpointStore defines durable cursor checkpoint persistence.
type CursorCheckpointStore interface {
	LoadCursor(ctx context.Context, replicaID string) (WALCursor, error)
	SaveCursor(ctx context.Context, replicaID string, cursor WALCursor) error
}

// DBFrameStream adapts DB WAL streaming to FrameStream.
type DBFrameStream struct {
	DB      *DB
	Options WALStreamOptions
}

// StreamFrom emits frames from the DB WAL stream.
func (s DBFrameStream) StreamFrom(ctx context.Context, from WALCursor, emit func(*WALFrame) error) error {
	if s.DB == nil {
		return ErrInvalidRequest
	}
	return s.DB.StreamWALFromOptions(from, s.Options, func(f *WALFrame) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return emit(f)
	})
}

// ReplicationCheckpointStore adapts WALReplication cursor methods.
type ReplicationCheckpointStore struct {
	Replication *WALReplication
}

// LoadCursor loads a replica cursor from WALReplication.
func (s ReplicationCheckpointStore) LoadCursor(_ context.Context, replicaID string) (WALCursor, error) {
	if s.Replication == nil {
		return WALCursor{}, ErrInvalidRequest
	}
	return s.Replication.CursorFor(replicaID)
}

// SaveCursor stores a replica cursor via WALReplication.
func (s ReplicationCheckpointStore) SaveCursor(_ context.Context, replicaID string, cursor WALCursor) error {
	if s.Replication == nil {
		return ErrInvalidRequest
	}
	return s.Replication.SetCursorFor(replicaID, cursor)
}

// MinAckTracker tracks per-replica acknowledgements and computes min ack.
type MinAckTracker struct {
	mu   sync.RWMutex
	acks map[string]WALCursor
}

// NewMinAckTracker creates a new in-memory ack tracker.
func NewMinAckTracker() *MinAckTracker {
	return &MinAckTracker{acks: make(map[string]WALCursor)}
}

// Update stores an ack and returns the minimum ack across replicas.
func (t *MinAckTracker) Update(ack ReplicationAck) WALCursor {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.acks == nil {
		t.acks = make(map[string]WALCursor)
	}
	if ack.ReplicaID == "" || ack.Cursor.IsZero() {
		return t.minLocked()
	}
	if cur, ok := t.acks[ack.ReplicaID]; ok && !cur.Less(ack.Cursor) {
		return t.minLocked()
	}
	t.acks[ack.ReplicaID] = ack.Cursor
	return t.minLocked()
}

// Min returns the minimum ack across replicas.
func (t *MinAckTracker) Min() WALCursor {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.minLocked()
}

func (t *MinAckTracker) minLocked() WALCursor {
	if len(t.acks) == 0 {
		return WALCursor{}
	}
	first := true
	min := WALCursor{}
	for _, c := range t.acks {
		if first {
			min = c
			first = false
			continue
		}
		if c.Less(min) {
			min = c
		}
	}
	return min
}

// StreamWALFrom streams WAL frames from the provided LSN (inclusive).
func (db *DB) StreamWALFrom(start WALCursor, fn func(*WALFrame) error) error {
	return db.StreamWALFromOptions(start, WALStreamOptions{}, fn)
}

// StreamWALFromOptions streams WAL frames from the provided cursor (inclusive).
func (db *DB) StreamWALFromOptions(start WALCursor, opts WALStreamOptions, fn func(*WALFrame) error) error {
	if !db.opt.EnableWAL {
		return nil
	}
	end := WALCursor{}
	if opts.DurableOnly {
		durable, _ := db.WALLSN()
		if durable.IsZero() {
			return nil
		}
		end = durable
	}
	return db.wal.streamFrom(valuePointerFromWALCursor(start), valuePointerFromWALCursor(end), fn)
}

func classifyWALFrame(e Entry) WALFrameType {
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

func (l *wal) streamFrom(start valuePointer, end valuePointer, fn func(*WALFrame) error) error {
	if !l.enabled() || fn == nil {
		return nil
	}

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

	for _, lf := range files {
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
			e, err := read.Entry(io.Reader(lf.NewReader(int(read.recordOffset))))
			if err == io.EOF || err == io.ErrUnexpectedEOF || err == errTruncate {
				break
			}
			if err != nil {
				lf.lock.RUnlock()
				return err
			}
			if e == nil || e.isZero() {
				break
			}
			vp := valuePointer{Fid: lf.fid, Offset: e.offset}
			vp.Len = uint32(int(e.hlen) + len(e.Key) + len(e.Value) + crc32.Size)
			read.recordOffset += vp.Len
			if bytes.HasPrefix(e.Key, walReplicationCursorPrefix) {
				continue
			}
			if !end.IsZero() && end.Less(vp) {
				lf.lock.RUnlock()
				return nil
			}
			frame := &WALFrame{
				Version:   WALFrameVersion,
				Type:      classifyWALFrame(*e),
				Lsn:       walCursorToPB(walCursorFromValuePointer(vp)),
				Key:       y.SafeCopy(nil, e.Key),
				Value:     y.SafeCopy(nil, e.Value),
				Meta:      []byte{e.meta},
				UserMeta:  []byte{e.UserMeta},
				ExpiresAt: e.ExpiresAt,
			}
			if err := fn(frame); err != nil {
				lf.lock.RUnlock()
				return err
			}
		}
		lf.lock.RUnlock()
	}
	return nil
}
