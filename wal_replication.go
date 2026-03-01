package badger

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

var walReplicationCursorPrefix = []byte("!badger!wal-replication-cursor")
var walReplicationCursorKey = []byte("!badger!wal-replication-cursor")

type ReplicationRole int32

const (
	ReplicationRolePrimary ReplicationRole = iota
	ReplicationRoleReplica
)

func (r ReplicationRole) String() string {
	switch r {
	case ReplicationRolePrimary:
		return "primary"
	case ReplicationRoleReplica:
		return "replica"
	default:
		return "unknown"
	}
}

// ApplyOptions controls how frames are applied on replicas.
type ApplyOptions struct {
	// ReplicaID stores the apply cursor under a replica-specific key.
	// Empty ReplicaID uses the default cursor key.
	ReplicaID string
	// StrictCursorContinuity verifies that newly applied frames are contiguous
	// with the previous applied cursor and with each other.
	StrictCursorContinuity bool
}

// WALApplyOptions is an alias kept for compatibility.
type WALApplyOptions = ApplyOptions

type replTxnIntent struct {
	lsn WALCursor
	e   *Entry
}

// WALReplication holds WAL replication state and primitives.
type WALReplication struct {
	db *DB

	mu sync.Mutex

	role atomic.Int32
	// namespace -> txnID -> intents
	pendingIntents map[string]map[uint64][]replTxnIntent
}

// MarshalFrame encodes a WAL frame using vtprotobuf.
func (r *WALReplication) MarshalFrame(frame *WALFrame) ([]byte, error) {
	if frame == nil {
		return nil, errors.New("nil WAL frame")
	}
	return frame.MarshalVT()
}

// UnmarshalFrame decodes a WAL frame using vtprotobuf.
func (r *WALReplication) UnmarshalFrame(data []byte) (*WALFrame, error) {
	frame := &WALFrame{}
	if err := frame.UnmarshalVT(data); err != nil {
		return nil, err
	}
	return frame, nil
}

func (r *WALReplication) init(db *DB) {
	r.db = db
	r.role.Store(int32(ReplicationRolePrimary))
	r.pendingIntents = make(map[string]map[uint64][]replTxnIntent)
}

// Role returns the current runtime role.
func (r *WALReplication) Role() ReplicationRole {
	if r == nil {
		return ReplicationRolePrimary
	}
	return ReplicationRole(r.role.Load())
}

func (r *WALReplication) isReplica() bool {
	return r.Role() == ReplicationRoleReplica
}

// SetRole switches runtime role without reopening the DB.
func (r *WALReplication) SetRole(role ReplicationRole) error {
	if r == nil || r.db == nil {
		return ErrInvalidRequest
	}
	switch role {
	case ReplicationRolePrimary, ReplicationRoleReplica:
		r.role.Store(int32(role))
		return nil
	default:
		return ErrInvalidRequest
	}
}

// SetSafeLSN updates the source-side pruning lower bound.
func (r *WALReplication) SetSafeLSN(lsn WALCursor) {
	if !r.enabled() {
		return
	}
	r.db.wal.setReplicationSafeLSN(valuePointerFromWALCursor(lsn))
}

// SafeLSN returns the source-side WAL pruning lower bound.
func (r *WALReplication) SafeLSN() WALCursor {
	if !r.enabled() {
		return WALCursor{}
	}
	return walCursorFromValuePointer(r.db.wal.replicationSafeLSNValue())
}

// Replication returns the WAL replication primitive set.
func (db *DB) Replication() *WALReplication {
	return &db.replication
}

// Cursor returns the latest replicated WAL cursor applied to this DB.
func (r *WALReplication) Cursor() (WALCursor, error) {
	return r.CursorFor("")
}

// CursorFor returns the latest replicated WAL cursor for a replica.
func (r *WALReplication) CursorFor(replicaID string) (WALCursor, error) {
	if !r.enabled() {
		return WALCursor{}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loadCursorLocked(walReplicationCursorUserKey(replicaID))
}

// SetCursor persists the replication apply cursor.
func (r *WALReplication) SetCursor(cursor WALCursor) error {
	return r.SetCursorFor("", cursor)
}

// SetCursorFor persists the replication apply cursor for a replica.
func (r *WALReplication) SetCursorFor(replicaID string, cursor WALCursor) error {
	if !r.enabled() || cursor.IsZero() {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := walReplicationCursorUserKey(replicaID)
	current, err := r.loadCursorLocked(key)
	if err != nil {
		return err
	}
	if !current.Less(cursor) {
		return nil
	}
	return r.persistCursorLocked(key, cursor)
}

// ApplyFrame applies a single WAL frame using exact WAL semantics.
func (r *WALReplication) ApplyFrame(frame *WALFrame) (WALCursor, error) {
	return r.ApplyFrameFor("", frame)
}

// ApplyFrameBytes decodes and applies a single vtprotobuf-encoded WAL frame.
func (r *WALReplication) ApplyFrameBytes(data []byte) (WALCursor, error) {
	frame, err := r.UnmarshalFrame(data)
	if err != nil {
		return WALCursor{}, err
	}
	return r.ApplyFrame(frame)
}

// ApplyFrameFor applies a single WAL frame for a replica cursor namespace.
func (r *WALReplication) ApplyFrameFor(replicaID string, frame *WALFrame) (WALCursor, error) {
	return r.ApplyFramesFor(replicaID, []*WALFrame{frame})
}

// ApplyFrameBytesFor decodes and applies one frame for the given replica namespace.
func (r *WALReplication) ApplyFrameBytesFor(replicaID string, data []byte) (WALCursor, error) {
	frame, err := r.UnmarshalFrame(data)
	if err != nil {
		return WALCursor{}, err
	}
	return r.ApplyFrameFor(replicaID, frame)
}

// ApplyFrames applies WAL frames in order and updates the local apply cursor.
func (r *WALReplication) ApplyFrames(frames []*WALFrame) (WALCursor, error) {
	return r.ApplyFramesWithOptions(frames, ApplyOptions{})
}

// ApplyFramesFor applies WAL frames in order and updates a replica cursor.
func (r *WALReplication) ApplyFramesFor(replicaID string, frames []*WALFrame) (WALCursor, error) {
	return r.ApplyFramesWithOptions(frames, ApplyOptions{ReplicaID: replicaID})
}

// ApplyFramesWithOptions applies WAL frames with custom apply options.
func (r *WALReplication) ApplyFramesWithOptions(frames []*WALFrame, opts ApplyOptions) (WALCursor, error) {
	return r.applyFrames(walReplicationCursorUserKey(opts.ReplicaID), frames, opts)
}

// ApplyStream applies frames from a streaming source callback.
func (r *WALReplication) ApplyStream(next func(func(*WALFrame) error) error) (WALCursor, error) {
	return r.ApplyStreamWithOptions(next, ApplyOptions{})
}

// ApplyStreamWithOptions applies stream-delivered frames with custom options.
func (r *WALReplication) ApplyStreamWithOptions(next func(func(*WALFrame) error) error, opts ApplyOptions) (WALCursor, error) {
	if next == nil {
		return WALCursor{}, errors.New("nil WAL stream callback")
	}
	var last WALCursor
	err := next(func(f *WALFrame) error {
		cur, err := r.ApplyFramesWithOptions([]*WALFrame{f}, opts)
		if err != nil {
			return err
		}
		last = cur
		return nil
	})
	if err != nil {
		return last, err
	}
	if !last.IsZero() {
		return last, nil
	}
	return r.CursorFor(opts.ReplicaID)
}

func (r *WALReplication) enabled() bool {
	return r != nil && r.db != nil && r.db.opt.EnableWAL
}

func (r *WALReplication) applyFrames(cursorUserKey []byte, frames []*WALFrame, opts ApplyOptions) (WALCursor, error) {
	if len(cursorUserKey) == 0 {
		return WALCursor{}, errors.New("empty WAL replication cursor key")
	}
	if !r.enabled() {
		return WALCursor{}, ErrInvalidRequest
	}
	if !r.isReplica() {
		return WALCursor{}, ErrPrimaryRoleApply
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	current, err := r.loadCursorLocked(cursorUserKey)
	if err != nil {
		return WALCursor{}, err
	}
	if len(frames) == 0 {
		return current, nil
	}

	ns := string(cursorUserKey)
	if r.pendingIntents[ns] == nil {
		r.pendingIntents[ns] = make(map[uint64][]replTxnIntent)
	}

	batch := make([]*Entry, 0, len(frames))
	lastSeen := current
	strictPrev := current
	lastApplied := current
	for _, frame := range frames {
		if frame == nil {
			continue
		}
		if frame.Version != WALFrameVersion {
			return current, fmt.Errorf("unsupported WAL frame version %d", frame.Version)
		}
		if frame.Type == WALFrameUnknown {
			return current, errors.New("unknown WAL frame type")
		}
		lsn := walCursorFromPB(frame.Lsn)
		if lsn.IsZero() {
			return current, errors.New("missing WAL frame cursor")
		}
		if !current.Less(lsn) {
			continue
		}
		if lsn.Less(lastSeen) {
			return current, fmt.Errorf("out of order WAL frame cursor: current=%+v previous=%+v", lsn, lastSeen)
		}
		if lsn.Equal(lastSeen) {
			continue
		}
		lastSeen = lsn
		if opts.StrictCursorContinuity && !walCursorFollows(strictPrev, lsn) {
			return current, fmt.Errorf("gap detected in WAL frame cursors: previous=%+v current=%+v", strictPrev, lsn)
		}
		strictPrev = lsn

		entries, advance, err := r.frameToEntriesLocked(ns, frame)
		if err != nil {
			return current, err
		}
		if len(entries) > 0 {
			batch = append(batch, entries...)
		}
		if advance {
			lastApplied = lsn
		}
	}

	if len(batch) > 0 {
		if err := r.db.applyWALEntries(batch); err != nil {
			return current, err
		}
		var maxVersion uint64
		for _, e := range batch {
			if v := y.ParseTs(e.Key); v > maxVersion {
				maxVersion = v
			}
		}
		r.advanceOracleForAppliedVersion(maxVersion)
	}
	if !current.Less(lastApplied) {
		return current, nil
	}
	if err := r.persistCursorLocked(cursorUserKey, lastApplied); err != nil {
		return current, err
	}
	return lastApplied, nil
}

func (r *WALReplication) frameToEntriesLocked(namespace string, frame *WALFrame) ([]*Entry, bool, error) {
	if bytes.HasPrefix(frame.Key, walReplicationCursorPrefix) {
		return nil, false, nil
	}
	if frame.Type == WALFrameCheckpoint || bytes.Equal(frame.Key, walCheckpointKey) {
		return nil, true, nil
	}
	if len(frame.Key) == 0 {
		return nil, false, errors.New("missing WAL frame key")
	}

	if frame.Type == WALFrameTxnIntent {
		txnID, ok := parseTxnIntentKey(frame.Key)
		if !ok {
			return nil, false, errors.New("invalid WAL txn intent key")
		}
		intent, ok := decodeTxnIntentValue(frame.Value)
		if !ok {
			return nil, false, errors.New("invalid WAL txn intent value")
		}
		intentCopy := &Entry{
			Key:       y.SafeCopy(nil, intent.Key),
			Value:     y.SafeCopy(nil, intent.Value),
			UserMeta:  intent.UserMeta,
			ExpiresAt: intent.ExpiresAt,
			version:   intent.version,
			meta:      intent.meta,
		}
		recs := r.pendingIntents[namespace][txnID]
		for _, rec := range recs {
			if rec.lsn.Equal(walCursorFromPB(frame.Lsn)) {
				return nil, false, nil
			}
		}
		r.pendingIntents[namespace][txnID] = append(r.pendingIntents[namespace][txnID], replTxnIntent{lsn: walCursorFromPB(frame.Lsn), e: intentCopy})
		return nil, false, nil
	}

	if frame.Type == WALFrameTxnCommit {
		commitTs, txnID, ok := decodeTxnCommitValue(frame.Value)
		if !ok {
			return nil, false, errors.New("invalid WAL txn commit value")
		}
		if txnID == 0 {
			return nil, true, nil
		}
		recs := r.pendingIntents[namespace][txnID]
		if len(recs) == 0 {
			return nil, true, nil
		}
		entries := make([]*Entry, 0, len(recs))
		for _, rec := range recs {
			ie := rec.e
			ts := ie.version
			if ts == 0 {
				ts = commitTs
			}
			if ts == 0 {
				continue
			}
			entries = append(entries, &Entry{
				Key:       y.KeyWithTs(y.SafeCopy(nil, ie.Key), ts),
				Value:     y.SafeCopy(nil, ie.Value),
				UserMeta:  ie.UserMeta,
				ExpiresAt: ie.ExpiresAt,
				meta:      ie.meta,
			})
		}
		delete(r.pendingIntents[namespace], txnID)
		return entries, true, nil
	}

	meta := frameMetaByte(frame.Meta) &^ bitWALPointer
	if frame.Type != WALFrameEntry {
		return nil, false, fmt.Errorf("unsupported WAL frame type %d", frame.Type)
	}

	entry := &Entry{
		Key:       y.SafeCopy(nil, frame.Key),
		Value:     y.SafeCopy(nil, frame.Value),
		UserMeta:  frameMetaByte(frame.UserMeta),
		ExpiresAt: frame.ExpiresAt,
		meta:      meta,
	}
	return []*Entry{entry}, true, nil
}

func frameMetaByte(v []byte) byte {
	if len(v) == 0 {
		return 0
	}
	return v[0]
}

func (r *WALReplication) loadCursorLocked(cursorUserKey []byte) (WALCursor, error) {
	vs, err := r.db.get(y.KeyWithTs(cursorUserKey, math.MaxUint64))
	if err != nil {
		if err == ErrKeyNotFound {
			return WALCursor{}, nil
		}
		return WALCursor{}, err
	}
	if vs.Meta == 0 && vs.Value == nil {
		return WALCursor{}, nil
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return WALCursor{}, nil
	}
	var cursorPB pb.WALCursor
	if err := cursorPB.UnmarshalVT(vs.Value); err != nil {
		return WALCursor{}, err
	}
	return walCursorFromPB(&cursorPB), nil
}

func (r *WALReplication) persistCursorLocked(cursorUserKey []byte, cursor WALCursor) error {
	if cursor.IsZero() {
		return nil
	}
	entry := &Entry{
		Key:   y.KeyWithTs(cursorUserKey, walCursorVersion(cursor)),
		Value: nil,
	}
	v, err := walCursorToPB(cursor).MarshalVT()
	if err != nil {
		return err
	}
	entry.Value = v
	return r.db.applyWALEntries([]*Entry{entry})
}

func walReplicationCursorUserKey(replicaID string) []byte {
	if replicaID == "" {
		return append([]byte{}, walReplicationCursorKey...)
	}
	out := make([]byte, len(walReplicationCursorPrefix)+1+len(replicaID))
	copy(out, walReplicationCursorPrefix)
	out[len(walReplicationCursorPrefix)] = '/'
	copy(out[len(walReplicationCursorPrefix)+1:], []byte(replicaID))
	return out
}

func walCursorFollows(prev WALCursor, next WALCursor) bool {
	if prev.IsZero() || next.IsZero() {
		return true
	}
	if next.Fid < prev.Fid {
		return false
	}
	if next.Fid == prev.Fid {
		return next.Offset == prev.Offset+prev.Len
	}
	if next.Fid != prev.Fid+1 {
		return false
	}
	return next.Offset == uint32(vlogHeaderSize)
}

func (r *WALReplication) advanceOracleForAppliedVersion(maxVersion uint64) {
	if maxVersion == 0 || r.db.orc == nil {
		return
	}
	orc := r.db.orc
	orc.Lock()
	if maxVersion >= orc.nextTxnTs {
		orc.nextTxnTs = maxVersion + 1
	}
	orc.Unlock()
	if !orc.isManaged {
		orc.txnMark.Done(maxVersion)
	}
}
