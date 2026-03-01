package badger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.linka.cloud/badger/v4/pb"
	"go.linka.cloud/badger/v4/y"
)

const walReplicationCursorStateFile = "WAL-REPLICATION-CURSOR"

const (
	walReplicationCursorBatchSize = 256
	walReplicationCursorBatchWait = 200 * time.Millisecond
)

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

// Replication provides runtime role policy and WAL frame replication hooks.
type Replication interface {
	// Start initializes replication runtime state.
	//
	// Start must perform initialization only and return promptly. Any long-running
	// replication loops should be started in background goroutines that observe ctx.
	Start(ctx context.Context, db *DB) error
	Role() ReplicationRole
	Replicate(ctx context.Context, frame *WALFrame) error
}

type replTxnIntent struct {
	lsn    WALCursor
	e      *Entry
	walPtr valuePointer
}

type replicationCursorState struct {
	LSN WALCursor
}

func (r *WALReplication) cursorStateSnapshot() (replicationCursorState, error) {
	if !r.enabled() {
		return replicationCursorState{}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadCursorState(); err != nil {
		return replicationCursorState{}, err
	}
	return r.cursorState, nil
}

// WALReplication holds WAL replication state and primitives.
type WALReplication struct {
	db *DB

	mu sync.Mutex

	pendingIntents      map[uint64][]replTxnIntent
	pendingIntentSeen   map[uint64]map[WALCursor]struct{}
	lastMinRetainCursor WALCursor
	localSourceByFid    map[uint32]WALCursor

	cursorState       replicationCursorState
	cursorLoaded      bool
	cursorDirty       bool
	cursorDirtyCount  int
	lastCursorPersist time.Time
}

func (r *WALReplication) init(db *DB) {
	r.db = db
	r.pendingIntents = make(map[uint64][]replTxnIntent)
	r.pendingIntentSeen = make(map[uint64]map[WALCursor]struct{})
	r.localSourceByFid = make(map[uint32]WALCursor)
}

func (r *WALReplication) role() ReplicationRole {
	if r == nil {
		return ReplicationRolePrimary
	}
	if r.db == nil || r.db.opt.Replication == nil {
		return ReplicationRolePrimary
	}
	return r.db.opt.Replication.Role()
}

func (r *WALReplication) isReplica() bool {
	return r.role() == ReplicationRoleReplica
}

func (r *WALReplication) replicate(ctx context.Context, frame *WALFrame) error {
	if r == nil || r.db == nil || r.db.opt.Replication == nil || frame == nil {
		return nil
	}
	return r.db.opt.Replication.Replicate(ctx, frame)
}

// Replication returns the WAL replication primitive set.
func (db *DB) Replication() *WALReplication {
	return &db.replication
}

// Cursor returns the latest replicated WAL cursor applied to this DB.
func (r *WALReplication) Cursor() (WALCursor, error) {
	if !r.enabled() {
		return WALCursor{}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadCursorState(); err != nil {
		return WALCursor{}, err
	}
	return r.cursorState.LSN, nil
}

// SetCursor persists the replication apply cursor.
func (r *WALReplication) SetCursor(cursor WALCursor) error {
	if !r.enabled() || cursor.IsZero() {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadCursorState(); err != nil {
		return err
	}
	current := r.cursorState.LSN
	if !current.Less(cursor) {
		return nil
	}
	r.cursorState.LSN = cursor
	return r.persistCursorState(r.cursorState)
}

// ApplyFrame applies a single WAL frame using exact WAL semantics.
func (r *WALReplication) ApplyFrame(frame *WALFrame) (WALCursor, error) {
	return r.ApplyFrames(frameSlice(frame))
}

// ApplyFrames applies WAL frames in order and updates the local apply cursor.
func (r *WALReplication) ApplyFrames(frames []*WALFrame) (WALCursor, error) {
	return r.applyFrames(frames)
}

func (r *WALReplication) enabled() bool {
	return r != nil && r.db != nil && r.db.opt.EnableWAL
}

func (r *WALReplication) applyFrames(frames []*WALFrame) (WALCursor, error) {
	if !r.enabled() {
		return WALCursor{}, ErrInvalidRequest
	}
	if !r.isReplica() {
		return WALCursor{}, ErrPrimaryRoleApply
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.loadCursorState(); err != nil {
		return WALCursor{}, err
	}
	state := r.cursorState
	current := state.LSN
	if len(frames) == 0 {
		return current, nil
	}

	batchLimit := r.db.opt.maxBatchSize
	countLimit := r.db.opt.maxBatchCount
	if batchLimit <= 0 {
		batchLimit = 64 << 20
	}
	if countLimit <= 0 {
		countLimit = 1024
	}

	batch := make([]*Entry, 0, countLimit)
	var batchSize int64
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := r.db.applyWALEntries(batch); err != nil {
			return err
		}
		var maxVersion uint64
		for _, e := range batch {
			if v := y.ParseTs(e.Key); v > maxVersion {
				maxVersion = v
			}
		}
		r.advanceOracleForAppliedVersion(maxVersion)
		batch = make([]*Entry, 0, countLimit)
		batchSize = 0
		return nil
	}

	lastSeen := current
	strictPrev := current
	lastApplied := current
	maxMinRetainCursor := WALCursor{}
	advancedCount := 0
	forcePersist := false
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
		if !current.IsZero() && !current.Less(lsn) {
			continue
		}
		if lsn.Less(lastSeen) {
			return current, fmt.Errorf("out of order WAL frame cursor: current=%+v previous=%+v", lsn, lastSeen)
		}
		if lsn.Equal(lastSeen) {
			continue
		}
		lastSeen = lsn
		minRetain := walCursorFromPB(frame.MinRetainCursor)
		if !walCursorFollows(strictPrev, lsn) && canRecoverFromCompactionGap(strictPrev, lsn, minRetain) {
			r.pendingIntents = make(map[uint64][]replTxnIntent)
			r.pendingIntentSeen = make(map[uint64]map[WALCursor]struct{})
		}
		strictPrev = lsn

		walPtr, err := r.appendReplicatedFrame(frame)
		if err != nil {
			return current, err
		}

		entries, advance, err := r.frameToEntries(frame, walPtr)
		if err != nil {
			return current, err
		}
		if len(entries) > 0 {
			for _, be := range entries {
				es := be.estimateSizeAndSetThreshold(r.db.valueThreshold()) + int64(len(be.Value))
				if len(batch) > 0 && (int64(len(batch))+1 >= countLimit || batchSize+es >= batchLimit) {
					if err := flushBatch(); err != nil {
						return current, err
					}
				}
				batch = append(batch, be)
				batchSize += es
			}
		}
		if advance {
			lastApplied = lsn
			advancedCount++
			if frame.Type == WALFrameTxnCommit || frame.Type == WALFrameCheckpoint {
				forcePersist = true
			}
			if maxMinRetainCursor.Less(minRetain) {
				maxMinRetainCursor = minRetain
			}
		}
		r.recordLocalSourceCursor(walPtr.Fid, lsn)
	}

	if err := flushBatch(); err != nil {
		return current, err
	}
	if !lastApplied.IsZero() && !current.Less(lastApplied) {
		return current, nil
	}
	r.cursorState = replicationCursorState{LSN: lastApplied}
	r.cursorLoaded = true
	r.cursorDirty = true
	r.cursorDirtyCount += advancedCount
	if !maxMinRetainCursor.IsZero() {
		if r.lastMinRetainCursor.Less(maxMinRetainCursor) {
			if err := r.pruneByMinRetainCursor(maxMinRetainCursor); err != nil {
				return current, err
			}
			r.lastMinRetainCursor = maxMinRetainCursor
		}
	}
	if r.shouldPersistCursor(forcePersist) {
		if err := r.persistCursorState(r.cursorState); err != nil {
			return current, err
		}
	}
	return lastApplied, nil
}

func (r *WALReplication) appendReplicatedFrame(frame *WALFrame) (valuePointer, error) {
	if frame == nil {
		return valuePointer{}, nil
	}
	if frame.Type == WALFrameUnknown {
		return valuePointer{}, errors.New("unknown WAL frame type")
	}
	lsn := walCursorFromPB(frame.Lsn)
	if lsn.IsZero() {
		return valuePointer{}, errors.New("missing WAL frame cursor")
	}
	e := &Entry{
		Key:       y.SafeCopy(nil, frame.Key),
		Value:     y.SafeCopy(nil, frame.Value),
		UserMeta:  frameMetaByte(frame.UserMeta),
		ExpiresAt: frame.ExpiresAt,
		meta:      frameMetaByte(frame.Meta),
	}
	return r.db.wal.appendReplicatedAtLSN(e, lsn)
}

func (r *WALReplication) recordLocalSourceCursor(localFid uint32, source WALCursor) {
	if localFid == 0 || source.IsZero() {
		return
	}
	fm := r.localSourceByFid
	if cur := fm[localFid]; cur.Less(source) {
		fm[localFid] = source
	}
}

func (r *WALReplication) pruneByMinRetainCursor(minCursor WALCursor) error {
	fm := r.localSourceByFid
	if len(fm) == 0 || minCursor.IsZero() {
		return nil
	}
	fids := make([]int, 0, len(fm))
	for fid := range fm {
		fids = append(fids, int(fid))
	}
	sort.Ints(fids)

	minRetainFid := uint32(0)
	maxFid := uint32(fids[len(fids)-1])
	for _, fidInt := range fids {
		fid := uint32(fidInt)
		c := fm[fid]
		if c.IsZero() {
			minRetainFid = fid
			break
		}
		if !c.Less(minCursor) {
			minRetainFid = fid
			break
		}
	}
	if minRetainFid == 0 {
		minRetainFid = maxFid + 1
	}
	if minRetainFid > 1 {
		if err := r.db.wal.pruneBeforeFid(minRetainFid); err != nil {
			return err
		}
	}
	for fid := range fm {
		if fid < minRetainFid {
			delete(fm, fid)
		}
	}
	return nil
}

// frameToEntries converts one WAL frame into apply entries.
// Caller must hold r.mu.
func (r *WALReplication) frameToEntries(frame *WALFrame, walPtr valuePointer) ([]*Entry, bool, error) {
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
		lsn := walCursorFromPB(frame.Lsn)
		seen := r.pendingIntentSeen[txnID]
		if seen != nil {
			if _, ok := seen[lsn]; ok {
				return nil, false, nil
			}
		} else {
			seen = make(map[WALCursor]struct{})
			r.pendingIntentSeen[txnID] = seen
		}
		r.pendingIntents[txnID] = append(r.pendingIntents[txnID], replTxnIntent{lsn: lsn, e: intentCopy, walPtr: walPtr})
		seen[lsn] = struct{}{}
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
		recs := r.pendingIntents[txnID]
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
				meta:      ie.meta | bitTxnIntent | bitWALPointer,
				walPtr:    rec.walPtr,
			})
		}
		delete(r.pendingIntents, txnID)
		delete(r.pendingIntentSeen, txnID)
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
		meta:      meta | bitWALPointer,
		walPtr:    walPtr,
	}
	return []*Entry{entry}, true, nil
}

func frameMetaByte(v []byte) byte {
	if len(v) == 0 {
		return 0
	}
	return v[0]
}

// Caller must hold r.mu.
func (r *WALReplication) loadCursorState() error {
	if r.cursorLoaded {
		return nil
	}
	b, err := os.ReadFile(r.cursorStatePath())
	if err != nil {
		if os.IsNotExist(err) {
			r.cursorState = replicationCursorState{}
			r.cursorLoaded = true
			r.lastCursorPersist = time.Now()
			return nil
		}
		return err
	}
	var cursorPB pb.WALReplicationCursor
	if err := cursorPB.UnmarshalVT(b); err != nil {
		return err
	}
	r.cursorState = replicationCursorState{
		LSN: walCursorFromPB(cursorPB.Lsn),
	}
	r.cursorLoaded = true
	r.lastCursorPersist = time.Now()
	return nil
}

// Caller must hold r.mu.
func (r *WALReplication) persistCursorState(cursor replicationCursorState) error {
	if cursor.LSN.IsZero() {
		return nil
	}
	v, err := (&pb.WALReplicationCursor{
		Lsn: walCursorToPB(cursor.LSN),
	}).MarshalVT()
	if err != nil {
		return err
	}
	tmp := r.cursorStatePath() + ".tmp"
	if err := os.WriteFile(tmp, v, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.cursorStatePath()); err != nil {
		return err
	}
	r.cursorDirty = false
	r.cursorDirtyCount = 0
	r.lastCursorPersist = time.Now()
	return nil
}

func (r *WALReplication) cursorStatePath() string {
	return filepath.Join(r.db.opt.ValueDir, walReplicationCursorStateFile)
}

// Caller must hold r.mu.
func (r *WALReplication) shouldPersistCursor(force bool) bool {
	if !r.cursorDirty {
		return false
	}
	if force {
		return true
	}
	if r.cursorDirtyCount >= walReplicationCursorBatchSize {
		return true
	}
	if r.lastCursorPersist.IsZero() {
		return true
	}
	return time.Since(r.lastCursorPersist) >= walReplicationCursorBatchWait
}

func frameSlice(frame *WALFrame) []*WALFrame {
	if frame == nil {
		return nil
	}
	return []*WALFrame{frame}
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

func canRecoverFromCompactionGap(previous WALCursor, current WALCursor, minRetain WALCursor) bool {
	if previous.IsZero() || current.IsZero() || minRetain.IsZero() {
		return false
	}
	if !previous.Less(minRetain) {
		return false
	}
	if current.Less(minRetain) {
		return false
	}
	return true
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
	if !orc.isManaged && maxVersion > orc.txnMark.DoneUntil() {
		orc.txnMark.Done(maxVersion)
	}
}
