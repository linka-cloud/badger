package badger

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

type testReplicationRuntime struct {
	mu       sync.RWMutex
	role     ReplicationRole
	seen     []*WALFrame
	startErr error
	err      error
	start    int
}

func (r *testReplicationRuntime) Start(_ context.Context, _ *DB) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.start++
	return r.startErr
}

func (r *testReplicationRuntime) Role() ReplicationRole {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role
}

func (r *testReplicationRuntime) Replicate(_ context.Context, frame *WALFrame) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if frame != nil {
		r.seen = append(r.seen, frame)
	}
	return r.err
}

func (r *testReplicationRuntime) setRole(role ReplicationRole) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.role = role
}

func fixedRuntime(role ReplicationRole) *testReplicationRuntime {
	return &testReplicationRuntime{role: role}
}

func withTestFrameSource(frame *WALFrame) *WALFrame {
	return frame
}

func TestWALCursorProtoRoundTrip(t *testing.T) {
	orig := WALCursor{Fid: 7, Offset: 55, Len: 99}
	b := walCursorToPB(orig)
	buf, err := b.MarshalVT()
	require.NoError(t, err)
	var pbCur pb.WALCursor
	require.NoError(t, pbCur.UnmarshalVT(buf))
	got := walCursorFromPB(&pbCur)
	require.Equal(t, orig, got)
}

func TestWALFrameVTMarshalUnmarshalAndApply(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-vt-frame")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	repl := db.Replication()
	frame := withTestFrameSource(&WALFrame{
		Version:   WALFrameVersion,
		Type:      WALFrameEntry,
		Key:       y.KeyWithTs([]byte("vt-k"), 10),
		Value:     []byte("vt-v"),
		Meta:      nil,
		UserMeta:  nil,
		ExpiresAt: 0,
	})
	frame.Lsn = walCursorToPB(nextWALCursorForFrame(t, db, frame))

	buf, err := frame.MarshalVT()
	require.NoError(t, err)

	decoded := &WALFrame{}
	err = decoded.UnmarshalVT(buf)
	require.NoError(t, err)
	require.Equal(t, frame.Version, decoded.Version)
	require.Equal(t, frame.Type, decoded.Type)
	require.Equal(t, frame.Key, decoded.Key)
	require.Equal(t, frame.Value, decoded.Value)
	require.Equal(t, frame.MinRetainCursor, decoded.MinRetainCursor)

	_, err = repl.ApplyFrame(decoded)
	require.NoError(t, err)

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("vt-k"))
		require.NoError(t, err)
		require.Equal(t, []byte("vt-v"), getItemValue(t, item))
		return nil
	}))
}

func TestApplyFramePrunesByMinRetainCursor(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-min-retain")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).WithWAL(true).WithSyncWrites(true)
	opt.ValueThreshold = 0
	opt.ValueLogFileSize = 1 << 20

	primary, err := Open(opt)
	require.NoError(t, err)

	for i := 0; i < 200; i++ {
		i := i
		v := make([]byte, 64<<10)
		for j := range v {
			v[j] = byte((i + j) % 251)
		}
		require.NoError(t, primary.Update(func(txn *Txn) error {
			return txn.Set([]byte("mr-k-"+strconv.Itoa(i)), v)
		}))
	}
	require.NoError(t, primary.Close())

	fidsBefore := walFIDsForTest(t, dir)
	require.GreaterOrEqual(t, len(fidsBefore), 2)
	maxFid := fidsBefore[len(fidsBefore)-1]

	replica, err := Open(opt.WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, replica.Close()) }()

	frame := withTestFrameSource(&WALFrame{
		Version:         WALFrameVersion,
		Type:            WALFrameCheckpoint,
		MinRetainCursor: walCursorToPB(WALCursor{Fid: maxFid, Offset: 0, Len: 0}),
	})
	frame.Lsn = walCursorToPB(walCursorForFrameFrom(t, replica, frame, WALCursor{Fid: maxFid + 10, Offset: uint32(vlogHeaderSize)}))
	_, err = replica.Replication().ApplyFrame(frame)
	require.NoError(t, err)

	fidsAfter := walFIDsForTest(t, dir)
	require.NotEmpty(t, fidsAfter)
	require.Less(t, len(fidsAfter), len(fidsBefore))
	for _, fid := range fidsAfter {
		require.GreaterOrEqual(t, fid, maxFid)
	}
}

func walFIDsForTest(t *testing.T, dir string) []uint32 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []uint32
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, walFileExt) {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSuffix(name, walFileExt), 10, 32)
		require.NoError(t, err)
		out = append(out, uint32(v))
	}
	require.NotEmpty(t, out)
	for i := 0; i < len(out)-1; i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func walBytesForFid(t *testing.T, dir string, fid uint32) []byte {
	t.Helper()
	b, err := os.ReadFile(walFilePath(dir, fid))
	require.NoError(t, err)
	return b
}

func walEncodedLenForFrame(t *testing.T, db *DB, frame *WALFrame, offset uint32) uint32 {
	t.Helper()
	e := &Entry{
		Key:       y.SafeCopy(nil, frame.Key),
		Value:     y.SafeCopy(nil, frame.Value),
		UserMeta:  frameMetaByte(frame.UserMeta),
		ExpiresAt: frame.ExpiresAt,
		meta:      frameMetaByte(frame.Meta),
	}
	db.wal.mu.RLock()
	lf := db.wal.files[db.wal.maxFid]
	require.NotNil(t, lf)
	var enc bytes.Buffer
	plen, err := lf.encodeEntry(&enc, e, offset)
	db.wal.mu.RUnlock()
	require.NoError(t, err)
	return uint32(plen)
}

func walFileCapacityForTest(t *testing.T, db *DB) int {
	t.Helper()
	db.wal.mu.RLock()
	defer db.wal.mu.RUnlock()
	lf := db.wal.files[db.wal.maxFid]
	require.NotNil(t, lf)
	return len(lf.Data)
}

func walCursorForFrameFrom(t *testing.T, db *DB, frame *WALFrame, start WALCursor) WALCursor {
	t.Helper()
	cap := walFileCapacityForTest(t, db)
	cur := start
	cur.Len = walEncodedLenForFrame(t, db, frame, cur.Offset)
	if int(cur.Offset)+int(cur.Len)+maxHeaderSize >= cap {
		cur.Fid++
		cur.Offset = uint32(vlogHeaderSize)
		cur.Len = walEncodedLenForFrame(t, db, frame, cur.Offset)
	}
	return cur
}

func nextWALCursorForFrame(t *testing.T, db *DB, frame *WALFrame) WALCursor {
	t.Helper()
	db.wal.mu.RLock()
	lf := db.wal.files[db.wal.maxFid]
	require.NotNil(t, lf)
	start := WALCursor{Fid: lf.fid, Offset: lf.writeAt}
	db.wal.mu.RUnlock()
	return walCursorForFrameFrom(t, db, frame, start)
}

func nextWALCursorAfter(t *testing.T, db *DB, frame *WALFrame, prev WALCursor) WALCursor {
	t.Helper()
	start := WALCursor{Fid: prev.Fid, Offset: prev.Offset + prev.Len}
	return walCursorForFrameFrom(t, db, frame, start)
}

func TestApplyWALFramesReplicatesAndPersistsCursor(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "badger-wal-repl-src")
	require.NoError(t, err)
	defer removeDir(srcDir)

	dstDir, err := os.MkdirTemp("", "badger-wal-repl-dst")
	require.NoError(t, err)
	defer removeDir(dstDir)

	srcOpt := getTestOptions(srcDir).WithWAL(true).WithSyncWrites(true)
	src, err := Open(srcOpt)
	require.NoError(t, err)

	require.NoError(t, src.Update(func(txn *Txn) error {
		require.NoError(t, txn.Set([]byte("k1"), []byte("v1")))
		require.NoError(t, txn.Set([]byte("k2"), []byte("v2")))
		return nil
	}))

	frames := make([]*WALFrame, 0, 16)
	target, _ := src.WALLSN()
	ctx, cancel := context.WithCancel(context.Background())
	err = src.StreamWAL(ctx, WALStreamOptions{Continuous: true}, func(f *WALFrame) error {
		frames = append(frames, f)
		if !target.IsZero() && !walCursorFromPB(f.Lsn).Less(target) {
			cancel()
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NotEmpty(t, frames)
	require.NoError(t, src.Close())

	dstOpt := getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica))
	dst, err := Open(dstOpt)
	require.NoError(t, err)
	repl := dst.Replication()

	cur, err := repl.ApplyFrames(frames)
	require.NoError(t, err)
	require.False(t, cur.IsZero())

	require.NoError(t, dst.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("k1"))
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), getItemValue(t, item))

		item, err = txn.Get([]byte("k2"))
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), getItemValue(t, item))
		return nil
	}))

	persisted, err := repl.Cursor()
	require.NoError(t, err)
	require.Equal(t, cur, persisted)
	require.NoError(t, dst.Close())

	dst, err = Open(dstOpt)
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()
	repl = dst.Replication()

	persisted, err = repl.Cursor()
	require.NoError(t, err)
	require.Equal(t, cur, persisted)

	cur2, err := repl.ApplyFrames(frames)
	require.NoError(t, err)
	require.Equal(t, cur, cur2)
}

func TestReplicatedWALFilesAreByteIdenticalAcrossReplicas(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "badger-wal-identical-src")
	require.NoError(t, err)
	defer removeDir(srcDir)

	dst1Dir, err := os.MkdirTemp("", "badger-wal-identical-dst1")
	require.NoError(t, err)
	defer removeDir(dst1Dir)

	dst2Dir, err := os.MkdirTemp("", "badger-wal-identical-dst2")
	require.NoError(t, err)
	defer removeDir(dst2Dir)

	srcOpt := getTestOptions(srcDir).WithWAL(true).WithSyncWrites(true)
	srcOpt.ValueThreshold = 0
	srcOpt.ValueLogFileSize = 1 << 20
	src, err := Open(srcOpt)
	require.NoError(t, err)

	for i := 0; i < 80; i++ {
		i := i
		payload := make([]byte, 16<<10)
		for j := range payload {
			payload[j] = byte((i + j) % 251)
		}
		require.NoError(t, src.Update(func(txn *Txn) error {
			return txn.Set([]byte("identical-k-"+strconv.Itoa(i)), payload)
		}))
	}

	frames := make([]*WALFrame, 0, 256)
	target, _ := src.WALLSN()
	ctx, cancel := context.WithCancel(context.Background())
	err = src.StreamWAL(ctx, WALStreamOptions{Continuous: true}, func(f *WALFrame) error {
		frames = append(frames, f)
		if !target.IsZero() && !walCursorFromPB(f.Lsn).Less(target) {
			cancel()
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NotEmpty(t, frames)
	require.NoError(t, src.Close())

	openReplica := func(dir string) *DB {
		dstOpt := getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
			WithReplication(fixedRuntime(ReplicationRoleReplica))
		dstOpt.ValueThreshold = 0
		dstOpt.ValueLogFileSize = 1 << 20
		db, err := Open(dstOpt)
		require.NoError(t, err)
		return db
	}

	dst1 := openReplica(dst1Dir)
	dst2 := openReplica(dst2Dir)
	defer func() { require.NoError(t, dst1.Close()) }()
	defer func() { require.NoError(t, dst2.Close()) }()

	_, err = dst1.Replication().ApplyFrames(frames)
	require.NoError(t, err)
	_, err = dst2.Replication().ApplyFrames(frames)
	require.NoError(t, err)

	fids1 := walFIDsForTest(t, dst1Dir)
	fids2 := walFIDsForTest(t, dst2Dir)
	require.Equal(t, fids1, fids2)

	for _, fid := range fids1 {
		b1 := walBytesForFid(t, dst1Dir, fid)
		b2 := walBytesForFid(t, dst2Dir, fid)
		require.Equal(t, b1, b2)
	}
}

func TestWALReplicationUsesGlobalCursorDomain(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-global-cursor")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	repl := db.Replication()

	curA := WALCursor{Fid: 10, Offset: 20, Len: 5}
	curB := WALCursor{Fid: 11, Offset: 30, Len: 7}
	require.NoError(t, repl.SetCursor(curA))
	require.NoError(t, repl.SetCursor(curB))

	gotA, err := repl.Cursor()
	require.NoError(t, err)
	gotB, err := repl.Cursor()
	require.NoError(t, err)
	gotDefault, err := repl.Cursor()
	require.NoError(t, err)

	require.Equal(t, curB, gotA)
	require.Equal(t, curB, gotB)
	require.Equal(t, curB, gotDefault)
}

func TestReplicationCursorBatchesAndForcesFlushOnCommit(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-cursor-batch")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	repl := db.Replication()

	entryFrame := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Key:     y.KeyWithTs([]byte("k"), 10),
		Value:   []byte("v"),
	})
	entryCursor := nextWALCursorForFrame(t, db, entryFrame)
	entryFrame.Lsn = walCursorToPB(entryCursor)

	_, err = repl.ApplyFrame(entryFrame)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, walReplicationCursorStateFile))
	require.ErrorIs(t, err, os.ErrNotExist)

	commitFrame := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnCommit,
		Key:     buildTxnCommitKey(0),
		Value:   encodeTxnCommitValue(1, 0),
	})
	commitCursor := nextWALCursorAfter(t, db, commitFrame, entryCursor)
	commitFrame.Lsn = walCursorToPB(commitCursor)

	cur, err := repl.ApplyFrame(commitFrame)
	require.NoError(t, err)
	require.Equal(t, walCursorFromPB(commitFrame.Lsn), cur)

	b, err := os.ReadFile(filepath.Join(dir, walReplicationCursorStateFile))
	require.NoError(t, err)
	var st pb.WALReplicationCursor
	require.NoError(t, st.UnmarshalVT(b))
	require.Equal(t, cur, walCursorFromPB(st.Lsn))
}

func TestApplyFrameFailsOnWALCursorPlacementMismatch(t *testing.T) {
	dstDir, err := os.MkdirTemp("", "badger-wal-cursor-mismatch-dst")
	require.NoError(t, err)
	defer removeDir(dstDir)

	dst, err := Open(getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()

	repl := dst.Replication()

	entry := &Entry{Key: y.KeyWithTs([]byte("mismatch-k1"), 10), Value: []byte("mismatch-v1")}

	dst.wal.mu.RLock()
	lf := dst.wal.files[dst.wal.maxFid]
	require.NotNil(t, lf)
	start := lf.writeAt
	fid := lf.fid
	var enc bytes.Buffer
	plen, err := lf.encodeEntry(&enc, entry, start)
	if int(start)+plen+maxHeaderSize >= len(lf.Data) {
		fid++
		start = uint32(vlogHeaderSize)
		enc.Reset()
		plen, err = lf.encodeEntry(&enc, entry, start)
	}
	dst.wal.mu.RUnlock()
	require.NoError(t, err)

	first := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Lsn: walCursorToPB(WALCursor{
			Fid:    fid,
			Offset: start,
			Len:    uint32(plen),
		}),
		Key:   y.SafeCopy(nil, entry.Key),
		Value: y.SafeCopy(nil, entry.Value),
	})
	_, err = repl.ApplyFrame(first)
	require.NoError(t, err)

	base := walCursorFromPB(first.Lsn)
	bad := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Lsn:     walCursorToPB(WALCursor{Fid: base.Fid, Offset: base.Offset + base.Len + 1, Len: base.Len}),
		Key:     y.KeyWithTs([]byte("mismatch-k2"), 11),
		Value:   []byte("mismatch-v2"),
	})
	_, err = repl.ApplyFrame(bad)
	require.Error(t, err)
	require.Contains(t, err.Error(), "wal offset mismatch")
}

func TestReapplyIntentFrameAfterRestartIsIdempotent(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-intent-reapply")
	require.NoError(t, err)
	defer removeDir(dir)

	openReplica := func() *DB {
		db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
			WithReplication(fixedRuntime(ReplicationRoleReplica)))
		require.NoError(t, err)
		return db
	}

	txnID := uint64(777)
	intent := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnIntent,
		Key:     buildTxnIntentKey(txnID, 0),
		Value:   encodeTxnIntentValue(NewEntry([]byte("replay-k"), []byte("replay-v"))),
	})

	db := openReplica()
	repl := db.Replication()
	intent.Lsn = walCursorToPB(nextWALCursorForFrame(t, db, intent))
	_, err = repl.ApplyFrame(intent)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db = openReplica()
	defer func() { require.NoError(t, db.Close()) }()
	repl = db.Replication()
	_, err = repl.ApplyFrame(intent)
	require.NoError(t, err)

	commit := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnCommit,
		Key:     buildTxnCommitKey(txnID),
		Value:   encodeTxnCommitValue(999, txnID),
	})
	commit.Lsn = walCursorToPB(nextWALCursorAfter(t, db, commit, walCursorFromPB(intent.Lsn)))
	cur, err := repl.ApplyFrame(commit)
	require.NoError(t, err)
	require.Equal(t, walCursorFromPB(commit.Lsn), cur)
}

func TestApplyFrameRecoversWriteOffsetFromDisk(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-recover-writeat")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	repl := db.Replication()
	first := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Key:     y.KeyWithTs([]byte("recover-k1"), 10),
		Value:   []byte("recover-v1"),
	})
	first.Lsn = walCursorToPB(nextWALCursorForFrame(t, db, first))
	_, err = repl.ApplyFrame(first)
	require.NoError(t, err)

	second := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Key:     y.KeyWithTs([]byte("recover-k2"), 11),
		Value:   []byte("recover-v2"),
	})
	second.Lsn = walCursorToPB(nextWALCursorAfter(t, db, second, walCursorFromPB(first.Lsn)))

	db.wal.mu.Lock()
	lf := db.wal.files[db.wal.maxFid]
	require.NotNil(t, lf)
	lf.writeAt = uint32(vlogHeaderSize)
	db.wal.mu.Unlock()

	_, err = repl.ApplyFrame(second)
	require.NoError(t, err)
}

func TestApplyFrameAllowsEmptyFileMidOffsetResume(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-mid-offset-resume")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	repl := db.Replication()
	frame := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Key:     y.KeyWithTs([]byte("mid-resume-k"), 10),
		Value:   []byte("mid-resume-v"),
	})

	c := walCursorForFrameFrom(t, db, frame, WALCursor{Fid: 3, Offset: uint32(vlogHeaderSize) + 128*1024})
	frame.Lsn = walCursorToPB(c)

	cur, err := repl.ApplyFrame(frame)
	require.NoError(t, err)
	require.Equal(t, c, cur)
}

func TestApplyWALFramesFromStream(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "badger-wal-stream-src")
	require.NoError(t, err)
	defer removeDir(srcDir)

	dstDir, err := os.MkdirTemp("", "badger-wal-stream-dst")
	require.NoError(t, err)
	defer removeDir(dstDir)

	src, err := Open(getTestOptions(srcDir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	require.NoError(t, src.Update(func(txn *Txn) error {
		return txn.Set([]byte("stream-apply-k"), []byte("stream-apply-v"))
	}))

	dst, err := Open(getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()
	repl := dst.Replication()

	target, _ := src.WALLSN()
	ctx, cancel := context.WithCancel(context.Background())
	cur := WALCursor{}
	err = src.StreamWAL(ctx, WALStreamOptions{Continuous: true}, func(f *WALFrame) error {
		var err error
		cur, err = repl.ApplyFrames([]*WALFrame{f})
		if err != nil {
			return err
		}
		if !target.IsZero() && !walCursorFromPB(f.Lsn).Less(target) {
			cancel()
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, cur.IsZero())

	require.NoError(t, dst.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("stream-apply-k"))
		require.NoError(t, err)
		require.Equal(t, []byte("stream-apply-v"), getItemValue(t, item))
		return nil
	}))
	require.NoError(t, src.Close())
}

func TestApplyWALFramesStrictCursorContinuity(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "badger-wal-strict-src")
	require.NoError(t, err)
	defer removeDir(srcDir)

	dstDir, err := os.MkdirTemp("", "badger-wal-strict-dst")
	require.NoError(t, err)
	defer removeDir(dstDir)

	src, err := Open(getTestOptions(srcDir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	require.NoError(t, src.Update(func(txn *Txn) error {
		require.NoError(t, txn.Set([]byte("strict-k1"), []byte("v1")))
		require.NoError(t, txn.Set([]byte("strict-k2"), []byte("v2")))
		require.NoError(t, txn.Set([]byte("strict-k3"), []byte("v3")))
		return nil
	}))

	frames := make([]*WALFrame, 0, 16)
	target, _ := src.WALLSN()
	ctx, cancel := context.WithCancel(context.Background())
	err = src.StreamWAL(ctx, WALStreamOptions{Continuous: true}, func(f *WALFrame) error {
		frames = append(frames, f)
		if !target.IsZero() && !walCursorFromPB(f.Lsn).Less(target) {
			cancel()
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.GreaterOrEqual(t, len(frames), 3)
	require.NoError(t, src.Close())

	dst, err := Open(getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()
	repl := dst.Replication()

	_, err = repl.ApplyFrames([]*WALFrame{frames[0], frames[2]})
	require.Error(t, err)
	require.Contains(t, err.Error(), "wal offset mismatch")
}

func TestApplyWALFramesStrictCursorContinuityAllowsCompactionRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-strict-compaction-recovery")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	repl := db.Replication()
	old := WALCursor{Fid: 1, Offset: 20, Len: 10}
	require.NoError(t, repl.SetCursor(old))

	frame := withTestFrameSource(&WALFrame{
		Version:         WALFrameVersion,
		Type:            WALFrameCheckpoint,
		Key:             []byte("c"),
		Value:           []byte{1},
		MinRetainCursor: walCursorToPB(WALCursor{Fid: 3, Offset: 20, Len: 8}),
	})
	frame.Lsn = walCursorToPB(walCursorForFrameFrom(t, db, frame, WALCursor{Fid: 4, Offset: uint32(vlogHeaderSize)}))

	cur, err := repl.ApplyFrames([]*WALFrame{frame})
	require.NoError(t, err)
	require.Equal(t, walCursorFromPB(frame.Lsn), cur)

	persisted, err := repl.Cursor()
	require.NoError(t, err)
	require.Equal(t, cur, persisted)
}

func TestApplyWALFramesGlobalPendingIntents(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-replica-intents")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	repl := db.Replication()

	txnID := uint64(42)

	intentA := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnIntent,
		Key:     buildTxnIntentKey(txnID, 0),
		Value:   encodeTxnIntentValue(NewEntry([]byte("a"), []byte("1"))),
	})
	intentACursor := nextWALCursorForFrame(t, db, intentA)
	intentA.Lsn = walCursorToPB(intentACursor)
	intentB := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnIntent,
		Key:     buildTxnIntentKey(txnID, 0),
		Value:   encodeTxnIntentValue(NewEntry([]byte("b"), []byte("2"))),
	})
	intentBCursor := nextWALCursorAfter(t, db, intentB, intentACursor)
	intentB.Lsn = walCursorToPB(intentBCursor)
	commit := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnCommit,
		Key:     buildTxnCommitKey(txnID),
		Value:   encodeTxnCommitValue(500, txnID),
	})
	commitCursor := nextWALCursorAfter(t, db, commit, intentBCursor)
	commit.Lsn = walCursorToPB(commitCursor)

	_, err = repl.ApplyFrames([]*WALFrame{intentA})
	require.NoError(t, err)
	_, err = repl.ApplyFrames([]*WALFrame{intentB})
	require.NoError(t, err)
	_, err = repl.ApplyFrames([]*WALFrame{commit})
	require.NoError(t, err)

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), getItemValue(t, item))

		item, err = txn.Get([]byte("b"))
		require.NoError(t, err)
		require.Equal(t, []byte("2"), getItemValue(t, item))
		return nil
	}))
}

func TestApplyFramesSkipsDuplicateLSNInBatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-dup-lsn")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	repl := db.Replication()
	base := &WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Key:     y.KeyWithTs([]byte("dup-a"), 50),
		Value:   []byte("value-a"),
	}
	lsn := walCursorToPB(nextWALCursorForFrame(t, db, base))
	frames := []*WALFrame{
		{
			Version: WALFrameVersion,
			Type:    WALFrameEntry,
			Lsn:     lsn,
			Key:     y.KeyWithTs([]byte("dup-a"), 50),
			Value:   []byte("value-a"),
		},
		{
			Version: WALFrameVersion,
			Type:    WALFrameEntry,
			Lsn:     lsn,
			Key:     y.KeyWithTs([]byte("dup-b"), 50),
			Value:   []byte("value-b"),
		},
	}
	for _, f := range frames {
		withTestFrameSource(f)
	}

	_, err = repl.ApplyFrames(frames)
	require.NoError(t, err)

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("dup-a"))
		require.NoError(t, err)
		require.Equal(t, []byte("value-a"), getItemValue(t, item))

		_, err = txn.Get([]byte("dup-b"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	}))
}

func TestReplicationRoleSwitchWithoutReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-repl-role-switch")
	require.NoError(t, err)
	defer removeDir(dir)

	runtime := &testReplicationRuntime{role: ReplicationRolePrimary}
	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(runtime))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.Equal(t, ReplicationRolePrimary, runtime.Role())

	runtime.setRole(ReplicationRoleReplica)
	require.Equal(t, ReplicationRoleReplica, runtime.Role())

	err = db.Update(func(txn *Txn) error {
		return txn.Set([]byte("blocked"), []byte("v"))
	})
	require.ErrorIs(t, err, ErrReadOnlyReplica)

	repl := db.Replication()
	frame := withTestFrameSource(&WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Key:     y.KeyWithTs([]byte("from-repl"), 10),
		Value:   []byte("ok"),
	})
	frame.Lsn = walCursorToPB(nextWALCursorForFrame(t, db, frame))
	_, err = repl.ApplyFrame(frame)
	require.NoError(t, err)

	runtime.setRole(ReplicationRolePrimary)
	require.Equal(t, ReplicationRolePrimary, runtime.Role())

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("user-write"), []byte("ok"))
	}))

	_, err = repl.ApplyFrame(frame)
	require.ErrorIs(t, err, ErrPrimaryRoleApply)
}

func TestWALReplicationFailoverSwitchesSourceDomain(t *testing.T) {
	primaryDir, err := os.MkdirTemp("", "badger-failover-primary")
	require.NoError(t, err)
	defer removeDir(primaryDir)

	promotedDir, err := os.MkdirTemp("", "badger-failover-promoted")
	require.NoError(t, err)
	defer removeDir(promotedDir)

	followerDir, err := os.MkdirTemp("", "badger-failover-follower")
	require.NoError(t, err)
	defer removeDir(followerDir)

	postFailoverDir, err := os.MkdirTemp("", "badger-failover-post")
	require.NoError(t, err)
	defer removeDir(postFailoverDir)

	primaryRuntime := fixedRuntime(ReplicationRolePrimary)
	primary, err := Open(getTestOptions(primaryDir).WithWAL(true).WithSyncWrites(true).WithReplication(primaryRuntime))
	require.NoError(t, err)
	defer func() { require.NoError(t, primary.Close()) }()

	promotedRuntime := fixedRuntime(ReplicationRoleReplica)
	promoted, err := Open(getTestOptions(promotedDir).WithWAL(true).WithSyncWrites(true).WithReplication(promotedRuntime))
	require.NoError(t, err)
	defer func() { require.NoError(t, promoted.Close()) }()

	followerRuntime := fixedRuntime(ReplicationRoleReplica)
	follower, err := Open(getTestOptions(followerDir).WithWAL(true).WithSyncWrites(true).WithReplication(followerRuntime))
	require.NoError(t, err)
	defer func() { require.NoError(t, follower.Close()) }()

	ship := func(src *DB, dst *DB, start WALCursor) (WALCursor, error) {
		last := start
		err := src.StreamWAL(context.Background(), WALStreamOptions{
			StartCursor: start,
		}, func(f *WALFrame) error {
			var err error
			last, err = dst.Replication().ApplyFrames([]*WALFrame{f})
			return err
		})
		return last, err
	}

	require.NoError(t, primary.Update(func(txn *Txn) error {
		return txn.Set([]byte("failover-pre"), []byte("v1"))
	}))

	start, err := promoted.Replication().Cursor()
	require.NoError(t, err)
	_, err = ship(primary, promoted, start)
	require.NoError(t, err)

	fStart, err := follower.Replication().Cursor()
	require.NoError(t, err)
	_, err = ship(primary, follower, fStart)
	require.NoError(t, err)

	stateBefore, err := follower.Replication().cursorStateSnapshot()
	require.NoError(t, err)
	require.False(t, stateBefore.LSN.IsZero())

	promotedRuntime.setRole(ReplicationRolePrimary)
	require.NoError(t, promoted.Update(func(txn *Txn) error {
		return txn.Set([]byte("failover-post"), []byte("v2"))
	}))

	postRuntime := fixedRuntime(ReplicationRoleReplica)
	postFollower, err := Open(getTestOptions(postFailoverDir).WithWAL(true).WithSyncWrites(true).WithReplication(postRuntime))
	require.NoError(t, err)
	defer func() { require.NoError(t, postFollower.Close()) }()

	_, err = ship(promoted, postFollower, WALCursor{})
	require.NoError(t, err)

	stateAfter, err := postFollower.Replication().cursorStateSnapshot()
	require.NoError(t, err)
	require.False(t, stateAfter.LSN.IsZero())

	require.NoError(t, postFollower.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("failover-pre"))
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), getItemValue(t, item))

		item, err = txn.Get([]byte("failover-post"))
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), getItemValue(t, item))
		return nil
	}))
}

func TestNewTransactionForcesReadOnlyInReplicaRole(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-repl-update-early")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	txn := db.NewTransaction(true)
	require.False(t, txn.update)
	txn.Discard()

	called := false
	err = db.Update(func(txn *Txn) error {
		called = true
		return txn.Set([]byte("k"), []byte("v"))
	})
	require.ErrorIs(t, err, ErrReadOnlyReplica)
	require.False(t, called)
}

func TestWALReplicationBetweenTwoInstances(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "badger-wal-repl-int-src")
	require.NoError(t, err)
	defer removeDir(srcDir)

	dstDir, err := os.MkdirTemp("", "badger-wal-repl-int-dst")
	require.NoError(t, err)
	defer removeDir(dstDir)

	src, err := Open(getTestOptions(srcDir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close()) }()

	dst, err := Open(getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()

	repl := dst.Replication()
	ship := func() WALCursor {
		start, err := repl.Cursor()
		require.NoError(t, err)
		target, _ := src.WALLSN()
		if target.IsZero() || !start.Less(target) {
			return start
		}
		ctx, cancel := context.WithCancel(context.Background())
		last := start
		err = src.StreamWAL(ctx, WALStreamOptions{StartCursor: start}, func(f *WALFrame) error {
			buf, err := f.MarshalVT()
			if err != nil {
				return err
			}
			decoded := &WALFrame{}
			if err := decoded.UnmarshalVT(buf); err != nil {
				return err
			}
			last, err = repl.ApplyFrame(decoded)
			if err != nil {
				return err
			}
			if !target.IsZero() && !walCursorFromPB(f.Lsn).Less(target) {
				cancel()
			}
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		return last
	}

	assertValue := func(db *DB, key string, val []byte) {
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(key))
			require.NoError(t, err)
			require.Equal(t, val, getItemValue(t, item))
			return nil
		}))
	}

	assertMissing := func(db *DB, key string) {
		require.NoError(t, db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte(key))
			require.ErrorIs(t, err, ErrKeyNotFound)
			return nil
		}))
	}

	require.NoError(t, src.Update(func(txn *Txn) error {
		require.NoError(t, txn.Set([]byte("alpha"), []byte("1")))
		require.NoError(t, txn.Set([]byte("beta"), []byte("2")))
		return nil
	}))

	first := ship()
	require.False(t, first.IsZero())
	assertValue(dst, "alpha", []byte("1"))
	assertValue(dst, "beta", []byte("2"))

	second := ship()
	require.Equal(t, first, second)

	require.NoError(t, src.Update(func(txn *Txn) error {
		require.NoError(t, txn.Set([]byte("alpha"), []byte("3")))
		require.NoError(t, txn.Delete([]byte("beta")))
		require.NoError(t, txn.Set([]byte("gamma"), []byte("4")))
		return nil
	}))

	third := ship()
	require.True(t, first.Less(third))
	assertValue(dst, "alpha", []byte("3"))
	assertMissing(dst, "beta")
	assertValue(dst, "gamma", []byte("4"))

	stored, err := repl.Cursor()
	require.NoError(t, err)
	require.Equal(t, third, stored)
}

func TestWALReplicationStrictRecoveryWhenReplicaFallsBehindCompaction(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "badger-wal-repl-recover-src")
	require.NoError(t, err)
	defer removeDir(srcDir)

	dstDir, err := os.MkdirTemp("", "badger-wal-repl-recover-dst")
	require.NoError(t, err)
	defer removeDir(dstDir)

	opt := getTestOptions(srcDir).WithWAL(true).WithSyncWrites(true)
	opt.ValueThreshold = 0
	opt.ValueLogFileSize = 1 << 20

	src, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close()) }()

	replicaOpt := getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true).
		WithReplication(fixedRuntime(ReplicationRoleReplica))

	dst, err := Open(replicaOpt)
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()

	repl := dst.Replication()

	shipStrict := func(from WALCursor) (WALCursor, error) {
		last := from
		err := src.StreamWAL(context.Background(), WALStreamOptions{StartCursor: from}, func(f *WALFrame) error {
			var err error
			last, err = repl.ApplyFrames([]*WALFrame{f})
			return err
		})
		if errors.Is(err, ErrWALCursorCompacted) {
			fallback := src.wal.minRetainCursorSnapshot()
			err = src.StreamWAL(context.Background(), WALStreamOptions{StartCursor: fallback}, func(f *WALFrame) error {
				var err error
				last, err = repl.ApplyFrames([]*WALFrame{f})
				return err
			})
		}
		return last, err
	}

	require.NoError(t, src.Update(func(txn *Txn) error {
		return txn.Set([]byte("recover-key"), []byte("v1"))
	}))

	start, err := repl.Cursor()
	require.NoError(t, err)
	synced, err := shipStrict(start)
	require.NoError(t, err)
	require.False(t, synced.IsZero())

	for i := 0; i < 140; i++ {
		i := i
		value := make([]byte, 32<<10)
		for j := range value {
			value[j] = byte((i + j) % 251)
		}
		require.NoError(t, src.Update(func(txn *Txn) error {
			return txn.Set([]byte("recover-fill-"+strconv.Itoa(i)), value)
		}))
	}
	require.NoError(t, src.Update(func(txn *Txn) error {
		return txn.Set([]byte("recover-key"), []byte("v2"))
	}))

	minRetain := WALCursor{}
	for i := 0; i < 20; i++ {
		err := src.RunWALGC(0.0001)
		if err != nil && err != ErrNoRewrite {
			require.NoError(t, err)
		}
		minRetain = src.wal.minRetainCursorSnapshot()
		if !minRetain.IsZero() && synced.Less(minRetain) {
			break
		}
	}
	require.False(t, minRetain.IsZero())
	require.True(t, synced.Less(minRetain))

	last, err := shipStrict(synced)
	require.NoError(t, err)
	require.False(t, last.Less(minRetain))

	require.NoError(t, dst.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("recover-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), getItemValue(t, item))
		return nil
	}))
}

func TestReplicationRuntimeStartCalled(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-repl-start")
	require.NoError(t, err)
	defer removeDir(dir)

	runtime := &testReplicationRuntime{role: ReplicationRolePrimary}
	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).WithReplication(runtime))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	require.Equal(t, 1, runtime.start)
}

func TestReplicationRuntimeStartError(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-repl-start-err")
	require.NoError(t, err)
	defer removeDir(dir)

	runtime := &testReplicationRuntime{role: ReplicationRolePrimary, startErr: context.Canceled}
	_, err = Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).WithReplication(runtime))
	require.Error(t, err)
	require.Contains(t, err.Error(), context.Canceled.Error())
}

func TestReplicationRuntimeReplicateCalled(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-repl-hook")
	require.NoError(t, err)
	defer removeDir(dir)

	runtime := &testReplicationRuntime{role: ReplicationRolePrimary}
	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).WithReplication(runtime))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("hook-k"), []byte("hook-v"))
	}))

	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	require.NotEmpty(t, runtime.seen)
}

func TestReplicationRuntimeReplicateErrorFailsWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-repl-hook-err")
	require.NoError(t, err)
	defer removeDir(dir)

	runtime := &testReplicationRuntime{role: ReplicationRolePrimary, err: ErrRejected}
	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true).WithReplication(runtime))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	err = db.Update(func(txn *Txn) error {
		return txn.Set([]byte("err-k"), []byte("err-v"))
	})
	require.ErrorIs(t, err, ErrRejected)
}

func TestReplicationRequiresWALMode(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-repl-requires-wal")
	require.NoError(t, err)
	defer removeDir(dir)

	_, err = Open(getTestOptions(dir).WithWAL(false).WithReplication(fixedRuntime(ReplicationRoleReplica)))
	require.ErrorIs(t, err, ErrReplicationRequiresWAL)
}
