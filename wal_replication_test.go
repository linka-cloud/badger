package badger

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

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

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Replication().SetRole(ReplicationRoleReplica))

	repl := db.Replication()
	frame := &WALFrame{
		Version:   WALFrameVersion,
		Type:      WALFrameEntry,
		Lsn:       walCursorToPB(WALCursor{Fid: 1, Offset: 20, Len: 10}),
		Key:       y.KeyWithTs([]byte("vt-k"), 10),
		Value:     []byte("vt-v"),
		Meta:      nil,
		UserMeta:  nil,
		ExpiresAt: 0,
	}

	buf, err := repl.MarshalFrame(frame)
	require.NoError(t, err)

	decoded, err := repl.UnmarshalFrame(buf)
	require.NoError(t, err)
	require.Equal(t, frame.Version, decoded.Version)
	require.Equal(t, frame.Type, decoded.Type)
	require.Equal(t, frame.Key, decoded.Key)
	require.Equal(t, frame.Value, decoded.Value)

	_, err = repl.ApplyFrameBytes(buf)
	require.NoError(t, err)

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("vt-k"))
		require.NoError(t, err)
		require.Equal(t, []byte("vt-v"), getItemValue(t, item))
		return nil
	}))
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
	require.NoError(t, src.StreamWALFrom(WALCursor{}, func(f *WALFrame) error {
		frames = append(frames, f)
		return nil
	}))
	require.NotEmpty(t, frames)
	require.NoError(t, src.Close())

	dstOpt := getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true)
	dst, err := Open(dstOpt)
	require.NoError(t, err)
	require.NoError(t, dst.Replication().SetRole(ReplicationRoleReplica))
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
	require.NoError(t, dst.Replication().SetRole(ReplicationRoleReplica))
	repl = dst.Replication()

	persisted, err = repl.Cursor()
	require.NoError(t, err)
	require.Equal(t, cur, persisted)

	cur2, err := repl.ApplyFrames(frames)
	require.NoError(t, err)
	require.Equal(t, cur, cur2)
}

func TestStreamWALDurableOnlyBound(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-durable-only")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).WithWAL(true).WithSyncWrites(false)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("durable-k"), []byte("durable-v"))
	}))

	durableBefore, _ := db.WALLSN()
	frames := 0
	require.NoError(t, db.StreamWALFromOptions(WALCursor{}, WALStreamOptions{DurableOnly: true}, func(f *WALFrame) error {
		if !durableBefore.IsZero() {
			require.False(t, durableBefore.Less(walCursorFromPB(f.Lsn)))
		}
		frames++
		return nil
	}))
	if durableBefore.IsZero() {
		require.Equal(t, 0, frames)
	}

	require.NoError(t, db.Sync())
	durableAfter, _ := db.WALLSN()
	require.False(t, durableAfter.IsZero())
	frames = 0
	require.NoError(t, db.StreamWALFromOptions(WALCursor{}, WALStreamOptions{DurableOnly: true}, func(f *WALFrame) error {
		require.False(t, durableAfter.Less(walCursorFromPB(f.Lsn)))
		frames++
		return nil
	}))
	require.Greater(t, frames, 0)
}

func TestSafeLSNSetGet(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-safe-lsn")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	repl := db.Replication()

	lsn := WALCursor{Fid: 2, Offset: 10, Len: 7}
	repl.SetSafeLSN(lsn)
	require.Equal(t, lsn, repl.SafeLSN())
}

func TestWALReplicationPerReplicaCursorNamespace(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-replica-cursors")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	repl := db.Replication()

	curA := WALCursor{Fid: 10, Offset: 20, Len: 5}
	curB := WALCursor{Fid: 11, Offset: 30, Len: 7}
	require.NoError(t, repl.SetCursorFor("replica-a", curA))
	require.NoError(t, repl.SetCursorFor("replica-b", curB))

	gotA, err := repl.CursorFor("replica-a")
	require.NoError(t, err)
	gotB, err := repl.CursorFor("replica-b")
	require.NoError(t, err)
	gotDefault, err := repl.Cursor()
	require.NoError(t, err)

	require.Equal(t, curA, gotA)
	require.Equal(t, curB, gotB)
	require.True(t, gotDefault.IsZero())
}

func TestApplyWALStreamHelper(t *testing.T) {
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

	dst, err := Open(getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()
	require.NoError(t, dst.Replication().SetRole(ReplicationRoleReplica))
	repl := dst.Replication()

	cur, err := repl.ApplyStream(func(emit func(*WALFrame) error) error {
		return src.StreamWALFrom(WALCursor{}, emit)
	})
	require.NoError(t, err)
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
	require.NoError(t, src.StreamWALFrom(WALCursor{}, func(f *WALFrame) error {
		frames = append(frames, f)
		return nil
	}))
	require.GreaterOrEqual(t, len(frames), 3)
	require.NoError(t, src.Close())

	dst, err := Open(getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()
	require.NoError(t, dst.Replication().SetRole(ReplicationRoleReplica))
	repl := dst.Replication()

	_, err = repl.ApplyFramesWithOptions([]*WALFrame{frames[0], frames[2]}, WALApplyOptions{StrictCursorContinuity: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "gap detected")
}

func TestApplyWALFramesReplicaNamespacesIsolatePendingIntents(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-replica-intents")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Replication().SetRole(ReplicationRoleReplica))
	repl := db.Replication()

	txnID := uint64(42)

	intentA := &WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnIntent,
		Lsn:     walCursorToPB(WALCursor{Fid: 1, Offset: 20, Len: 10}),
		Key:     buildTxnIntentKey(txnID, 0),
		Value:   encodeTxnIntentValue(NewEntry([]byte("a-key"), []byte("a-val"))),
	}
	intentB := &WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnIntent,
		Lsn:     walCursorToPB(WALCursor{Fid: 1, Offset: 20, Len: 10}),
		Key:     buildTxnIntentKey(txnID, 0),
		Value:   encodeTxnIntentValue(NewEntry([]byte("b-key"), []byte("b-val"))),
	}
	commitA := &WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnCommit,
		Lsn:     walCursorToPB(WALCursor{Fid: 1, Offset: 40, Len: 10}),
		Key:     buildTxnCommitKey(txnID),
		Value:   encodeTxnCommitValue(600, txnID),
	}
	commitB := &WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameTxnCommit,
		Lsn:     walCursorToPB(WALCursor{Fid: 1, Offset: 40, Len: 10}),
		Key:     buildTxnCommitKey(txnID),
		Value:   encodeTxnCommitValue(500, txnID),
	}

	_, err = repl.ApplyFramesFor("replica-a", []*WALFrame{intentA})
	require.NoError(t, err)
	_, err = repl.ApplyFramesFor("replica-b", []*WALFrame{intentB})
	require.NoError(t, err)
	_, err = repl.ApplyFramesFor("replica-b", []*WALFrame{commitB})
	require.NoError(t, err)

	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("a-key"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		item, err := txn.Get([]byte("b-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("b-val"), getItemValue(t, item))
		return nil
	}))

	_, err = repl.ApplyFramesFor("replica-a", []*WALFrame{commitA})
	require.NoError(t, err)

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("a-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("a-val"), getItemValue(t, item))
		return nil
	}))
}

func TestApplyFramesSkipsDuplicateLSNInBatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-wal-dup-lsn")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Replication().SetRole(ReplicationRoleReplica))

	repl := db.Replication()
	lsn := walCursorToPB(WALCursor{Fid: 1, Offset: 20, Len: 10})
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
			Lsn:     walCursorToPB(WALCursor{Fid: 1, Offset: 20, Len: 10}),
			Key:     y.KeyWithTs([]byte("dup-b"), 50),
			Value:   []byte("value-b"),
		},
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

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.Equal(t, ReplicationRolePrimary, db.Replication().Role())

	require.NoError(t, db.Replication().SetRole(ReplicationRoleReplica))
	require.Equal(t, ReplicationRoleReplica, db.Replication().Role())

	err = db.Update(func(txn *Txn) error {
		return txn.Set([]byte("blocked"), []byte("v"))
	})
	require.ErrorIs(t, err, ErrReadOnlyReplica)

	repl := db.Replication()
	frame := &WALFrame{
		Version: WALFrameVersion,
		Type:    WALFrameEntry,
		Lsn:     walCursorToPB(WALCursor{Fid: 1, Offset: 20, Len: 10}),
		Key:     y.KeyWithTs([]byte("from-repl"), 10),
		Value:   []byte("ok"),
	}
	_, err = repl.ApplyFrame(frame)
	require.NoError(t, err)

	require.NoError(t, db.Replication().SetRole(ReplicationRolePrimary))
	require.Equal(t, ReplicationRolePrimary, db.Replication().Role())

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("user-write"), []byte("ok"))
	}))

	_, err = repl.ApplyFrame(frame)
	require.ErrorIs(t, err, ErrPrimaryRoleApply)
}

func TestNewTransactionForcesReadOnlyInReplicaRole(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-repl-update-early")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.Replication().SetRole(ReplicationRoleReplica))
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

	dst, err := Open(getTestOptions(dstDir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close()) }()
	require.NoError(t, dst.Replication().SetRole(ReplicationRoleReplica))

	repl := dst.Replication()
	const replicaID = "replica-integration"

	ship := func() WALCursor {
		start, err := repl.CursorFor(replicaID)
		require.NoError(t, err)
		last := start
		err = src.StreamWALFrom(start, func(f *WALFrame) error {
			buf, err := repl.MarshalFrame(f)
			if err != nil {
				return err
			}
			last, err = repl.ApplyFrameBytesFor(replicaID, buf)
			if err != nil {
				return err
			}
			return nil
		})
		require.NoError(t, err)
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

	stored, err := repl.CursorFor(replicaID)
	require.NoError(t, err)
	require.Equal(t, third, stored)
}
