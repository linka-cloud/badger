package badger

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMinAckTracker(t *testing.T) {
	trk := NewMinAckTracker()
	require.True(t, trk.Min().IsZero())

	min := trk.Update(ReplicationAck{ReplicaID: "r1", Cursor: WALCursor{Fid: 2, Offset: 100, Len: 10}})
	require.Equal(t, WALCursor{Fid: 2, Offset: 100, Len: 10}, min)

	min = trk.Update(ReplicationAck{ReplicaID: "r2", Cursor: WALCursor{Fid: 1, Offset: 50, Len: 10}})
	require.Equal(t, WALCursor{Fid: 1, Offset: 50, Len: 10}, min)

	min = trk.Update(ReplicationAck{ReplicaID: "r2", Cursor: WALCursor{Fid: 1, Offset: 40, Len: 10}})
	require.Equal(t, WALCursor{Fid: 1, Offset: 50, Len: 10}, min)

	min = trk.Update(ReplicationAck{ReplicaID: "r2", Cursor: WALCursor{Fid: 3, Offset: 1, Len: 10}})
	require.Equal(t, WALCursor{Fid: 2, Offset: 100, Len: 10}, min)
}

func TestDBFrameStreamContract(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-frame-stream-contract")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("contract-k"), []byte("contract-v"))
	}))

	stream := DBFrameStream{DB: db}
	seen := 0
	require.NoError(t, stream.StreamFrom(context.Background(), WALCursor{}, func(f *WALFrame) error {
		seen++
		return nil
	}))
	require.Greater(t, seen, 0)
}
