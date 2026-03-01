package badger

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamWALFromIsContinuous(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-stream-continuous")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start, _ := db.WALLSN()
	got := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		errCh <- db.StreamWAL(ctx, WALStreamOptions{StartCursor: start, Continuous: true}, func(f *WALFrame) error {
			_ = f
			select {
			case <-got:
			default:
				close(got)
			}
			cancel()
			return nil
		})
	}()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("continuous-k"), []byte("continuous-v"))
	}))

	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for continuous WAL frame")
	}

	err = <-errCh
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamWALFromOptionsOneShotDefault(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-stream-oneshot")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("oneshot-k"), []byte("oneshot-v"))
	}))

	seen := 0
	err = db.StreamWAL(context.Background(), WALStreamOptions{}, func(f *WALFrame) error {
		_ = f
		seen++
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, seen, 0)
}

func TestStreamWALFromOptionsStopCursor(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-stream-stop-cursor")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	for i := 0; i < 3; i++ {
		i := i
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte{byte('a' + i)}, []byte{byte('1' + i)})
		}))
	}

	all := make([]WALCursor, 0, 16)
	err = db.StreamWAL(context.Background(), WALStreamOptions{}, func(f *WALFrame) error {
		all = append(all, walCursorFromPB(f.Lsn))
		return nil
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(all), 3)

	stop := all[len(all)/2]
	last := WALCursor{}
	count := 0
	err = db.StreamWAL(context.Background(), WALStreamOptions{StopCursor: stop}, func(f *WALFrame) error {
		c := walCursorFromPB(f.Lsn)
		require.False(t, stop.Less(c))
		last = c
		count++
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, count, 0)
	require.False(t, stop.Less(last))
	require.False(t, last.Less(stop))
}

func TestStreamWALReturnsErrorWhenStartBehindCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-stream-behind-compaction")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).WithWAL(true).WithSyncWrites(true)
	opt.ValueThreshold = 0
	opt.ValueLogFileSize = 1 << 20

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	for i := 0; i < 220; i++ {
		v := make([]byte, 32<<10)
		for j := range v {
			v[j] = byte((i + j) % 251)
		}
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("behind-k-"+strconv.Itoa(i)), v)
		}))
	}

	minRetain := WALCursor{}
	for i := 0; i < 20; i++ {
		err := db.RunWALGC(0.0001)
		if err != nil && err != ErrNoRewrite {
			require.NoError(t, err)
		}
		minRetain = db.wal.minRetainCursorSnapshot()
		if !minRetain.IsZero() {
			break
		}
	}
	require.False(t, minRetain.IsZero())

	err = db.StreamWAL(context.Background(), WALStreamOptions{}, func(f *WALFrame) error {
		_ = f
		return nil
	})
	require.ErrorIs(t, err, ErrWALCursorCompacted)
}
