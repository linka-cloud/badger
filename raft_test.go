package badger

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/shaj13/raft"
	"github.com/stretchr/testify/require"
)

func TestRaft(t *testing.T) {
	state := filepath.Join(os.TempDir(), "badger-raft/state")
	data := filepath.Join(os.TempDir(), "badger-raft/data")
	_ = data
	require.NoError(t, os.MkdirAll(state, 0o700))
	t.Logf("state: %q", state)

	ready := make(chan struct{})
	db, err := Open(DefaultOptions("").
		WithInMemory(true).
		WithLogger(defaultLogger(INFO)).
		WithRaft(DefaultRaftOptions.
			WithAddr("127.0.0.1:8765").
			WithStateDir(state).
			WithTickInterval(10 * time.Millisecond).
			WithSnapshotInterval(1000).
			WithStartOptions(WithFallback(
				WithInitCluster(),
				WithRestart(),
			)).
			WithStateChange(func(s raft.StateType) {
				if s == raft.StateLeader {
					close(ready)
				}
			}),
		),
	)
	require.NoError(t, err)
	defer db.Close()

	tm := time.NewTimer(120 * time.Second)
	select {
	case <-tm.C:
		t.Fatal("timeout")
	case <-ready:
	}

	start := 0
	t.Log("Looking for existing data")
	require.NoError(t, db.View(func(txn *Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := binary.BigEndian.Uint64(it.Item().Key())
			start = int(k)
			v := make([]byte, it.Item().ValueSize())
			if v, err = it.Item().ValueCopy(v); err != nil {
				return err
			}
		}
		return nil
	}))

	t.Logf("start inserting at %d", start+1)
	count := 1_000
	repeat := 2_000
	errs := make(chan error, repeat)
	for i := 0; i < repeat; i++ {
		i := i
		go func() {
			errs <- func() error {
				s := time.Now()
				// time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
				err := db.Update(func(txn *Txn) error {
					for j := start + 1 + (i * count); j < start+1+(i+1)*count; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(j))
						v := []byte(strconv.Itoa(i))
						err := txn.Set(k, v)
						if err != nil {
							return err
						}
					}
					return nil
				})
				t.Logf("inserted %d in %s", count, time.Since(s))
				return err
			}()
		}()
	}
	for i := 0; i < repeat; i++ {
		require.NoError(t, <-errs)
	}

	t.Logf("retrieving data")
	require.NoError(t, db.View(func(txn *Txn) error {
		start := time.Now()
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			k := binary.BigEndian.Uint64(it.Item().Key())
			v := make([]byte, it.Item().ValueSize())
			if v, err = it.Item().ValueCopy(v); err != nil {
				return err
			}
			_ = k
			count++
			// t.Logf("%d: %q", k, string(v))
		}
		t.Logf("count: %d in %v", count, time.Since(start))
		return nil
	}))
}
