/*
 * Copyright 2026 Linka Cloud and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.linka.cloud/badger/v3/y"
)

func TestTxLogReplayCommittedIntents(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-replay")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)

	intent1 := &Entry{
		Key:   buildTxnIntentKey(1, 0),
		Value: encodeTxnIntentValue(NewEntry([]byte("k1"), []byte("v1"))),
		meta:  bitTxnIntent,
	}
	intent2 := &Entry{
		Key:   buildTxnIntentKey(1, 1),
		Value: encodeTxnIntentValue(NewEntry([]byte("k2"), []byte("v2"))),
		meta:  bitTxnIntent,
	}
	_, err = db.appendTxnIntent(intent1)
	require.NoError(t, err)
	_, err = db.appendTxnIntent(intent2)
	require.NoError(t, err)
	require.NoError(t, db.appendTxnCommit(1, 42))
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	err = db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("k1"))
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), getItemValue(t, item))

		item, err = txn.Get([]byte("k2"))
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), getItemValue(t, item))
		return nil
	})
	require.NoError(t, err)
}

func TestDropPrefixClearsTxLogReplay(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-dropprefix")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		require.NoError(t, txn.SetEntry(NewEntry([]byte("aa-key"), []byte("aa-val"))))
		require.NoError(t, txn.SetEntry(NewEntry([]byte("bb-key"), []byte("bb-val"))))
		return nil
	}))
	require.NoError(t, db.DropPrefix([]byte("aa-")))
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("aa-key"))
		require.Equal(t, ErrKeyNotFound, err)

		item, err := txn.Get([]byte("bb-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("bb-val"), getItemValue(t, item))
		return nil
	}))
}

func TestDropAllClearsTxLogReplay(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-dropall")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	txnSet(t, db, []byte("k-dropall"), []byte("v"), 0x00)
	require.NoError(t, db.DropAll())
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("k-dropall"))
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))
}

func TestTxLogReplayIgnoresUncommittedIntents(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-uncommitted")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)

	intent := &Entry{
		Key:   buildTxnIntentKey(99, 0),
		Value: encodeTxnIntentValue(NewEntry([]byte("u-key"), []byte("u-val"))),
		meta:  bitTxnIntent,
	}
	_, err = db.appendTxnIntent(intent)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("u-key"))
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))
}

func TestTxLogReplayHandlesTruncatedTail(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-tail")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	txnSet(t, db, []byte("tail-key"), []byte("tail-val"), 0x00)
	lastFid := db.wal.maxFid
	txPath := walFilePath(dir, lastFid)
	require.NoError(t, db.Close())

	f, err := os.OpenFile(txPath, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("tail-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("tail-val"), getItemValue(t, item))
		return nil
	}))
}

func TestTxLogReplayIsIdempotent(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-idempotent")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	txnSet(t, db, []byte("idem-key"), []byte("idem-val"), 0x00)
	require.NoError(t, db.Close())

	countVersions := func(db *DB, key []byte) int {
		t.Helper()
		iopts := DefaultIteratorOptions
		iopts.AllVersions = true
		iopts.Prefix = key
		iopts.PrefetchValues = false

		count := 0
		require.NoError(t, db.View(func(txn *Txn) error {
			it := txn.NewIterator(iopts)
			defer it.Close()
			for it.Rewind(); it.ValidForPrefix(key); it.Next() {
				if bytes.Equal(it.Item().Key(), key) {
					count++
				}
			}
			return nil
		}))
		return count
	}

	db, err = Open(opt)
	require.NoError(t, err)
	require.Equal(t, 1, countVersions(db, []byte("idem-key")))
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.Equal(t, 1, countVersions(db, []byte("idem-key")))
}

func TestTxLogReplayMixedCommitWindows(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-mixed-windows")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)

	// Txn 1: committed.
	_, err = db.appendTxnIntent(&Entry{
		Key:   buildTxnIntentKey(100, 0),
		Value: encodeTxnIntentValue(NewEntry([]byte("mix-key"), []byte("v-committed"))),
		meta:  bitTxnIntent,
	})
	require.NoError(t, err)
	require.NoError(t, db.appendTxnCommit(100, 1000))

	// Txn 2: intent present, commit missing.
	_, err = db.appendTxnIntent(&Entry{
		Key:   buildTxnIntentKey(101, 0),
		Value: encodeTxnIntentValue(NewEntry([]byte("mix-key"), []byte("v-uncommitted"))),
		meta:  bitTxnIntent,
	})
	require.NoError(t, err)

	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("mix-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("v-committed"), getItemValue(t, item))
		return nil
	}))
}

func TestTxLogCommitFailsWhenWritesBlocked(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-blocked")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)

	require.NoError(t, db.blockWrite())
	txn := db.NewTransaction(true)
	require.NoError(t, txn.Set([]byte("blocked-key"), []byte("blocked-val")))
	err = txn.Commit()
	require.Equal(t, ErrBlockedWrites, err)
	db.unblockWrite()

	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("blocked-key"))
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))
}

func TestTxLogConcurrentCommits(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-concurrent")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	const goroutines = 8
	const perG = 60
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := []byte(fmt.Sprintf("txlog-c-%d-%d", g, i))
				val := []byte(fmt.Sprintf("v-%d-%d", g, i))
				require.NoError(t, db.Update(func(txn *Txn) error {
					return txn.Set(key, val)
				}))
			}
		}()
	}
	wg.Wait()

	require.NoError(t, db.View(func(txn *Txn) error {
		for g := 0; g < goroutines; g++ {
			for i := 0; i < perG; i++ {
				key := []byte(fmt.Sprintf("txlog-c-%d-%d", g, i))
				item, err := txn.Get(key)
				require.NoError(t, err)
				require.Equal(t, []byte(fmt.Sprintf("v-%d-%d", g, i)), getItemValue(t, item))
			}
		}
		return nil
	}))
}

func TestTxLogCommitDuringDropAllReturnsBlocked(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-dropall-race")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	txnSet(t, db, []byte("race-keep"), []byte("v"), 0x00)

	// Hold db.lock so DropAll is forced to pause after blocking writes. This
	// makes the blocked-writes window deterministic for the commit below.
	db.lock.Lock()
	lockReleased := false
	defer func() {
		if !lockReleased {
			db.lock.Unlock()
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- db.DropAll()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&db.blockWrites) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.EqualValues(t, 1, atomic.LoadInt32(&db.blockWrites))

	txn := db.NewTransaction(true)
	require.NoError(t, txn.Set([]byte("race-key"), []byte("race-val")))
	err = txn.Commit()
	require.Equal(t, ErrBlockedWrites, err)

	db.lock.Unlock()
	lockReleased = true

	require.NoError(t, <-done)
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("race-keep"))
		require.Equal(t, ErrKeyNotFound, err)
		_, err = txn.Get([]byte("race-key"))
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))
}

func TestTxLogCommitDuringDropPrefixReturnsBlocked(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-dropprefix-race")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	txnSet(t, db, []byte("pfx-a-key"), []byte("a"), 0x00)
	txnSet(t, db, []byte("pfx-b-key"), []byte("b"), 0x00)

	done := make(chan error, 1)
	go func() {
		done <- db.DropPrefix([]byte("pfx-a-"))
	}()

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&db.blockWrites) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.EqualValues(t, 1, atomic.LoadInt32(&db.blockWrites))

	txn := db.NewTransaction(true)
	require.NoError(t, txn.Set([]byte("pfx-race"), []byte("rv")))
	err = txn.Commit()
	require.Equal(t, ErrBlockedWrites, err)

	require.NoError(t, <-done)
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("pfx-a-key"))
		require.Equal(t, ErrKeyNotFound, err)
		item, err := txn.Get([]byte("pfx-b-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("b"), getItemValue(t, item))
		_, err = txn.Get([]byte("pfx-race"))
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))
}

func TestTxLogPointerPathSkipsVlogPayloadWrites(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-pointer")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("ptr-key"), []byte("ptr-val"))
	}))

	vs, err := db.get(y.KeyWithTs([]byte("ptr-key"), math.MaxUint64))
	require.NoError(t, err)
	require.True(t, vs.Meta&bitValuePointer > 0)
	require.True(t, vs.Meta&bitWALPointer > 0)
	require.Equal(t, int(vptrSize), len(vs.Value))

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("ptr-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("ptr-val"), getItemValue(t, item))
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestTxLogManifestSafePointPersisted(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-safepoint")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("sp-key"), []byte("sp-val"))
	}))
	require.NoError(t, db.Close())

	_, err = os.Stat(filepath.Join(dir, walSafePointFile))
	require.NoError(t, err)
}

func TestTxLogDropAllClearsSafePointAndLSN(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-dropall-safepoint")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)

	spPath := filepath.Join(dir, walSafePointFile)
	require.NoError(t, os.WriteFile(spPath, valuePointer{Fid: 123, Len: 1, Offset: 456}.Encode(), 0600))
	require.NoError(t, db.DropAll())

	_, err = os.Stat(spPath)
	require.True(t, os.IsNotExist(err))

	durable, applied := db.WALLSN()
	require.True(t, durable.IsZero())
	require.True(t, applied.IsZero())
	require.NoError(t, db.Close())
}

func TestRunWALGCCompactsOldWALFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-walgc")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)
	opt.ValueThreshold = 0

	db, err := Open(opt)
	require.NoError(t, err)

	key := []byte("walgc-key")
	val := make([]byte, 2048)
	for i := range val {
		val[i] = byte(i % 251)
	}
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set(key, val)
	}))

	before, err := db.get(y.KeyWithTs(key, math.MaxUint64))
	require.NoError(t, err)
	require.True(t, before.Meta&bitWALPointer > 0)
	require.Equal(t, int(vptrSize), len(before.Value))

	for i := 0; i < 6; i++ {
		prefix := []byte(fmt.Sprintf("walgc-%02d-", i))
		key := append(append([]byte{}, prefix...), []byte("key")...)
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(key, []byte("value"))
		}))
		require.NoError(t, db.DropPrefix(prefix))
	}

	oldestFid := uint32(0)
	for fid := range db.wal.files {
		if oldestFid == 0 || fid < oldestFid {
			oldestFid = fid
		}
	}
	require.NotZero(t, oldestFid)

	require.NoError(t, db.RunWALGC(0.0001))

	after, err := db.get(y.KeyWithTs(key, math.MaxUint64))
	require.NoError(t, err)
	require.True(t, after.Meta&bitWALPointer > 0)
	require.Equal(t, int(vptrSize), len(after.Value))
	var newPtr valuePointer
	newPtr.Decode(after.Value)

	_, err = os.Stat(walFilePath(dir, oldestFid))
	require.True(t, os.IsNotExist(err))

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get(key)
		require.NoError(t, err)
		require.Equal(t, val, getItemValue(t, item))
		return nil
	}))

	seenNoRewrite := false
	for i := 0; i < 20; i++ {
		err := db.RunWALGC(0.0001)
		if err == ErrNoRewrite {
			seenNoRewrite = true
			break
		}
		require.NoError(t, err)
	}
	require.True(t, seenNoRewrite)
	require.NoError(t, db.Close())
}

func TestRunWALGCRequiresWALMode(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-walgc-mode")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(false))
	require.NoError(t, err)
	require.Equal(t, ErrInvalidRequest, db.RunWALGC(0.0001))
	require.NoError(t, db.Close())
}

func TestRunWALGCRejectsInvalidRatio(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-walgc-ratio")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)
	require.Equal(t, ErrInvalidRequest, db.RunWALGC(0))
	require.Equal(t, ErrInvalidRequest, db.RunWALGC(1))
	require.Equal(t, ErrInvalidRequest, db.RunWALGC(-0.1))
	require.NoError(t, db.Close())
}

func countWALFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	count := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), walFileExt) {
			count++
		}
	}
	return count
}

func TestRunWALGCBoundedGrowthOverCycles(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-walgc-bounded")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)
	opt.ValueThreshold = 0
	opt.ValueLogFileSize = 1 << 20

	db, err := Open(opt)
	require.NoError(t, err)

	maxWalFiles := 0
	for cycle := 0; cycle < 20; cycle++ {
		prefix := fmt.Sprintf("cycle-%02d-", cycle)
		for i := 0; i < 60; i++ {
			k := []byte(fmt.Sprintf("%s%03d", prefix, i))
			v := bytes.Repeat([]byte{byte((cycle+i)%251 + 1)}, 4<<10)
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set(k, v)
			}))
		}

		if cycle > 2 {
			oldPrefix := []byte(fmt.Sprintf("cycle-%02d-", cycle-3))
			require.NoError(t, db.DropPrefix(oldPrefix))
		}

		for i := 0; i < 8; i++ {
			err := db.RunWALGC(0.0001)
			if err == ErrNoRewrite {
				break
			}
			require.NoError(t, err)
		}
		walFiles := countWALFiles(t, dir)
		if walFiles > maxWalFiles {
			maxWalFiles = walFiles
		}
		require.LessOrEqual(t, walFiles, 20)
	}
	require.LessOrEqual(t, maxWalFiles, 20)

	for cycle := 17; cycle < 20; cycle++ {
		prefix := fmt.Sprintf("cycle-%02d-", cycle)
		for i := 0; i < 10; i++ {
			k := []byte(fmt.Sprintf("%s%03d", prefix, i))
			require.NoError(t, db.View(func(txn *Txn) error {
				item, err := txn.Get(k)
				require.NoError(t, err)
				v := getItemValue(t, item)
				require.Len(t, v, 4<<10)
				return nil
			}))
		}
	}

	require.NoError(t, db.Close())
}

func TestRunWALGCIteratorSafeDeletion(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-walgc-iter")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)
	opt.ValueThreshold = 0
	opt.ValueLogFileSize = 1 << 20

	db, err := Open(opt)
	require.NoError(t, err)

	valueSize := 16 << 10
	value3 := make([]byte, valueSize)
	for i := 0; i < 120; i++ {
		v := bytes.Repeat([]byte{byte(i%251 + 1)}, valueSize)
		if i == 3 {
			copy(value3, v)
		}
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("key%03d", i)), v)
		}))
	}

	txn := db.NewTransaction(false)
	it := txn.NewIterator(IteratorOptions{PrefetchValues: false, PrefetchSize: 0})
	it.Rewind()
	require.True(t, it.Valid())
	require.Equal(t, []byte("key000"), it.Item().Key())
	it.Next()
	require.True(t, it.Valid())
	require.Equal(t, []byte("key001"), it.Item().Key())
	it.Next()
	require.True(t, it.Valid())
	require.Equal(t, []byte("key002"), it.Item().Key())

	require.NoError(t, db.RunWALGC(0.0001))
	require.NotEmpty(t, db.wal.filesToBeDeleted)
	pendingFid := db.wal.filesToBeDeleted[0]
	_, err = os.Stat(walFilePath(dir, pendingFid))
	require.NoError(t, err)

	it.Next()
	require.True(t, it.Valid())
	require.Equal(t, []byte("key003"), it.Item().Key())
	v3, err := it.Item().ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, value3, v3)

	it.Close()
	txn.Discard()

	_, err = os.Stat(walFilePath(dir, pendingFid))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, db.Close())
}

func TestRunWALGCRejectedWhenBusyAndAfterClose(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-walgc-rejected")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(getTestOptions(dir).WithWAL(true).WithSyncWrites(true))
	require.NoError(t, err)

	db.wal.garbageCh <- struct{}{}
	require.Equal(t, ErrRejected, db.RunWALGC(0.0001))
	<-db.wal.garbageCh

	require.NoError(t, db.Close())
	require.Equal(t, ErrRejected, db.RunWALGC(0.0001))
}

func TestWALReadIntentValueDoesNotDecodeRawPayload(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-wal-raw-payload")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)
	opt.ValueThreshold = 0

	db, err := Open(opt)
	require.NoError(t, err)

	key := []byte("raw-payload")
	val := make([]byte, 30)
	val[0] = 7
	val[29] = 9
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set(key, val)
	}))

	vs, err := db.get(y.KeyWithTs(key, math.MaxUint64))
	require.NoError(t, err)
	require.True(t, vs.Meta&bitWALPointer > 0)

	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get(key)
		require.NoError(t, err)
		require.Equal(t, val, getItemValue(t, item))
		return nil
	}))

	require.NoError(t, db.Close())
}

func TestTxLogStreamVersionedFrames(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txlog-stream")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)

	_, err = db.appendTxnIntent(&Entry{
		Key:   buildTxnIntentKey(701, 0),
		Value: encodeTxnIntentValue(NewEntry([]byte("stream-key"), []byte("stream-val"))),
		meta:  bitTxnIntent,
	})
	require.NoError(t, err)
	require.NoError(t, db.appendTxnCommit(701, 9001))

	var types []WALFrameType
	require.NoError(t, db.StreamWALFrom(valuePointer{}, func(f WALFrame) error {
		require.Equal(t, WALFrameVersion, f.Version)
		types = append(types, f.Type)
		return nil
	}))
	require.NotEmpty(t, types)
	hasIntent := false
	hasCommit := false
	for _, tp := range types {
		if tp == WALFrameTxnIntent {
			hasIntent = true
		}
		if tp == WALFrameTxnCommit {
			hasCommit = true
		}
	}
	require.True(t, hasIntent)
	require.True(t, hasCommit)
	require.NoError(t, db.Close())
}

func TestWALModeSwitchBlockedByDefault(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-walmode-blocked")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).WithWAL(false)
	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = Open(getTestOptions(dir).WithWAL(true))
	require.Error(t, err)
	require.Contains(t, err.Error(), ErrWALModeMismatch.Error())
}

func TestWALModeSwitchAllowedWhenRequested(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-walmode-allowed")
	require.NoError(t, err)
	defer removeDir(dir)

	require.NoError(t, os.WriteFile(filepath.Join(dir, walModeFile), []byte(walModeMem+"\n"), 0600))
	db, err := Open(getTestOptions(dir).WithWAL(true).WithAllowWALModeSwitch(true))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	b, err := os.ReadFile(filepath.Join(dir, walModeFile))
	require.NoError(t, err)
	require.Equal(t, walModeTx, strings.TrimSpace(string(b)))
}

func TestTxWalModeCreatesNoVlogFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txwal-novlog")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).
		WithWAL(true).
		WithSyncWrites(true)

	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("novlog-k"), []byte("novlog-v"))
	}))
	require.NoError(t, db.Close())

	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, ent := range ents {
		require.False(t, strings.HasSuffix(ent.Name(), ".vlog"))
		require.False(t, strings.HasSuffix(ent.Name(), ".mem"))
	}
}

func TestTxWalModeRejectsExistingVlogFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txwal-reject-vlog")
	require.NoError(t, err)
	defer removeDir(dir)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.vlog"), []byte("x"), 0600))
	_, err = Open(getTestOptions(dir).WithWAL(true))
	require.Error(t, err)
	require.Contains(t, err.Error(), ErrWALModeMismatch.Error())
}

func TestTxWalModeRejectsExistingMemFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-txwal-reject-mem")
	require.NoError(t, err)
	defer removeDir(dir)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "00001.mem"), []byte("x"), 0600))
	_, err = Open(getTestOptions(dir).WithWAL(true))
	require.Error(t, err)
	require.Contains(t, err.Error(), ErrWALModeMismatch.Error())
}
