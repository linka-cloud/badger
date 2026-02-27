/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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
	"context"
	"encoding/binary"
	"encoding/hex"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/z"
)

type oracle struct {
	isManaged       bool // Does not change value, so no locking required.
	detectConflicts bool // Determines if the txns should be checked for conflicts.

	sync.Mutex // For nextTxnTs and commits.
	// writeChLock lock is for ensuring that transactions go to the write
	// channel in the same order as their commit timestamps.
	writeChLock sync.Mutex
	nextTxnTs   uint64

	// Used to block NewTransaction, so all previous commits are visible to a new read.
	txnMark *y.WaterMark

	// Either of these is used to determine which versions can be permanently
	// discarded during compaction.
	discardTs uint64       // Used by ManagedDB.
	readMark  *y.WaterMark // Used by DB.

	// committedTxns contains all committed writes (contains fingerprints
	// of keys written and their latest commit counter).
	committedTxns []committedTxn
	lastCleanupTs uint64

	// closer is used to stop watermarks.
	closer *z.Closer
}

type committedTxn struct {
	ts uint64
	// ConflictKeys Keeps track of the entries written at timestamp ts.
	conflictKeys map[uint64]struct{}
}

func newOracle(opt Options) *oracle {
	orc := &oracle{
		isManaged:       opt.managedTxns,
		detectConflicts: opt.DetectConflicts,
		// We're not initializing nextTxnTs and readOnlyTs. It would be done after replay in Open.
		//
		// WaterMarks must be 64-bit aligned for atomic package, hence we must use pointers here.
		// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
		readMark: &y.WaterMark{Name: "badger.PendingReads"},
		txnMark:  &y.WaterMark{Name: "badger.TxnTimestamp"},
		closer:   z.NewCloser(2),
	}
	orc.readMark.Init(orc.closer)
	orc.txnMark.Init(orc.closer)
	return orc
}

func (o *oracle) Stop() {
	o.closer.SignalAndWait()
}

func (o *oracle) readTs() uint64 {
	if o.isManaged {
		panic("ReadTs should not be retrieved for managed DB")
	}

	var readTs uint64
	o.Lock()
	readTs = o.nextTxnTs - 1
	o.readMark.Begin(readTs)
	o.Unlock()

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree
	// process. Not waiting here could mean that some txns which have been
	// committed would not be read.
	y.Check(o.txnMark.WaitForMark(context.Background(), readTs))
	return readTs
}

func (o *oracle) nextTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextTxnTs
}

func (o *oracle) incrementNextTs() {
	o.Lock()
	defer o.Unlock()
	o.nextTxnTs++
}

// Any deleted or invalid versions at or below ts would be discarded during
// compaction to reclaim disk space in LSM tree and thence value log.
func (o *oracle) setDiscardTs(ts uint64) {
	o.Lock()
	defer o.Unlock()
	o.discardTs = ts
	o.cleanupCommittedTransactions()
}

func (o *oracle) discardAtOrBelow() uint64 {
	if o.isManaged {
		o.Lock()
		defer o.Unlock()
		return o.discardTs
	}
	return o.readMark.DoneUntil()
}

// hasConflict must be called while having a lock.
func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}
	for _, committedTxn := range o.committedTxns {
		// If the committedTxn.ts is less than txn.readTs that implies that the
		// committedTxn finished before the current transaction started.
		// We don't need to check for conflict in that case.
		// This change assumes linearizability. Lack of linearizability could
		// cause the read ts of a new txn to be lower than the commit ts of
		// a txn before it (@mrjn).
		if committedTxn.ts <= txn.readTs {
			continue
		}

		for _, ro := range txn.reads {
			if _, has := committedTxn.conflictKeys[ro]; has {
				return true
			}
		}
	}

	return false
}

func (o *oracle) newCommitTs(txn *Txn) (uint64, bool) {
	o.Lock()
	defer o.Unlock()

	if o.hasConflict(txn) {
		return 0, true
	}

	var ts uint64
	if !o.isManaged {
		o.doneRead(txn)
		o.cleanupCommittedTransactions()

		// This is the general case, when user doesn't specify the read and commit ts.
		ts = o.nextTxnTs
		o.nextTxnTs++
		o.txnMark.Begin(ts)

	} else {
		// If commitTs is set, use it instead.
		ts = txn.commitTs
	}

	y.AssertTrue(ts >= o.lastCleanupTs)

	if o.detectConflicts {
		// We should ensure that txns are not added to o.committedTxns slice when
		// conflict detection is disabled otherwise this slice would keep growing.
		o.committedTxns = append(o.committedTxns, committedTxn{
			ts:           ts,
			conflictKeys: txn.conflictKeys,
		})
	}

	return ts, false
}

func (o *oracle) doneRead(txn *Txn) {
	if !txn.doneRead {
		txn.doneRead = true
		o.readMark.Done(txn.readTs)
	}
}

func (o *oracle) cleanupCommittedTransactions() { // Must be called under o.Lock
	if !o.detectConflicts {
		// When detectConflicts is set to false, we do not store any
		// committedTxns and so there's nothing to clean up.
		return
	}
	// Same logic as discardAtOrBelow but unlocked
	var maxReadTs uint64
	if o.isManaged {
		maxReadTs = o.discardTs
	} else {
		maxReadTs = o.readMark.DoneUntil()
	}

	y.AssertTrue(maxReadTs >= o.lastCleanupTs)

	// do not run clean up if the maxReadTs (read timestamp of the
	// oldest transaction that is still in flight) has not increased
	if maxReadTs == o.lastCleanupTs {
		return
	}
	o.lastCleanupTs = maxReadTs

	tmp := o.committedTxns[:0]
	for _, txn := range o.committedTxns {
		if txn.ts <= maxReadTs {
			continue
		}
		tmp = append(tmp, txn)
	}
	o.committedTxns = tmp
}

func (o *oracle) doneCommit(cts uint64) {
	if o.isManaged {
		// No need to update anything.
		return
	}
	o.txnMark.Done(cts)
}

// Txn represents a Badger transaction.
type Txn struct {
	readTs   uint64
	commitTs uint64
	txnID    uint64
	seq      uint64
	db       *DB

	reads []uint64 // contains fingerprints of keys read.
	// contains fingerprints of keys written. This is used for conflict detection.
	conflictKeys map[uint64]struct{}
	readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	pendingWrites   map[string]*txnWrite
	duplicateWrites []*txnWrite
	pinnedMts       map[*memTable]uint32

	numIterators atomic.Int32
	discarded    bool
	doneRead     bool
	update       bool // update is used to conditionally keep track of reads.
}

type txnWalRef struct {
	mt  *memTable
	vp  valuePointer
	val []byte
}

var txnIntentPrefix = append(append([]byte{}, badgerPrefix...), []byte("txn-intent")...)

func buildTxnIntentKey(txnID, seq uint64) []byte {
	out := make([]byte, len(txnIntentPrefix)+16)
	copy(out, txnIntentPrefix)
	binary.BigEndian.PutUint64(out[len(txnIntentPrefix):], txnID)
	binary.BigEndian.PutUint64(out[len(txnIntentPrefix)+8:], seq)
	return out
}

func parseTxnIntentKey(key []byte) (uint64, bool) {
	if len(key) != len(txnIntentPrefix)+16 {
		return 0, false
	}
	if !bytes.Equal(key[:len(txnIntentPrefix)], txnIntentPrefix) {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(txnIntentPrefix):]), true
}

func encodeTxnIntentValue(e *Entry) []byte {
	klen := len(e.Key)
	vlen := len(e.Value)
	out := make([]byte, 2+8+8+4+4+klen+vlen)
	out[0] = e.meta
	out[1] = e.UserMeta
	binary.BigEndian.PutUint64(out[2:], e.ExpiresAt)
	binary.BigEndian.PutUint64(out[10:], e.version)
	binary.BigEndian.PutUint32(out[18:], uint32(klen))
	binary.BigEndian.PutUint32(out[22:], uint32(vlen))
	copy(out[26:], e.Key)
	copy(out[26+klen:], e.Value)
	return out
}

func decodeTxnIntentValue(v []byte) (*Entry, bool) {
	if len(v) < 26 {
		return nil, false
	}
	klen := binary.BigEndian.Uint32(v[18:])
	vlen := binary.BigEndian.Uint32(v[22:])
	need := 26 + int(klen) + int(vlen)
	if need > len(v) {
		return nil, false
	}
	start := 26
	e := &Entry{
		meta:      v[0],
		UserMeta:  v[1],
		ExpiresAt: binary.BigEndian.Uint64(v[2:]),
		version:   binary.BigEndian.Uint64(v[10:]),
		Key:       y.SafeCopy(nil, v[start:start+int(klen)]),
		Value:     y.SafeCopy(nil, v[start+int(klen):need]),
	}
	return e, true
}

func encodeTxnCommitValue(commitTs, txnID uint64) []byte {
	out := make([]byte, 17)
	out[0] = 1
	binary.BigEndian.PutUint64(out[1:], commitTs)
	binary.BigEndian.PutUint64(out[9:], txnID)
	return out
}

func decodeTxnCommitValue(v []byte) (uint64, uint64, bool) {
	if len(v) == 17 && v[0] == 1 {
		return binary.BigEndian.Uint64(v[1:]), binary.BigEndian.Uint64(v[9:]), true
	}
	commitTs, err := strconv.ParseUint(string(v), 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return commitTs, 0, true
}

type txnWrite struct {
	key       []byte
	meta      byte
	userMeta  byte
	expiresAt uint64
	version   uint64
	ref       txnWalRef
}

type pendingWritesIterator struct {
	entries  []*txnWrite
	nextIdx  int
	readTs   uint64
	reversed bool
	db       *DB
}

func (pi *pendingWritesIterator) Next() {
	pi.nextIdx++
}

func (pi *pendingWritesIterator) Rewind() {
	pi.nextIdx = 0
}

func (pi *pendingWritesIterator) Seek(key []byte) {
	key = y.ParseKey(key)
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].key, key)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (pi *pendingWritesIterator) Key() []byte {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.KeyWithTs(entry.key, pi.readTs)
}

func (pi *pendingWritesIterator) Value() y.ValueStruct {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	value := pi.db.readTxnValue(entry.ref)
	return y.ValueStruct{
		Value:     value,
		Meta:      entry.meta,
		UserMeta:  entry.userMeta,
		ExpiresAt: entry.expiresAt,
		Version:   pi.readTs,
	}
}

func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

func (pi *pendingWritesIterator) Close() error {
	return nil
}

func (txn *Txn) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	if !txn.update || len(txn.pendingWrites) == 0 {
		return nil
	}
	entries := make([]*txnWrite, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		entries = append(entries, e)
	}
	// Keep writes sorted by user key for iterator semantics inside a transaction.
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].key, entries[j].key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		readTs:   txn.readTs,
		entries:  entries,
		reversed: reversed,
		db:       txn.db,
	}
}

func exceedsSize(prefix string, max int64, key []byte) error {
	return errors.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

func (txn *Txn) modify(e *Entry) error {
	const maxKeySize = 65000

	switch {
	case !txn.update:
		return ErrReadOnlyTxn
	case txn.discarded:
		return ErrDiscardedTxn
	case len(e.Key) == 0:
		return ErrEmptyKey
	case bytes.HasPrefix(e.Key, badgerPrefix):
		return ErrInvalidKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, e.Key)
	case int64(len(e.Value)) > txn.db.opt.ValueLogFileSize:
		return exceedsSize("Value", txn.db.opt.ValueLogFileSize, e.Value)
	case txn.db.opt.InMemory && int64(len(e.Value)) > txn.db.valueThreshold():
		return exceedsSize("Value", txn.db.valueThreshold(), e.Value)
	}

	if err := txn.db.isBanned(e.Key); err != nil {
		return err
	}

	// The txn.conflictKeys is used for conflict detection. If conflict detection
	// is disabled, we don't need to store key hashes in this map.
	if txn.db.opt.DetectConflicts {
		fp := z.MemHash(e.Key) // Avoid dealing with byte arrays.
		txn.conflictKeys[fp] = struct{}{}
	}
	// If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
	// Add the entry to duplicateWrites only if both the entries have different versions. For
	// same versions, we will overwrite the existing entry.
	intent := &Entry{
		Key:       buildTxnIntentKey(txn.txnID, txn.seq),
		Value:     encodeTxnIntentValue(e),
		UserMeta:  0,
		ExpiresAt: 0,
		meta:      bitTxnIntent,
	}
	txn.seq++

	ref, err := txn.db.appendTxnIntent(intent)
	if err != nil {
		return err
	}
	txn.pinMt(ref.mt)

	w := &txnWrite{
		key:       e.Key,
		meta:      e.meta,
		userMeta:  e.UserMeta,
		expiresAt: e.ExpiresAt,
		version:   e.version,
		ref:       ref,
	}

	if oldEntry, ok := txn.pendingWrites[string(e.Key)]; ok && oldEntry.version != w.version {
		txn.duplicateWrites = append(txn.duplicateWrites, oldEntry)
	}
	txn.pendingWrites[string(e.Key)] = w
	return nil
}

// Set adds a key-value pair to the database.
// It will return ErrReadOnlyTxn if update flag was set to false when creating the transaction.
//
// The current transaction keeps a reference to the key and val byte slice
// arguments. Users must not modify key and val until the end of the transaction.
func (txn *Txn) Set(key, val []byte) error {
	return txn.SetEntry(NewEntry(key, val))
}

// SetEntry takes an Entry struct and adds the key-value pair in the struct,
// along with other metadata to the database.
//
// The current transaction keeps a reference to the entry passed in argument.
// Users must not modify the entry until the end of the transaction.
func (txn *Txn) SetEntry(e *Entry) error {
	return txn.modify(e)
}

// Delete deletes a key.
//
// This is done by adding a delete marker for the key at commit timestamp.  Any
// reads happening before this timestamp would be unaffected. Any reads after
// this commit would see the deletion.
//
// The current transaction keeps a reference to the key byte slice argument.
// Users must not modify the key until the end of the transaction.
func (txn *Txn) Delete(key []byte) error {
	e := &Entry{
		Key:  key,
		meta: bitDelete,
	}
	return txn.modify(e)
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if txn.discarded {
		return nil, ErrDiscardedTxn
	}

	if err := txn.db.isBanned(key); err != nil {
		return nil, err
	}

	item = new(Item)
	if txn.update {
		if e, has := txn.pendingWrites[string(key)]; has && bytes.Equal(key, e.key) {
			if isDeletedOrExpired(e.meta, e.expiresAt) {
				return nil, ErrKeyNotFound
			}
			// Fulfill from transaction intent state.
			val := txn.db.readTxnValue(e.ref)
			item.meta = e.meta
			item.val = val
			item.userMeta = e.userMeta
			item.key = key
			item.status = prefetched
			item.version = txn.readTs
			item.expiresAt = e.expiresAt
			// We probably don't need to set db on item here.
			return item, nil
		}
		// Only track reads if this is update txn. No need to track read if txn serviced it
		// internally.
		txn.addReadKey(key)
	}

	seek := y.KeyWithTs(key, txn.readTs)
	vs, err := txn.db.get(seek)
	if err != nil {
		return nil, y.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return nil, ErrKeyNotFound
	}

	item.key = key
	item.version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.vptr = y.SafeCopy(item.vptr, vs.Value)
	item.txn = txn
	item.expiresAt = vs.ExpiresAt
	return item, nil
}

func (txn *Txn) addReadKey(key []byte) {
	if txn.update {
		fp := z.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		txn.readsLock.Lock()
		txn.reads = append(txn.reads, fp)
		txn.readsLock.Unlock()
	}
}

func (txn *Txn) pinMt(mt *memTable) {
	if mt == nil {
		return
	}
	txn.pinnedMts[mt]++
}

// Discard discards a created transaction. This method is very important and must be called. Commit
// method calls this internally, however, calling this multiple times doesn't cause any issues. So,
// this can safely be called via a defer right when transaction is created.
//
// NOTE: If any operations are run on a discarded transaction, ErrDiscardedTxn is returned.
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	if txn.numIterators.Load() > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	txn.discarded = true
	if !txn.db.orc.isManaged {
		txn.db.orc.doneRead(txn)
	}
	for mt, cnt := range txn.pinnedMts {
		for i := uint32(0); i < cnt; i++ {
			mt.DecrRef()
		}
	}
}

func (txn *Txn) commitAndSend() (func() error, error) {
	orc := txn.db.orc
	// Ensure that the order in which we get the commit timestamp is the same as
	// the order in which we push these updates to the write channel. So, we
	// acquire a writeChLock before getting a commit timestamp, and only release
	// it after pushing the entries to it.
	orc.writeChLock.Lock()
	defer orc.writeChLock.Unlock()

	commitTs, conflict := orc.newCommitTs(txn)
	if conflict {
		return nil, ErrConflict
	}

	keepTogether := true
	setVersion := func(e *txnWrite) {
		if e.version == 0 {
			e.version = commitTs
		} else {
			keepTogether = false
		}
	}
	for _, e := range txn.pendingWrites {
		setVersion(e)
	}
	// The duplicateWrites slice will be non-empty only if there are duplicate
	// entries with different versions.
	for _, e := range txn.duplicateWrites {
		setVersion(e)
	}

	buildEntry := func(w *txnWrite) *Entry {
		val := txn.db.readTxnValue(w.ref)
		e := &Entry{
			Key:       y.KeyWithTs(w.key, w.version),
			Value:     val,
			UserMeta:  w.userMeta,
			ExpiresAt: w.expiresAt,
			meta:      w.meta | bitTxnIntent,
		}
		if keepTogether {
			e.meta |= bitTxn
		}
		return e
	}

	batchLimit := txn.db.opt.maxBatchSize
	countLimit := txn.db.opt.maxBatchCount
	if batchLimit <= 0 {
		batchLimit = 64 << 20
	}
	if countLimit <= 0 {
		countLimit = 1024
	}
	batch := make([]*Entry, 0, countLimit)
	var batchSize int64
	var reqs []*request
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		req, err := txn.db.sendToWriteCh(batch)
		if err != nil {
			return err
		}
		reqs = append(reqs, req)
		batch = make([]*Entry, 0, countLimit)
		batchSize = 0
		return nil
	}

	appendEntry := func(e *Entry) error {
		es := e.estimateSizeAndSetThreshold(txn.db.valueThreshold())
		if len(batch) > 0 && (int64(len(batch))+1 >= countLimit || batchSize+es >= batchLimit) {
			if err := flushBatch(); err != nil {
				return err
			}
		}
		batch = append(batch, e)
		batchSize += es
		return nil
	}

	// The following debug information is what led to determining the cause of
	// bank txn violation bug, and it took a whole bunch of effort to narrow it
	// down to here. So, keep this around for at least a couple of months.
	// var b strings.Builder
	// fmt.Fprintf(&b, "Read: %d. Commit: %d. reads: %v. writes: %v. Keys: ",
	// 	txn.readTs, commitTs, txn.reads, txn.conflictKeys)
	for _, e := range txn.pendingWrites {
		if err := appendEntry(buildEntry(e)); err != nil {
			orc.doneCommit(commitTs)
			return nil, err
		}
	}
	for _, e := range txn.duplicateWrites {
		if err := appendEntry(buildEntry(e)); err != nil {
			orc.doneCommit(commitTs)
			return nil, err
		}
	}

	if err := appendEntry(&Entry{
		Key:   y.KeyWithTs(txnKey, commitTs),
		Value: encodeTxnCommitValue(commitTs, txn.txnID),
		meta:  bitFinTxn,
	}); err != nil {
		orc.doneCommit(commitTs)
		return nil, err
	}

	if err := flushBatch(); err != nil {
		orc.doneCommit(commitTs)
		return nil, err
	}
	ret := func() error {
		var err error
		for _, req := range reqs {
			if rerr := req.Wait(); err == nil {
				err = rerr
			}
		}
		// Wait before marking commitTs as done.
		// We can't defer doneCommit above, because it is being called from a
		// callback here.
		orc.doneCommit(commitTs)
		return err
	}
	return ret, nil
}

func (txn *Txn) commitPrecheck() error {
	if txn.discarded {
		return errors.New("Trying to commit a discarded txn")
	}
	keepTogether := true
	for _, e := range txn.pendingWrites {
		if e.version != 0 {
			keepTogether = false
		}
	}

	// If keepTogether is True, it implies transaction markers will be added.
	// In that case, commitTs should not be never be zero. This might happen if
	// someone uses txn.Commit instead of txn.CommitAt in managed mode.  This
	// should happen only in managed mode. In normal mode, keepTogether will
	// always be true.
	if keepTogether && txn.db.opt.managedTxns && txn.commitTs == 0 {
		return errors.New("CommitTs cannot be zero. Please use commitAt instead")
	}
	return nil
}

// Commit commits the transaction, following these steps:
//
// 1. If there are no writes, return immediately.
//
// 2. Check if read rows were updated since txn started. If so, return ErrConflict.
//
// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
//
// 4. Batch up all writes, write them to value log and LSM tree.
//
// 5. If callback is provided, Badger will return immediately after checking
// for conflicts. Writes to the database will happen in the background.  If
// there is a conflict, an error will be returned and the callback will not
// run. If there are no conflicts, the callback will be called in the
// background upon successful completion of writes or any error during write.
//
// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
func (txn *Txn) Commit() error {
	// txn.conflictKeys can be zero if conflict detection is turned off. So we
	// should check txn.pendingWrites.
	if len(txn.pendingWrites) == 0 {
		return nil // Nothing to do.
	}
	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil {
		return err
	}
	defer txn.Discard()

	txnCb, err := txn.commitAndSend()
	if err != nil {
		return err
	}
	// If batchSet failed, LSM would not have been updated. So, no need to rollback anything.

	// TODO: What if some of the txns successfully make it to value log, but others fail.
	// Nothing gets updated to LSM, until a restart happens.
	return txnCb()
}

type txnCb struct {
	commit func() error
	user   func(error)
	err    error
}

func runTxnCallback(cb *txnCb) {
	switch {
	case cb == nil:
		panic("txn callback is nil")
	case cb.user == nil:
		panic("Must have caught a nil callback for txn.CommitWith")
	case cb.err != nil:
		cb.user(cb.err)
	case cb.commit != nil:
		err := cb.commit()
		cb.user(err)
	default:
		cb.user(nil)
	}
}

// CommitWith acts like Commit, but takes a callback, which gets run via a
// goroutine to avoid blocking this function. The callback is guaranteed to run,
// so it is safe to increment sync.WaitGroup before calling CommitWith, and
// decrementing it in the callback; to block until all callbacks are run.
func (txn *Txn) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if len(txn.pendingWrites) == 0 {
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		return
	}

	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil {
		cb(err)
		return
	}

	defer txn.Discard()

	commitCb, err := txn.commitAndSend()
	if err != nil {
		go runTxnCallback(&txnCb{user: cb, err: err})
		return
	}

	go runTxnCallback(&txnCb{user: cb, commit: commitCb})
}

// ReadTs returns the read timestamp of the transaction.
func (txn *Txn) ReadTs() uint64 {
	return txn.readTs
}

// NewTransaction creates a new transaction. Badger supports concurrent execution of transactions,
// providing serializable snapshot isolation, avoiding write skews. Badger achieves this by tracking
// the keys read and at Commit time, ensuring that these read keys weren't concurrently modified by
// another transaction.
//
// For read-only transactions, set update to false. In this mode, we don't track the rows read for
// any changes. Thus, any long running iterations done in this mode wouldn't pay this overhead.
//
// Running transactions concurrently is OK. However, a transaction itself isn't thread safe, and
// should only be run serially. It doesn't matter if a transaction is created by one goroutine and
// passed down to other, as long as the Txn APIs are called serially.
//
// When you create a new transaction, it is absolutely essential to call
// Discard(). This should be done irrespective of what the update param is set
// to. Commit API internally runs Discard, but running it twice wouldn't cause
// any issues.
//
//	txn := db.NewTransaction(false)
//	defer txn.Discard()
//	// Call various APIs.
func (db *DB) NewTransaction(update bool) *Txn {
	return db.newTransaction(update, false)
}

func (db *DB) newTransaction(update, isManaged bool) *Txn {
	if db.opt.ReadOnly && update {
		// DB is read-only, force read-only transaction.
		update = false
	}

	var id uint64
	if update {
		id = db.nextTxnID.Add(1)
	}
	txn := &Txn{
		update: update,
		db:     db,
		txnID:  id,
	}
	if update {
		if db.opt.DetectConflicts {
			txn.conflictKeys = make(map[uint64]struct{})
		}
		txn.pendingWrites = make(map[string]*txnWrite)
		txn.pinnedMts = make(map[*memTable]uint32)
	}
	if !isManaged {
		txn.readTs = db.orc.readTs()
	}
	return txn
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
// If View is used with managed transactions, it would assume a read timestamp of MaxUint64.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	var txn *Txn
	if db.opt.managedTxns {
		txn = db.NewTransactionAt(math.MaxUint64, false)
	} else {
		txn = db.NewTransaction(false)
	}
	defer txn.Discard()

	return fn(txn)
}

// Update executes a function, creating and managing a read-write transaction
// for the user. Error returned by the function is relayed by the Update method.
// Update cannot be used with managed transactions.
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	if db.opt.managedTxns {
		panic("Update can only be used with managedDB=false.")
	}
	txn := db.NewTransaction(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}
