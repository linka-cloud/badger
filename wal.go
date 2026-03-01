package badger

import (
	"bytes"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"

	"github.com/dgraph-io/badger/v4/y"
)

const walFileExt = ".wal"
const walSafePointFile = "WAL-SAFEPOINT"

var walCheckpointKey = []byte("!badger!wal-checkpoint")

type wal struct {
	db *DB

	mu     sync.RWMutex
	files  map[uint32]*logFile
	maxFid uint32
	buf    bytes.Buffer

	lastAppendLSN      valuePointer
	durableLSN         valuePointer
	appliedLSN         valuePointer
	replicationSafeLSN valuePointer
	manifestSafeLSN    valuePointer

	garbageCh          chan struct{}
	numActiveIterators int32
	filesToBeDeleted   []uint32
}

func walFilePath(dir string, fid uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", fid, walFileExt))
}

func (l *wal) enabled() bool {
	return l != nil && l.db != nil && l.db.opt.EnableWAL && !l.db.opt.InMemory
}

func (l *wal) init(db *DB) error {
	l.db = db
	if !l.enabled() {
		return nil
	}

	l.files = make(map[uint32]*logFile)
	l.garbageCh = make(chan struct{}, 1)
	if err := l.populateFilesMap(); err != nil {
		return err
	}

	if len(l.files) == 0 {
		if l.db.opt.ReadOnly {
			return nil
		}
		_, err := l.createFile(1)
		return err
	}

	if err := l.loadManifestSafePoint(); err != nil {
		return err
	}
	return nil
}

func (l *wal) safePointPath() string {
	return filepath.Join(l.db.opt.ValueDir, walSafePointFile)
}

func (l *wal) loadManifestSafePoint() error {
	if !l.enabled() {
		return nil
	}
	b, err := os.ReadFile(l.safePointPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return y.Wrap(err, "while reading wal safe point")
	}
	if len(b) < int(vptrSize) {
		return nil
	}
	var vp valuePointer
	vp.Decode(b[:vptrSize])
	l.manifestSafeLSN = vp
	if l.appliedLSN.Less(vp) {
		l.appliedLSN = vp
	}
	if l.durableLSN.Less(vp) {
		l.durableLSN = vp
	}
	return nil
}

func (l *wal) persistManifestSafePoint(vp valuePointer) error {
	if !l.enabled() || l.db.opt.ReadOnly || vp.IsZero() {
		return nil
	}
	tmp := l.safePointPath() + ".tmp"
	buf := vp.Encode()
	if err := os.WriteFile(tmp, buf, 0600); err != nil {
		return y.Wrap(err, "while writing wal safe point temp file")
	}
	if err := os.Rename(tmp, l.safePointPath()); err != nil {
		return y.Wrap(err, "while renaming wal safe point file")
	}
	l.manifestSafeLSN = vp
	l.publishLSNMetricsLocked()
	return nil
}

func (l *wal) updateManifestSafePointFromApplied() error {
	if !l.enabled() {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.appliedLSN.IsZero() || !l.manifestSafeLSN.Less(l.appliedLSN) {
		return nil
	}
	return l.persistManifestSafePoint(l.appliedLSN)
}

func (l *wal) pruneCutoffLSNLocked() valuePointer {
	cutoff := l.manifestSafeLSN
	if cutoff.IsZero() {
		return valuePointer{}
	}
	if !l.appliedLSN.IsZero() && l.appliedLSN.Less(cutoff) {
		cutoff = l.appliedLSN
	}
	if !l.replicationSafeLSN.IsZero() && l.replicationSafeLSN.Less(cutoff) {
		cutoff = l.replicationSafeLSN
	}
	return cutoff
}

func (l *wal) pruneLocked() error {
	if !l.enabled() || l.db.opt.ReadOnly {
		return nil
	}

	cutoff := l.pruneCutoffLSNLocked()
	if cutoff.IsZero() || cutoff.Fid <= 1 {
		return nil
	}

	removed := 0
	for _, fid := range l.sortedFids() {
		if fid >= cutoff.Fid {
			break
		}
		lf := l.files[fid]
		if lf == nil {
			continue
		}
		lf.lock.Lock()
		err := lf.Delete()
		lf.lock.Unlock()
		if err != nil {
			return y.Wrapf(err, "while deleting wal file: %s", lf.path)
		}
		delete(l.files, fid)
		removed++
	}

	if removed > 0 {
		l.db.opt.Infof("WAL checkpoint prune: cutoff_fid=%d removed_files=%d", cutoff.Fid, removed)
	}
	return nil
}

func (l *wal) deleteLogFile(lf *logFile) error {
	if lf == nil {
		return nil
	}
	lf.lock.Lock()
	err := lf.Delete()
	lf.lock.Unlock()
	if err != nil {
		return y.Wrapf(err, "while deleting wal file: %s", lf.path)
	}
	return nil
}

func (l *wal) incrIteratorCount() {
	atomic.AddInt32(&l.numActiveIterators, 1)
}

func (l *wal) iteratorCount() int {
	return int(atomic.LoadInt32(&l.numActiveIterators))
}

func (l *wal) decrIteratorCount() error {
	num := atomic.AddInt32(&l.numActiveIterators, -1)
	if num != 0 {
		return nil
	}

	l.mu.Lock()
	deferred := append([]uint32(nil), l.filesToBeDeleted...)
	l.filesToBeDeleted = nil
	files := make([]*logFile, 0, len(deferred))
	for _, fid := range deferred {
		lf, ok := l.files[fid]
		if !ok {
			continue
		}
		files = append(files, lf)
		delete(l.files, fid)
	}
	l.mu.Unlock()

	for _, lf := range files {
		if err := l.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

func (l *wal) pickCompactionFile(excluded map[uint32]struct{}) *logFile {
	if !l.enabled() || l.db.opt.ReadOnly {
		return nil
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	cutoff := l.pruneCutoffLSNLocked()
	if cutoff.IsZero() || cutoff.Fid <= 1 {
		return nil
	}
	for _, fid := range l.sortedFids() {
		if fid >= cutoff.Fid {
			break
		}
		if _, skip := excluded[fid]; skip {
			continue
		}
		if fid >= l.maxFid {
			continue
		}
		if lf := l.files[fid]; lf != nil {
			return lf
		}
	}
	return nil
}

func (l *wal) shouldRewriteEntry(e Entry, vp valuePointer) (bool, string, error) {
	if isWalCheckpointEntry(e) {
		return false, "internal", nil
	}
	if bytes.HasPrefix(e.Key, badgerPrefix) {
		return false, "internal", nil
	}
	if e.meta&(bitTxnIntent|bitFinTxn|bitTxn) > 0 {
		return false, "txn", nil
	}

	vs, err := l.db.get(e.Key)
	if err != nil {
		return false, "", err
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) || (vs.Meta&bitValuePointer) == 0 || (vs.Meta&bitWALPointer) == 0 {
		return false, "stale", nil
	}
	if len(vs.Value) != int(vptrSize) {
		return false, "stale", nil
	}
	var cur valuePointer
	cur.Decode(vs.Value)
	if cur.Fid != vp.Fid || cur.Offset != vp.Offset {
		return false, "stale", nil
	}
	return true, "", nil
}

func (l *wal) estimateDiscardRatio(lf *logFile) (float64, int64, int64, error) {
	if lf == nil {
		return 0, 0, 0, ErrNoRewrite
	}
	var scannedBytes int64
	var movedBytes int64
	_, err := lf.iterate(true, 0, func(e Entry, vp valuePointer) error {
		scannedBytes += int64(vp.Len)
		move, _, err := l.shouldRewriteEntry(e, vp)
		if err != nil {
			return err
		}
		if move {
			movedBytes += int64(vp.Len)
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if scannedBytes == 0 {
		return 1.0, 0, 0, nil
	}
	discard := float64(scannedBytes-movedBytes) / float64(scannedBytes)
	return discard, scannedBytes, movedBytes, nil
}

func (l *wal) rewrite(lf *logFile) error {
	if lf == nil {
		return ErrNoRewrite
	}
	start := time.Now()
	l.db.opt.Infof("WAL GC rewrite start: fid=%d", lf.fid)

	wb := make([]*Entry, 0, 1024)
	var size int64
	moved := 0
	scanned := 0
	skippedInternal := 0
	skippedTxn := 0
	skippedStale := 0

	flush := func() error {
		if len(wb) == 0 {
			return nil
		}
		if err := l.db.batchSet(wb); err != nil {
			return err
		}
		wb = wb[:0]
		size = 0
		return nil
	}

	_, err := lf.iterate(true, 0, func(e Entry, vp valuePointer) error {
		scanned++
		move, reason, err := l.shouldRewriteEntry(e, vp)
		if err != nil {
			return err
		}
		if !move {
			switch reason {
			case "internal":
				skippedInternal++
			case "txn":
				skippedTxn++
			default:
				skippedStale++
			}
			return nil
		}

		ne := &Entry{
			Key:       append([]byte{}, e.Key...),
			Value:     append([]byte{}, e.Value...),
			UserMeta:  e.UserMeta,
			meta:      e.meta &^ (bitValuePointer | bitWALPointer | bitTxn | bitFinTxn),
			ExpiresAt: e.ExpiresAt,
		}
		es := ne.estimateSizeAndSetThreshold(l.db.valueThreshold()) + int64(len(ne.Value))
		if int64(len(wb)+1) >= l.db.opt.maxBatchCount || size+es >= l.db.opt.maxBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
		wb = append(wb, ne)
		size += es
		moved++
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	current, ok := l.files[lf.fid]
	if !ok || current == nil {
		if moved == 0 {
			return ErrNoRewrite
		}
		return nil
	}
	if l.iteratorCount() == 0 {
		delete(l.files, lf.fid)
		if err := l.deleteLogFile(current); err != nil {
			return err
		}
	} else {
		l.filesToBeDeleted = append(l.filesToBeDeleted, lf.fid)
	}

	if moved == 0 {
		l.db.opt.Infof("WAL GC removed stale file: fid=%d scanned=%d skipped_internal=%d skipped_txn=%d skipped_stale=%d duration=%s", lf.fid, scanned, skippedInternal, skippedTxn, skippedStale, time.Since(start).Truncate(time.Millisecond))
	} else {
		l.db.opt.Infof("WAL GC rewrote file: fid=%d moved_entries=%d scanned=%d skipped_internal=%d skipped_txn=%d skipped_stale=%d duration=%s", lf.fid, moved, scanned, skippedInternal, skippedTxn, skippedStale, time.Since(start).Truncate(time.Millisecond))
	}
	return nil
}

func (l *wal) runGC(discardRatio float64) error {
	if !l.enabled() {
		return ErrInvalidRequest
	}
	start := time.Now()
	select {
	case l.garbageCh <- struct{}{}:
		l.db.opt.Infof("WAL GC start")
		defer func() {
			<-l.garbageCh
		}()
		if err := l.checkpoint(); err != nil {
			l.db.opt.Warningf("WAL GC checkpoint failed after %s: %v", time.Since(start).Truncate(time.Millisecond), err)
			return err
		}
		processed := 0
		skippedByRatio := 0
		excluded := make(map[uint32]struct{})
		for {
			lf := l.pickCompactionFile(excluded)
			if lf == nil {
				if processed == 0 {
					l.db.opt.Infof("WAL GC no rewrite candidate after %s (skipped_by_ratio=%d)", time.Since(start).Truncate(time.Millisecond), skippedByRatio)
					return ErrNoRewrite
				}
				l.db.opt.Infof("WAL GC done in %s; compacted_files=%d skipped_by_ratio=%d", time.Since(start).Truncate(time.Millisecond), processed, skippedByRatio)
				return nil
			}
			discard, scannedBytes, movedBytes, err := l.estimateDiscardRatio(lf)
			if err != nil {
				l.db.opt.Warningf("WAL GC estimation failed on fid=%d after %s: %v", lf.fid, time.Since(start).Truncate(time.Millisecond), err)
				return err
			}
			if discard < discardRatio {
				skippedByRatio++
				excluded[lf.fid] = struct{}{}
				l.db.opt.Infof("WAL GC skipped candidate: fid=%d discard_ratio=%.4f required=%.4f scanned_bytes=%d moved_bytes=%d", lf.fid, discard, discardRatio, scannedBytes, movedBytes)
				continue
			}
			l.db.opt.Infof("WAL GC picked candidate: fid=%d", lf.fid)
			err = l.rewrite(lf)
			if err != nil {
				l.db.opt.Warningf("WAL GC failed on fid=%d after %s: %v", lf.fid, time.Since(start).Truncate(time.Millisecond), err)
				return err
			}
			excluded[lf.fid] = struct{}{}
			processed++
		}
	default:
		l.db.opt.Debugf("WAL GC rejected: another run in progress")
		return ErrRejected
	}
}

func (l *wal) populateFilesMap() error {
	files, err := os.ReadDir(l.db.opt.ValueDir)
	if err != nil {
		return errFile(err, l.db.opt.ValueDir, "Unable to open wal dir")
	}

	for _, file := range files {
		name := file.Name()
		if !strings.HasSuffix(name, walFileExt) {
			continue
		}
		fid, err := strconv.ParseUint(strings.TrimSuffix(name, walFileExt), 10, 32)
		if err != nil {
			return errFile(err, name, "Unable to parse wal file id")
		}

		lf := &logFile{
			fid:      uint32(fid),
			path:     filepath.Join(l.db.opt.ValueDir, name),
			registry: l.db.registry,
			writeAt:  vlogHeaderSize,
			opt:      l.db.opt,
		}
		if err := lf.open(lf.path, l.db.opt.getFileFlags(), 2*l.db.opt.ValueLogFileSize); err != nil {
			if err == z.NewFile {
				continue
			}
			return y.Wrapf(err, "while opening wal file: %s", lf.path)
		}
		l.files[lf.fid] = lf
		if lf.fid > l.maxFid {
			l.maxFid = lf.fid
		}
	}
	return nil
}

func (l *wal) createFile(fid uint32) (*logFile, error) {
	lf := &logFile{
		fid:      fid,
		path:     walFilePath(l.db.opt.ValueDir, fid),
		registry: l.db.registry,
		writeAt:  vlogHeaderSize,
		opt:      l.db.opt,
	}
	err := lf.open(lf.path, os.O_CREATE|l.db.opt.getFileFlags(), 2*l.db.opt.ValueLogFileSize)
	if err != nil && err != z.NewFile {
		return nil, y.Wrapf(err, "while creating wal file: %s", lf.path)
	}
	lf.writeAt = vlogHeaderSize
	l.files[fid] = lf
	l.maxFid = fid
	return lf, nil
}

func (l *wal) append(e *Entry) (valuePointer, error) {
	if !l.enabled() {
		return valuePointer{}, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(e)
}

func (l *wal) appendLocked(e *Entry) (valuePointer, error) {
	if !l.enabled() {
		return valuePointer{}, nil
	}

	lf := l.files[l.maxFid]
	if lf == nil {
		var err error
		lf, err = l.createFile(1)
		if err != nil {
			return valuePointer{}, err
		}
	}

	for {
		l.buf.Reset()
		plen, err := lf.encodeEntry(&l.buf, e, lf.writeAt)
		if err != nil {
			return valuePointer{}, err
		}
		if int(lf.writeAt)+plen+maxHeaderSize >= len(lf.Data) {
			if err := lf.doneWriting(lf.writeAt); err != nil {
				return valuePointer{}, err
			}
			lf, err = l.createFile(l.maxFid + 1)
			if err != nil {
				return valuePointer{}, err
			}
			continue
		}

		start := lf.writeAt
		y.AssertTrue(plen == copy(lf.Data[lf.writeAt:], l.buf.Bytes()))
		lf.writeAt += uint32(plen)
		lf.zeroNextEntry()
		lsn := valuePointer{Fid: lf.fid, Len: uint32(plen), Offset: start}
		l.lastAppendLSN = lsn
		if l.db.opt.SyncWrites {
			if err := lf.Sync(); err != nil {
				return valuePointer{}, y.Wrapf(err, "while syncing wal")
			}
			l.durableLSN = lsn
		}
		if e.meta&bitFinTxn == 0 {
			y.NumWritesVlogAdd(l.db.opt.MetricsEnabled, 1)
			y.NumBytesWrittenVlogAdd(l.db.opt.MetricsEnabled, int64(plen))
		}
		l.publishLSNMetricsLocked()
		return lsn, nil
	}
}

func isWalCheckpointEntry(e Entry) bool {
	return bytes.Equal(e.Key, walCheckpointKey)
}

func (l *wal) readIntentValue(vp valuePointer) ([]byte, error) {
	return l.readIntentValueAt(vp, true)
}

func (l *wal) readIntentValueNoMetrics(vp valuePointer) ([]byte, error) {
	return l.readIntentValueAt(vp, false)
}

func (l *wal) readIntentValueAt(vp valuePointer, countMetrics bool) ([]byte, error) {
	if !l.enabled() {
		return nil, nil
	}

	l.mu.RLock()
	lf := l.files[vp.Fid]
	l.mu.RUnlock()
	if lf == nil {
		return nil, fmt.Errorf("wal file with id %d not found", vp.Fid)
	}

	lf.lock.RLock()
	defer lf.lock.RUnlock()

	buf, err := lf.read(vp)
	if err != nil {
		return nil, err
	}
	e, err := lf.decodeEntry(buf, vp.Offset)
	if err != nil {
		return nil, err
	}
	if countMetrics {
		y.NumReadsVlogAdd(l.db.opt.MetricsEnabled, 1)
		y.NumBytesReadsVlogAdd(l.db.opt.MetricsEnabled, int64(len(buf)))
	}
	if e.meta&bitTxnIntent == 0 {
		return y.SafeCopy(nil, e.Value), nil
	}
	intent, ok := decodeTxnIntentValue(e.Value)
	if !ok {
		return nil, stderrors.New("unable to decode wal transaction intent value")
	}
	return y.SafeCopy(nil, intent.Value), nil
}

func (l *wal) Close() error {
	if !l.enabled() {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	var fids []int
	for fid := range l.files {
		fids = append(fids, int(fid))
	}
	sort.Ints(fids)

	var err error
	for _, id := range fids {
		lf := l.files[uint32(id)]
		lf.lock.Lock()
		offset := int64(-1)
		if !l.db.opt.ReadOnly && lf.fid == l.maxFid {
			offset = int64(lf.writeAt)
		}
		if terr := lf.Close(offset); terr != nil && err == nil {
			err = terr
		}
	}
	return err
}

func (l *wal) sync() error {
	if !l.enabled() {
		return nil
	}

	l.mu.RLock()
	lf := l.files[l.maxFid]
	l.mu.RUnlock()
	if lf == nil {
		return nil
	}
	if err := lf.Sync(); err != nil {
		return err
	}
	l.mu.Lock()
	l.durableLSN = l.lastAppendLSN
	l.publishLSNMetricsLocked()
	l.mu.Unlock()
	return nil
}

func (l *wal) markApplied(vp valuePointer) {
	if !l.enabled() || vp.IsZero() {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.appliedLSN.Less(vp) {
		l.appliedLSN = vp
		l.publishLSNMetricsLocked()
	}
}

func (l *wal) lsn() (valuePointer, valuePointer) {
	if !l.enabled() {
		return valuePointer{}, valuePointer{}
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.durableLSN, l.appliedLSN
}

func (l *wal) setReplicationSafeLSN(vp valuePointer) {
	if !l.enabled() || vp.IsZero() {
		return
	}
	l.mu.Lock()
	if l.replicationSafeLSN.Less(vp) {
		l.replicationSafeLSN = vp
	}
	l.mu.Unlock()
}

func (l *wal) replicationSafeLSNValue() valuePointer {
	if !l.enabled() {
		return valuePointer{}
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.replicationSafeLSN
}

func (l *wal) publishLSNMetricsLocked() {
	base := l.db.opt.ValueDir
	y.WALLSNSet(l.db.opt.MetricsEnabled, base+":durable_fid", int64(l.durableLSN.Fid))
	y.WALLSNSet(l.db.opt.MetricsEnabled, base+":durable_offset", int64(l.durableLSN.Offset))
	y.WALLSNSet(l.db.opt.MetricsEnabled, base+":applied_fid", int64(l.appliedLSN.Fid))
	y.WALLSNSet(l.db.opt.MetricsEnabled, base+":applied_offset", int64(l.appliedLSN.Offset))
	y.WALLSNSet(l.db.opt.MetricsEnabled, base+":manifest_safe_fid", int64(l.manifestSafeLSN.Fid))
	y.WALLSNSet(l.db.opt.MetricsEnabled, base+":manifest_safe_offset", int64(l.manifestSafeLSN.Offset))
}

func (l *wal) sortedFids() []uint32 {
	var fids []uint32
	for fid := range l.files {
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	return fids
}

func (l *wal) replay() error {
	if !l.enabled() || l.db.opt.ReadOnly {
		return nil
	}
	replayStartTime := time.Now()

	type replayStats struct {
		checkpointHits int
		replayed       int
		skippedExists  int
		skippedMarker  int
	}
	stats := replayStats{}

	type replayStart struct {
		fid    uint32
		offset uint32
	}
	start := replayStart{}
	fastStart := false

	l.mu.RLock()
	fids := l.sortedFids()
	files := make([]*logFile, 0, len(fids))
	for _, fid := range fids {
		files = append(files, l.files[fid])
	}
	l.mu.RUnlock()

	l.db.opt.Infof("WAL replay start: files=%d max_fid=%d", len(files), l.maxFid)

	if !l.manifestSafeLSN.IsZero() {
		start = replayStart{fid: l.manifestSafeLSN.Fid, offset: l.manifestSafeLSN.Offset + l.manifestSafeLSN.Len}
		fastStart = true
		stats.checkpointHits = 1
	}

	batchLimit := l.db.opt.maxBatchSize
	countLimit := l.db.opt.maxBatchCount
	if batchLimit <= 0 {
		batchLimit = 64 << 20
	}
	if countLimit <= 0 {
		countLimit = 1024
	}

	batch := make([]*Entry, 0, countLimit)
	var batchSize int64
	flushes := 0
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		flushes++
		l.db.opt.Debugf("WAL replay flush: batch_entries=%d batch_size=%d flush_count=%d", len(batch), batchSize, flushes)
		if err := l.db.applyWALEntries(batch); err != nil {
			return err
		}
		batch = make([]*Entry, 0, countLimit)
		batchSize = 0
		return nil
	}

	entryExists := func(key []byte) bool {
		vs, err := l.db.get(key)
		if err != nil {
			return false
		}
		if vs.Meta == 0 && vs.Value == nil {
			return false
		}
		return vs.Version == y.ParseTs(key)
	}

	appendEntry := func(e Entry, vp valuePointer) error {
		if isWalCheckpointEntry(e) {
			stats.skippedMarker++
			return nil
		}
		if !fastStart && entryExists(e.Key) {
			stats.skippedExists++
			return nil
		}
		ne := &Entry{Key: y.SafeCopy(nil, e.Key), UserMeta: e.UserMeta, meta: e.meta, ExpiresAt: e.ExpiresAt}
		if !vp.IsZero() {
			ne.Value = y.SafeCopy(nil, e.Value)
			if !ne.skipVlogAndSetThreshold(l.db.valueThreshold()) {
				ne.Value = vp.Encode()
				ne.meta |= bitWALPointer
			}
		} else {
			ne.Value = y.SafeCopy(nil, e.Value)
		}
		es := ne.estimateSizeAndSetThreshold(l.db.valueThreshold())
		if len(batch) > 0 && (int64(len(batch))+1 >= countLimit || batchSize+es >= batchLimit) {
			if err := flushBatch(); err != nil {
				return err
			}
		}
		batch = append(batch, ne)
		batchSize += es
		stats.replayed++
		if stats.replayed%100000 == 0 {
			l.db.opt.Infof("WAL replay progress: applied=%d skipped_existing=%d skipped_markers=%d",
				stats.replayed, stats.skippedExists, stats.skippedMarker)
		}
		return nil
	}

	for _, lf := range files {
		if lf == nil {
			continue
		}
		if start.fid > 0 && lf.fid < start.fid {
			continue
		}
		offset := uint32(vlogHeaderSize)
		if start.fid > 0 && lf.fid == start.fid {
			offset = start.offset
		}
		l.db.opt.Infof("WAL replay file start: fid=%d offset=%d", lf.fid, offset)
		fileStart := time.Now()
		beforeApplied := stats.replayed
		beforeSkippedExisting := stats.skippedExists
		beforeSkippedMarkers := stats.skippedMarker
		lf.lock.RLock()
		endOff, err := lf.iterate(true, offset, func(e Entry, vp valuePointer) error {
			return appendEntry(e, vp)
		})
		lf.lock.RUnlock()
		if err != nil {
			return y.Wrapf(err, "while replaying wal file: %s", lf.path)
		}
		l.db.opt.Infof("WAL replay file done: fid=%d start_offset=%d end_offset=%d applied=%d skipped_existing=%d skipped_markers=%d elapsed=%s",
			lf.fid, offset, endOff, stats.replayed-beforeApplied, stats.skippedExists-beforeSkippedExisting,
			stats.skippedMarker-beforeSkippedMarkers, time.Since(fileStart))
		if lf.fid == l.maxFid {
			lf.lock.Lock()
			if err := lf.Truncate(int64(endOff)); err != nil {
				lf.lock.Unlock()
				return y.Wrapf(err, "while truncating wal file: %s", lf.path)
			}
			lf.writeAt = endOff
			lf.lock.Unlock()
		}
	}

	if err := flushBatch(); err != nil {
		return err
	}

	metricKey := l.db.opt.ValueDir
	y.WALReplayAdd(l.db.opt.MetricsEnabled, metricKey+":checkpoints", int64(stats.checkpointHits))
	y.WALReplayAdd(l.db.opt.MetricsEnabled, metricKey+":applied", int64(stats.replayed))
	y.WALReplayAdd(l.db.opt.MetricsEnabled, metricKey+":skipped_existing", int64(stats.skippedExists))
	y.WALReplayAdd(l.db.opt.MetricsEnabled, metricKey+":skipped_markers", int64(stats.skippedMarker))

	l.db.opt.Infof("WAL replay finished: fast_start=%t checkpoints=%d applied=%d skipped_existing=%d skipped_markers=%d start_fid=%d start_offset=%d",
		fastStart, stats.checkpointHits, stats.replayed, stats.skippedExists, stats.skippedMarker, start.fid, start.offset)
	l.db.opt.Infof("WAL replay entries replayed: %d", stats.replayed)
	l.db.opt.Infof("WAL replay duration: %s", time.Since(replayStartTime))

	return l.sync()
}

func (l *wal) checkpoint() error {
	if !l.enabled() || l.db.opt.ReadOnly {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	lf := l.files[l.maxFid]
	if lf == nil {
		var err error
		lf, err = l.createFile(1)
		if err != nil {
			return err
		}
	}
	if err := lf.doneWriting(lf.writeAt); err != nil {
		return err
	}
	_, err := l.createFile(l.maxFid + 1)
	if err != nil {
		return err
	}
	vp, err := l.appendLocked(&Entry{Key: walCheckpointKey, Value: []byte{1}})
	if err != nil {
		return err
	}
	if err := l.persistManifestSafePoint(vp); err != nil {
		return err
	}
	return nil
}

func (l *wal) resetLSNLocked() {
	l.lastAppendLSN = valuePointer{}
	l.durableLSN = valuePointer{}
	l.appliedLSN = valuePointer{}
	l.replicationSafeLSN = valuePointer{}
	l.manifestSafeLSN = valuePointer{}
	l.publishLSNMetricsLocked()
}

func (l *wal) dropAll() (int, error) {
	if !l.enabled() {
		return 0, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	count := 0
	for _, lf := range l.files {
		lf.lock.Lock()
		err := lf.Delete()
		lf.lock.Unlock()
		if err != nil {
			return count, err
		}
		count++
	}
	l.files = make(map[uint32]*logFile)
	l.maxFid = 0
	l.resetLSNLocked()
	if err := os.Remove(l.safePointPath()); err != nil && !os.IsNotExist(err) {
		return count, err
	}

	if l.db.opt.ReadOnly {
		return count, nil
	}
	if _, err := l.createFile(1); err != nil {
		return count, err
	}
	return count, nil
}
