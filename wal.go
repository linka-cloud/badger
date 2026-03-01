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
	nextStreamSubID    uint64
	streamSubscribers  map[uint64]chan *WALFrame

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
	l.streamSubscribers = make(map[uint64]chan *WALFrame)
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
	l.publishLSNMetrics()
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

// pruneCutoffLSN computes the prune cutoff cursor.
// Caller must hold l.mu.
func (l *wal) pruneCutoffLSN() valuePointer {
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

// prune removes WAL files older than the prune cutoff.
// Caller must hold l.mu.
func (l *wal) prune() error {
	if !l.enabled() || l.db.opt.ReadOnly {
		return nil
	}

	cutoff := l.pruneCutoffLSN()
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

// minRetainCursor returns the minimum source WAL cursor to retain.
// Caller must hold l.mu.
func (l *wal) minRetainCursor() WALCursor {
	cutoff := l.pruneCutoffLSN()
	if cutoff.IsZero() || cutoff.Fid <= 1 {
		return WALCursor{}
	}
	return walCursorFromValuePointer(cutoff)
}

func (l *wal) minRetainCursorSnapshot() WALCursor {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.minRetainCursor()
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

	cutoff := l.pruneCutoffLSN()
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

func (l *wal) compactionFileStats(excluded map[uint32]struct{}) (eligible, total int, ratio float64) {
	if !l.enabled() || l.db.opt.ReadOnly {
		return 0, 0, 0
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	total = len(l.files)
	if total == 0 {
		return 0, 0, 0
	}

	cutoff := l.pruneCutoffLSN()
	if cutoff.IsZero() || cutoff.Fid <= 1 {
		return 0, total, 0
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
		if l.files[fid] != nil {
			eligible++
		}
	}
	if total > 0 {
		ratio = float64(eligible) / float64(total)
	}
	return eligible, total, ratio
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
		excluded := make(map[uint32]struct{})
		for {
			eligible, total, fileRatio := l.compactionFileStats(excluded)
			if eligible == 0 {
				if processed == 0 {
					l.db.opt.Infof("WAL GC no rewrite candidate after %s (eligible_files=0 total_files=%d)", time.Since(start).Truncate(time.Millisecond), total)
					return ErrNoRewrite
				}
				l.db.opt.Infof("WAL GC done in %s; compacted_files=%d eligible_files=0 total_files=%d", time.Since(start).Truncate(time.Millisecond), processed, total)
				return nil
			}
			if fileRatio < discardRatio {
				if processed == 0 {
					l.db.opt.Infof("WAL GC skipped by file ratio after %s (eligible_files=%d total_files=%d file_ratio=%.4f required=%.4f)", time.Since(start).Truncate(time.Millisecond), eligible, total, fileRatio, discardRatio)
					return ErrNoRewrite
				}
				l.db.opt.Infof("WAL GC done in %s; compacted_files=%d eligible_files=%d total_files=%d file_ratio=%.4f required=%.4f", time.Since(start).Truncate(time.Millisecond), processed, eligible, total, fileRatio, discardRatio)
				return nil
			}

			lf := l.pickCompactionFile(excluded)
			if lf == nil {
				if processed == 0 {
					l.db.opt.Infof("WAL GC no rewrite candidate after %s (eligible_files=%d total_files=%d)", time.Since(start).Truncate(time.Millisecond), eligible, total)
					return ErrNoRewrite
				}
				l.db.opt.Infof("WAL GC done in %s; compacted_files=%d eligible_files=%d total_files=%d", time.Since(start).Truncate(time.Millisecond), processed, eligible, total)
				return nil
			}
			l.db.opt.Infof("WAL GC picked candidate: fid=%d eligible_files=%d total_files=%d file_ratio=%.4f required=%.4f", lf.fid, eligible, total, fileRatio, discardRatio)
			err := l.rewrite(lf)
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
	if err == z.NewFile {
		if e := l.setDeterministicWALFileHeader(lf); e != nil {
			return nil, e
		}
	}
	lf.writeAt = vlogHeaderSize
	l.files[fid] = lf
	l.maxFid = fid
	return lf, nil
}

func (l *wal) setDeterministicWALFileHeader(lf *logFile) error {
	if lf == nil || len(lf.Data) < vlogHeaderSize {
		return nil
	}
	iv := deterministicWALBaseIV(lf.fid)
	copy(lf.Data[8:20], iv[:])
	lf.baseIV = lf.Data[8:20]
	if l.db.opt.SyncWrites {
		if err := lf.Sync(); err != nil {
			return y.Wrapf(err, "while syncing wal header")
		}
	}
	return nil
}

func deterministicWALBaseIV(fid uint32) [12]byte {
	var iv [12]byte
	iv[0] = byte(fid >> 24)
	iv[1] = byte(fid >> 16)
	iv[2] = byte(fid >> 8)
	iv[3] = byte(fid)
	x := uint64(fid)*0x9e3779b97f4a7c15 + 0xd1b54a32d192ed03
	for i := 4; i < 12; i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		iv[i] = byte(x)
	}
	return iv
}

func (l *wal) append(e *Entry) (valuePointer, error) {
	if !l.enabled() {
		return valuePointer{}, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.doAppend(e)
}

func (l *wal) appendReplicatedAtLSN(e *Entry, expected WALCursor) (valuePointer, error) {
	if !l.enabled() {
		return valuePointer{}, nil
	}
	if e == nil {
		return valuePointer{}, stderrors.New("nil wal entry")
	}
	if expected.IsZero() {
		return valuePointer{}, stderrors.New("missing expected wal cursor")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	expectedVP := valuePointerFromWALCursor(expected)
	return l.doAppendAtLSN(e, expectedVP)
}

// doAppend appends one WAL entry to the active WAL file.
// Caller must hold l.mu.
func (l *wal) doAppend(e *Entry) (valuePointer, error) {
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
		// In sync write mode, flush WAL once per commit marker instead of per frame.
		// The commit frame durability implies durability for all prior frames in the same file.
		if l.db.opt.SyncWrites && (e.meta&bitFinTxn > 0 || e.meta&bitTxnIntent == 0) {
			if err := lf.Sync(); err != nil {
				return valuePointer{}, y.Wrapf(err, "while syncing wal")
			}
			l.durableLSN = lsn
		}
		if e.meta&bitFinTxn == 0 {
			y.NumWritesVlogAdd(l.db.opt.MetricsEnabled, 1)
			y.NumBytesWrittenVlogAdd(l.db.opt.MetricsEnabled, int64(plen))
		}
		frame := newWALFrameFromEntry(*e, lsn)
		if frame != nil && frame.Type == WALFrameCheckpoint {
			mr := l.minRetainCursor()
			if !mr.IsZero() {
				frame.MinRetainCursor = walCursorToPB(mr)
			}
		}
		if l.db.replication.isReplica() {
			l.publishStreamFrame(frame)
			l.publishLSNMetrics()
			return lsn, nil
		}
		if err := l.db.replication.replicate(l.db.replCtx, frame); err != nil {
			return valuePointer{}, err
		}
		l.publishStreamFrame(frame)
		l.publishLSNMetrics()
		return lsn, nil
	}
}

// doAppendAtLSN appends one WAL entry at the exact expected cursor.
// Caller must hold l.mu.
func (l *wal) doAppendAtLSN(e *Entry, expected valuePointer) (valuePointer, error) {
	if !l.enabled() {
		return valuePointer{}, nil
	}
	if expected.IsZero() {
		return valuePointer{}, stderrors.New("missing expected wal cursor")
	}
	if expected.Offset < uint32(vlogHeaderSize) {
		return valuePointer{}, fmt.Errorf("invalid WAL offset %d for fid=%d", expected.Offset, expected.Fid)
	}

	lf := l.files[expected.Fid]
	if lf != nil {
		shouldCheckExisting := expected.Fid < l.maxFid || (expected.Fid == l.maxFid && expected.Offset < lf.writeAt)
		if shouldCheckExisting {
			match, err := l.entryMatchesAt(lf, expected, e)
			if err != nil {
				return valuePointer{}, err
			}
			if match {
				return expected, nil
			}
		}
	}
	if lf == nil {
		if expected.Fid < l.maxFid {
			return valuePointer{}, fmt.Errorf("expected WAL fid %d behind current fid %d", expected.Fid, l.maxFid)
		}
		if cur := l.files[l.maxFid]; cur != nil && cur.fid != expected.Fid {
			if err := cur.doneWriting(cur.writeAt); err != nil {
				return valuePointer{}, err
			}
		}
		var err error
		lf, err = l.createFile(expected.Fid)
		if err != nil {
			return valuePointer{}, err
		}
	}
	if lf.fid != l.maxFid {
		return valuePointer{}, fmt.Errorf("expected active WAL fid %d, got %d", expected.Fid, l.maxFid)
	}
	if lf.writeAt != expected.Offset {
		if lf.writeAt < expected.Offset {
			endOff, err := l.fileEndOffset(lf)
			if err != nil {
				return valuePointer{}, err
			}
			if endOff > lf.writeAt {
				lf.writeAt = endOff
				if lf.writeAt == expected.Offset {
					goto appendAtExpected
				}
			} else if lf.writeAt == uint32(vlogHeaderSize) {
				// Snapshot bootstrap can legitimately resume from a mid-file cursor
				// while the local WAL file is still empty. Allow advancing writeAt
				// to the expected offset in that case.
				lf.writeAt = expected.Offset
				goto appendAtExpected
			}
		}
		return valuePointer{}, fmt.Errorf("wal offset mismatch for fid %d: expected=%d actual=%d", expected.Fid, expected.Offset, lf.writeAt)
	}

appendAtExpected:

	l.buf.Reset()
	plen, err := lf.encodeEntry(&l.buf, e, lf.writeAt)
	if err != nil {
		return valuePointer{}, err
	}
	if uint32(plen) != expected.Len {
		return valuePointer{}, fmt.Errorf("wal length mismatch for fid %d offset %d: expected=%d actual=%d", expected.Fid, expected.Offset, expected.Len, plen)
	}
	required := int(lf.writeAt) + plen + maxHeaderSize
	if required >= len(lf.Data) {
		if err := l.ensureWritableCapacity(lf, required+1); err != nil {
			return valuePointer{}, err
		}
		if required >= len(lf.Data) {
			return valuePointer{}, fmt.Errorf("wal entry exceeds file bounds at fid %d offset %d", expected.Fid, expected.Offset)
		}
	}

	start := lf.writeAt
	y.AssertTrue(plen == copy(lf.Data[lf.writeAt:], l.buf.Bytes()))
	lf.writeAt += uint32(plen)
	lf.zeroNextEntry()
	lsn := valuePointer{Fid: lf.fid, Len: uint32(plen), Offset: start}
	if lsn.Fid != expected.Fid || lsn.Offset != expected.Offset || lsn.Len != expected.Len {
		return valuePointer{}, fmt.Errorf("wal cursor mismatch: expected=(fid=%d offset=%d len=%d) actual=(fid=%d offset=%d len=%d)",
			expected.Fid, expected.Offset, expected.Len, lsn.Fid, lsn.Offset, lsn.Len)
	}
	l.lastAppendLSN = lsn
	if l.db.opt.SyncWrites && (e.meta&bitFinTxn > 0 || e.meta&bitTxnIntent == 0) {
		if err := lf.Sync(); err != nil {
			return valuePointer{}, y.Wrapf(err, "while syncing wal")
		}
		l.durableLSN = lsn
	}
	if e.meta&bitFinTxn == 0 {
		y.NumWritesVlogAdd(l.db.opt.MetricsEnabled, 1)
		y.NumBytesWrittenVlogAdd(l.db.opt.MetricsEnabled, int64(plen))
	}
	frame := newWALFrameFromEntry(*e, lsn)
	if frame != nil && frame.Type == WALFrameCheckpoint {
		mr := l.minRetainCursor()
		if !mr.IsZero() {
			frame.MinRetainCursor = walCursorToPB(mr)
		}
	}
	l.publishStreamFrame(frame)
	l.publishLSNMetrics()
	return lsn, nil
}

// Caller must hold l.mu.
func (l *wal) ensureWritableCapacity(lf *logFile, needed int) error {
	if lf == nil || needed <= len(lf.Data) {
		return nil
	}
	newSize := len(lf.Data)
	if newSize <= 0 {
		newSize = int(vlogHeaderSize)
	}
	for newSize < needed {
		newSize *= 2
	}
	minSize := int(2 * l.db.opt.ValueLogFileSize)
	if minSize > newSize {
		newSize = minSize
	}
	lf.lock.Lock()
	defer lf.lock.Unlock()
	if needed <= len(lf.Data) {
		return nil
	}
	if err := lf.Truncate(int64(newSize)); err != nil {
		return y.Wrapf(err, "while expanding wal file: %s", lf.path)
	}
	return nil
}

// Caller must hold l.mu.
func (l *wal) fileEndOffset(lf *logFile) (uint32, error) {
	if lf == nil {
		return uint32(vlogHeaderSize), nil
	}
	lf.lock.RLock()
	defer lf.lock.RUnlock()
	return lf.iterate(true, uint32(vlogHeaderSize), func(Entry, valuePointer) error { return nil })
}

// Caller must hold l.mu.
func (l *wal) entryMatchesAt(lf *logFile, expected valuePointer, e *Entry) (bool, error) {
	if lf == nil || e == nil || expected.IsZero() {
		return false, nil
	}
	lf.lock.RLock()
	defer lf.lock.RUnlock()
	buf, err := lf.read(expected)
	if err != nil {
		if err == y.ErrEOF {
			return false, nil
		}
		return false, err
	}
	existing, err := lf.decodeEntry(buf, expected.Offset)
	if err != nil {
		if err == errTruncate {
			return false, nil
		}
		return false, err
	}
	if existing == nil {
		return false, nil
	}
	if existing.meta != e.meta || existing.UserMeta != e.UserMeta || existing.ExpiresAt != e.ExpiresAt {
		return false, nil
	}
	if !bytes.Equal(existing.Key, e.Key) || !bytes.Equal(existing.Value, e.Value) {
		return false, nil
	}
	return true, nil
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
	l.closeAllStreamSubscribers()

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
	l.publishLSNMetrics()
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
		l.publishLSNMetrics()
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

// publishLSNMetrics updates WAL cursor metrics.
// Caller must hold l.mu.
func (l *wal) publishLSNMetrics() {
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

func (l *wal) addStreamSubscriber() (uint64, <-chan *WALFrame, valuePointer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.streamSubscribers == nil {
		l.streamSubscribers = make(map[uint64]chan *WALFrame)
	}
	id := l.nextStreamSubID
	l.nextStreamSubID++
	ch := make(chan *WALFrame, 1024)
	l.streamSubscribers[id] = ch
	return id, ch, l.lastAppendLSN
}

func (l *wal) removeStreamSubscriber(id uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ch, ok := l.streamSubscribers[id]
	if !ok {
		return
	}
	delete(l.streamSubscribers, id)
	close(ch)
}

// closeAllStreamSubscribers closes and removes all WAL stream subscribers.
// Caller must hold l.mu.
func (l *wal) closeAllStreamSubscribers() {
	for id, ch := range l.streamSubscribers {
		delete(l.streamSubscribers, id)
		close(ch)
	}
}

// publishStreamFrame fans out one frame to all current WAL subscribers.
// Caller must hold l.mu.
func (l *wal) publishStreamFrame(frame *WALFrame) {
	if frame == nil || len(l.streamSubscribers) == 0 {
		return
	}
	for id, ch := range l.streamSubscribers {
		select {
		case ch <- frame:
		default:
			delete(l.streamSubscribers, id)
			close(ch)
		}
	}
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
	vp, err := l.doAppend(&Entry{Key: walCheckpointKey, Value: []byte{1}})
	if err != nil {
		return err
	}
	if err := l.persistManifestSafePoint(vp); err != nil {
		return err
	}
	return nil
}

// resetLSN clears in-memory WAL cursor state.
// Caller must hold l.mu.
func (l *wal) resetLSN() {
	l.lastAppendLSN = valuePointer{}
	l.durableLSN = valuePointer{}
	l.appliedLSN = valuePointer{}
	l.replicationSafeLSN = valuePointer{}
	l.manifestSafeLSN = valuePointer{}
	l.publishLSNMetrics()
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
	l.resetLSN()
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

func (l *wal) pruneBeforeFid(minRetainFid uint32) error {
	if !l.enabled() || l.db.opt.ReadOnly || minRetainFid <= 1 {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	toDeleteNow := make([]*logFile, 0)
	for _, fid := range l.sortedFids() {
		if fid >= minRetainFid {
			break
		}
		if fid >= l.maxFid {
			continue
		}
		lf := l.files[fid]
		if lf == nil {
			continue
		}
		if l.iteratorCount() == 0 {
			delete(l.files, fid)
			toDeleteNow = append(toDeleteNow, lf)
		} else {
			l.filesToBeDeleted = append(l.filesToBeDeleted, fid)
		}
	}

	for _, lf := range toDeleteNow {
		if err := l.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}
