package badger

import (
	"hash/crc32"
	"io"

	"github.com/dgraph-io/badger/v4/y"
)

const WALFrameVersion uint8 = 1

type WALFrameType uint8

const (
	WALFrameUnknown WALFrameType = iota
	WALFrameEntry
	WALFrameTxnIntent
	WALFrameTxnCommit
	WALFrameCheckpoint
)

type WALFrame struct {
	Version   uint8
	Type      WALFrameType
	LSN       valuePointer
	Key       []byte
	Value     []byte
	Meta      byte
	UserMeta  byte
	ExpiresAt uint64
}

// StreamWALFrom streams WAL frames from the provided LSN (inclusive).
func (db *DB) StreamWALFrom(start valuePointer, fn func(WALFrame) error) error {
	if !db.opt.EnableWAL {
		return nil
	}
	return db.wal.streamFrom(start, fn)
}

func classifyWALFrame(e Entry) WALFrameType {
	if isWalCheckpointEntry(e) {
		return WALFrameCheckpoint
	}
	if e.meta&bitFinTxn > 0 {
		return WALFrameTxnCommit
	}
	if e.meta&bitTxnIntent > 0 {
		return WALFrameTxnIntent
	}
	return WALFrameEntry
}

func (l *wal) streamFrom(start valuePointer, fn func(WALFrame) error) error {
	if !l.enabled() || fn == nil {
		return nil
	}

	l.mu.RLock()
	fids := l.sortedFids()
	files := make([]*logFile, 0, len(fids))
	for _, fid := range fids {
		if start.Fid > 0 && fid < start.Fid {
			continue
		}
		files = append(files, l.files[fid])
	}
	l.mu.RUnlock()

	for _, lf := range files {
		if lf == nil {
			continue
		}
		offset := uint32(vlogHeaderSize)
		if start.Fid > 0 && lf.fid == start.Fid && start.Offset > 0 {
			offset = start.Offset
		}
		lf.lock.RLock()
		read := safeRead{
			k:  make([]byte, 10),
			v:  make([]byte, 10),
			lf: lf,
		}
		read.recordOffset = offset
		for {
			e, err := read.Entry(io.Reader(lf.NewReader(int(read.recordOffset))))
			if err == io.EOF || err == io.ErrUnexpectedEOF || err == errTruncate {
				break
			}
			if err != nil {
				lf.lock.RUnlock()
				return err
			}
			if e == nil || e.isZero() {
				break
			}
			vp := valuePointer{Fid: lf.fid, Offset: e.offset}
			vp.Len = uint32(int(e.hlen) + len(e.Key) + len(e.Value) + crc32.Size)
			read.recordOffset += vp.Len
			frame := WALFrame{
				Version:   WALFrameVersion,
				Type:      classifyWALFrame(*e),
				LSN:       vp,
				Key:       y.SafeCopy(nil, e.Key),
				Value:     y.SafeCopy(nil, e.Value),
				Meta:      e.meta,
				UserMeta:  e.UserMeta,
				ExpiresAt: e.ExpiresAt,
			}
			if err := fn(frame); err != nil {
				lf.lock.RUnlock()
				return err
			}
		}
		lf.lock.RUnlock()
	}
	return nil
}
