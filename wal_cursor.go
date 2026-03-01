package badger

// WALCursor identifies a WAL frame position.
type WALCursor struct {
	Fid    uint32
	Offset uint32
	Len    uint32
}

func (c WALCursor) IsZero() bool {
	return c.Fid == 0 && c.Offset == 0 && c.Len == 0
}

func (c WALCursor) Less(o WALCursor) bool {
	if c.Fid != o.Fid {
		return c.Fid < o.Fid
	}
	if c.Offset != o.Offset {
		return c.Offset < o.Offset
	}
	return c.Len < o.Len
}

func (c WALCursor) Equal(o WALCursor) bool {
	return c.Fid == o.Fid && c.Offset == o.Offset && c.Len == o.Len
}

func walCursorFromValuePointer(vp valuePointer) WALCursor {
	return WALCursor{Fid: vp.Fid, Offset: vp.Offset, Len: vp.Len}
}

func valuePointerFromWALCursor(c WALCursor) valuePointer {
	return valuePointer{Fid: c.Fid, Offset: c.Offset, Len: c.Len}
}

func walCursorVersion(c WALCursor) uint64 {
	if c.IsZero() {
		return 1
	}
	ver := (uint64(c.Fid) << 32) | uint64(c.Offset)
	if ver == 0 {
		return 1
	}
	return ver
}
