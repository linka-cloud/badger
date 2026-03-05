package replication

import (
	"testing"

	replicationpb "go.linka.cloud/badger/replication/pb"
	"go.linka.cloud/badger/v4"
)

func TestCursorConversionRoundtrip(t *testing.T) {
	in := badger.WALCursor{Fid: 3, Offset: 100, Len: 24}
	pb := pbCursorFromWALCursor(in)
	out := walCursorFromPBCursor(pb)
	if out != in {
		t.Fatalf("cursor mismatch: got=%+v want=%+v", out, in)
	}

	if pbCursorFromWALCursor(badger.WALCursor{}) != nil {
		t.Fatalf("zero cursor should encode to nil")
	}
}

func TestStatePreferenceOrdering(t *testing.T) {
	a := nodeState{ID: 1, Epoch: 1, Cursor: badger.WALCursor{Fid: 2, Offset: 20, Len: 10}}
	b := nodeState{ID: 2, Epoch: 9, Cursor: badger.WALCursor{Fid: 1, Offset: 999, Len: 1}}
	if !isStatePreferred(a, b) {
		t.Fatalf("higher cursor should win")
	}

	a = nodeState{ID: 1, Epoch: 3, Cursor: badger.WALCursor{Fid: 2, Offset: 20, Len: 10}}
	b = nodeState{ID: 2, Epoch: 2, Cursor: badger.WALCursor{Fid: 2, Offset: 20, Len: 10}}
	if !isStatePreferred(a, b) {
		t.Fatalf("higher epoch should break cursor ties")
	}

	a = nodeState{ID: 9, Epoch: 3, Cursor: badger.WALCursor{Fid: 2, Offset: 20, Len: 10}}
	b = nodeState{ID: 2, Epoch: 3, Cursor: badger.WALCursor{Fid: 2, Offset: 20, Len: 10}}
	if !isStatePreferred(a, b) {
		t.Fatalf("higher id should break full ties")
	}
}

func TestParseJoin(t *testing.T) {
	got := parseJoin(" 127.0.0.1:1, ,127.0.0.1:2 ")
	if len(got) != 2 || got[0] != "127.0.0.1:1" || got[1] != "127.0.0.1:2" {
		t.Fatalf("unexpected join parse: %#v", got)
	}
	if out := parseJoin("   "); out != nil {
		t.Fatalf("expected nil for blank join, got %#v", out)
	}
}

func TestNodeStateFromPB(t *testing.T) {
	resp := &replicationpb.GetNodeStateResponse{
		NodeId:   4,
		Epoch:    7,
		LeaderId: 9,
		Cursor:   pbCursorFromWALCursor(badger.WALCursor{Fid: 5, Offset: 6, Len: 7}),
	}
	st := nodeStateFromPB(resp)
	if st.ID != 4 || st.Epoch != 7 || st.LeaderID != 9 || st.Cursor.Fid != 5 {
		t.Fatalf("unexpected node state: %+v", st)
	}
}

func TestMemberlistSecretKey(t *testing.T) {
	raw := memberlistSecretKey("1234567890abcdef")
	if len(raw) != 16 {
		t.Fatalf("expected 16-byte raw secret key, got %d", len(raw))
	}

	hashed := memberlistSecretKey("short-key")
	if len(hashed) != 32 {
		t.Fatalf("expected hashed 32-byte secret key, got %d", len(hashed))
	}
	if string(hashed) == "short-key" {
		t.Fatalf("expected hashed key for non-standard length")
	}
}
