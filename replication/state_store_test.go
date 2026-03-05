package replication

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStateStoreLoadMissing(t *testing.T) {
	dir := t.TempDir()
	s := newStateStore(dir)
	st, err := s.load()
	if err != nil {
		t.Fatalf("load missing state: %v", err)
	}
	if st.KnownMembers == nil || len(st.KnownMembers) != 0 {
		t.Fatalf("expected empty known members map")
	}
}

func TestStateStoreSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	s := newStateStore(dir)
	in := persistedState{
		NodeID:            3,
		Epoch:             4,
		LastKnownLeaderID: 3,
		KnownMembers: map[string]persistedMember{
			"2": {ID: 2, GRPCAddr: "127.0.0.1:19002", LastSeenUnix: 100, LastEpochSeen: 4},
		},
	}
	if err := s.save(in); err != nil {
		t.Fatalf("save state: %v", err)
	}
	out, err := s.load()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if out.NodeID != in.NodeID || out.Epoch != in.Epoch || out.LastKnownLeaderID != in.LastKnownLeaderID {
		t.Fatalf("state mismatch: got=%+v want=%+v", out, in)
	}
	if len(out.KnownMembers) != 1 || out.KnownMembers["2"].ID != 2 {
		t.Fatalf("known members mismatch: %#v", out.KnownMembers)
	}

	if _, err := os.Stat(filepath.Join(dir, "cluster_state.json.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temp file should not remain, err=%v", err)
	}
}

func TestStateStoreLoadMalformed(t *testing.T) {
	dir := t.TempDir()
	s := newStateStore(dir)
	if err := os.WriteFile(filepath.Join(dir, "cluster_state.json"), []byte("not-json"), 0o600); err != nil {
		t.Fatalf("write malformed file: %v", err)
	}
	if _, err := s.load(); err == nil {
		t.Fatalf("expected unmarshal error")
	}
}
