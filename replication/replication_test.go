package replication

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go.linka.cloud/badger/v4"
)

func TestRequiredAcksMajority(t *testing.T) {
	r := newRuntime(defaultOptions())
	if got := r.requiredAcks(0); got != 0 {
		t.Fatalf("peer=0 expected 0, got %d", got)
	}
	if got := r.requiredAcks(1); got != 1 {
		t.Fatalf("peer=1 expected 1, got %d", got)
	}
	if got := r.requiredAcks(2); got != 1 {
		t.Fatalf("peer=2 expected 1, got %d", got)
	}
	if got := r.requiredAcks(3); got != 2 {
		t.Fatalf("peer=3 expected 2, got %d", got)
	}
}

func TestRequiredAcksOverride(t *testing.T) {
	cfg := defaultOptions()
	cfg.QuorumAcks = 2
	r := newRuntime(cfg)
	if got := r.requiredAcks(3); got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
	if got := r.requiredAcks(1); got != 1 {
		t.Fatalf("override must clamp to peer count, got %d", got)
	}
}

func TestRecoveryPeerGuards(t *testing.T) {
	r := newRuntime(defaultOptions())
	if !r.beginPeerRecovery(10) {
		t.Fatalf("first begin should succeed")
	}
	if r.beginPeerRecovery(10) {
		t.Fatalf("second begin should be deduped")
	}
	if !r.isPeerRecovering(10) {
		t.Fatalf("peer should be marked recovering")
	}
	r.endPeerRecovery(10)
	if r.isPeerRecovering(10) {
		t.Fatalf("peer should no longer be recovering")
	}
}

func TestCanParticipateInElectionKnownMembersGate(t *testing.T) {
	cfg := defaultOptions()
	cfg.NodeID = 2
	cfg.StartupGrace = 0
	cfg.RequiredSeenRatio = 1.0
	cfg.KnownUnreachableAfter = time.Hour
	r := newRuntime(cfg)

	now := time.Now()
	r.startupAt = now.Add(-time.Second)
	r.knownMembers[1] = persistedMember{ID: 1, LastSeenUnix: now.Unix()}
	r.knownMembers[3] = persistedMember{ID: 3, LastSeenUnix: now.Unix()}

	if r.canParticipateInElection(now) {
		t.Fatalf("should not participate when seen ratio is not met")
	}

	r.peers[1] = peerInfo{ID: 1, Alive: true}
	r.peers[3] = peerInfo{ID: 3, Alive: true}
	if !r.canParticipateInElection(now) {
		t.Fatalf("should participate when all known members are visible")
	}
}

func TestAcceptLeaderFencing(t *testing.T) {
	r := newRuntime(defaultOptions())
	r.epoch = 5
	r.leaderID = 3
	if err := r.acceptLeader(4, 4); err == nil {
		t.Fatalf("expected stale epoch rejection")
	}
	if err := r.acceptLeader(4, 5); err == nil {
		t.Fatalf("expected leader mismatch rejection")
	}
	if err := r.acceptLeader(4, 6); err != nil {
		t.Fatalf("expected newer epoch acceptance: %v", err)
	}
}

func TestStartFailureDoesNotLeaveStartedState(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultOptions()
	cfg.NodeID = 2
	cfg.StateDir = dir
	store := newStateStore(dir)
	if err := store.save(persistedState{NodeID: 3, KnownMembers: map[string]persistedMember{}}); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	db, err := badger.Open(badger.DefaultOptions(filepath.Join(dir, "db")).WithWAL(true))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	r := newRuntime(cfg)
	if err := r.Start(context.Background(), db); err == nil {
		t.Fatalf("expected start failure")
	}
	r.mu.RLock()
	started := r.started
	starting := r.starting
	r.mu.RUnlock()
	if started || starting {
		t.Fatalf("runtime should not be left started/starting after failed start")
	}
}

func TestStartFailsWithInvalidTLSMaterial(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultOptions()
	cfg.NodeID = 1
	cfg.StateDir = dir
	cfg.GRPCAddr = "127.0.0.1:0"
	cfg.MemberlistBindAddr = "127.0.0.1:0"
	WithServerCert([]byte("bad-cert"))(&cfg)
	WithServerKey([]byte("bad-key"))(&cfg)

	db, err := badger.Open(badger.DefaultOptions(filepath.Join(dir, "db")).WithWAL(true))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	r := newRuntime(cfg)
	if err := r.Start(context.Background(), db); err == nil {
		t.Fatalf("expected start failure from invalid TLS config")
	}
	r.mu.RLock()
	started := r.started
	r.mu.RUnlock()
	if started {
		t.Fatalf("runtime should not be marked started on TLS startup failure")
	}
}

func TestStartHonorsAlreadyCanceledContext(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultOptions()
	cfg.NodeID = 1
	cfg.StateDir = dir
	cfg.GRPCAddr = "127.0.0.1:0"
	cfg.MemberlistBindAddr = "127.0.0.1:0"

	db, err := badger.Open(badger.DefaultOptions(filepath.Join(dir, "db")).WithWAL(true))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := newRuntime(cfg)
	err = r.Start(ctx, db)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got: %v", err)
	}
	r.mu.RLock()
	started := r.started
	starting := r.starting
	r.mu.RUnlock()
	if started || starting {
		t.Fatalf("runtime should not be left started/starting after canceled start")
	}
}

func TestStartHonorsContextCanceledAfterLoad(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultOptions()
	cfg.NodeID = 1
	cfg.StateDir = dir
	cfg.GRPCAddr = "127.0.0.1:0"
	cfg.MemberlistBindAddr = "127.0.0.1:0"

	ctx, cancel := context.WithCancel(context.Background())
	cfg.testHooks = &testHooks{
		onLoadState: func(*persistedState) error {
			cancel()
			return nil
		},
	}

	db, err := badger.Open(badger.DefaultOptions(filepath.Join(dir, "db")).WithWAL(true))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	r := newRuntime(cfg)
	err = r.Start(ctx, db)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got: %v", err)
	}
	r.mu.RLock()
	started := r.started
	starting := r.starting
	r.mu.RUnlock()
	if started || starting {
		t.Fatalf("runtime should not be left started/starting after canceled start")
	}
}

func TestLeaderHelpers(t *testing.T) {
	cfg := defaultOptions()
	cfg.NodeID = 1
	cfg.GRPCAddr = "127.0.0.1:19001"
	r := newRuntime(cfg)

	r.mu.Lock()
	r.role = badger.ReplicationRolePrimary
	r.leaderID = 1
	r.mu.Unlock()

	if !r.IsLeader() {
		t.Fatalf("expected leader=true")
	}
	if got := r.CurrentLeader(); got != cfg.GRPCAddr {
		t.Fatalf("expected local grpc addr, got %q", got)
	}

	r.mu.Lock()
	r.role = badger.ReplicationRoleReplica
	r.leaderID = 2
	r.peers[2] = peerInfo{ID: 2, GRPCAddr: "127.0.0.1:19002", Alive: true}
	r.mu.Unlock()

	if r.IsLeader() {
		t.Fatalf("expected leader=false")
	}
	if got := r.CurrentLeader(); got != "127.0.0.1:19002" {
		t.Fatalf("unexpected current leader: %q", got)
	}
}

func TestSubscribePublishesLeaderChanges(t *testing.T) {
	cfg := defaultOptions()
	cfg.NodeID = 1
	cfg.GRPCAddr = "127.0.0.1:19001"
	r := newRuntime(cfg)

	ch := r.Subscribe()
	select {
	case <-ch:
		t.Fatalf("did not expect initial subscription event")
	case <-time.After(50 * time.Millisecond):
	}

	r.mu.Lock()
	r.peers[2] = peerInfo{ID: 2, GRPCAddr: "127.0.0.1:19002", Alive: true}
	r.mu.Unlock()
	if err := r.acceptLeader(2, 1); err != nil {
		t.Fatalf("accept leader: %v", err)
	}

	deadline := time.After(time.Second)
	for {
		select {
		case v := <-ch:
			if v == "127.0.0.1:19002" {
				return
			}
		case <-deadline:
			t.Fatalf("expected leader change event")
		}
	}
}
