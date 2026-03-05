package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.linka.cloud/badger/v4"
	"go.uber.org/goleak"
)

func TestFaultInjectionRecoveryTriggerDedup(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	var syncCalls atomic.Int32
	cfg := defaultOptions()
	cfg.testHooks = &testHooks{
		onSyncPeer: func(uint64, peerInfo) error {
			syncCalls.Add(1)
			return nil
		},
	}
	r := newRuntime(cfg)
	peer := peerInfo{ID: 7}

	r.triggerPeerRecovery(1, peer, errors.New("cursor mismatch"))
	r.triggerPeerRecovery(1, peer, errors.New("cursor mismatch"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !r.isPeerRecovering(peer.ID) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := syncCalls.Load(); got != 1 {
		t.Fatalf("expected one sync call, got %d", got)
	}
}

func TestFaultInjectionApplyErrorTriggersRecovery(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	var syncCalls atomic.Int32
	cfg := defaultOptions()
	cfg.ApplyTimeout = 200 * time.Millisecond
	cfg.testHooks = &testHooks{
		onApplyFramesToPeer: func(context.Context, peerInfo, uint64, uint64, []*badger.WALFrame) error {
			return errors.New("wal offset mismatch")
		},
		onSyncPeer: func(uint64, peerInfo) error {
			syncCalls.Add(1)
			return nil
		},
	}
	r := newRuntime(cfg)
	r.peers[2] = peerInfo{ID: 2, GRPCAddr: "injected", Alive: true}

	err := r.executeReplicationTask(replicationTask{
		batch:    []*badger.WALFrame{{Type: badger.WALFrameEntry}},
		leaderID: 1,
		epoch:    1,
		sync:     true,
	})
	if err == nil {
		t.Fatalf("expected quorum failure")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if syncCalls.Load() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected recovery sync to be triggered")
}

func TestFaultInjectionSyncReplicationReturnsOnQuorum(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	cfg := defaultOptions()
	cfg.ApplyTimeout = 20 * time.Millisecond
	cfg.testHooks = &testHooks{
		onApplyFramesToPeer: func(ctx context.Context, p peerInfo, _ uint64, _ uint64, _ []*badger.WALFrame) error {
			if p.ID == 2 {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	r := newRuntime(cfg)
	r.peers[2] = peerInfo{ID: 2, GRPCAddr: "fast", Alive: true}
	r.peers[3] = peerInfo{ID: 3, GRPCAddr: "slow", Alive: true}

	started := time.Now()
	err := r.executeReplicationTask(replicationTask{
		batch:    []*badger.WALFrame{{Type: badger.WALFrameEntry}},
		leaderID: 1,
		epoch:    1,
		sync:     true,
	})
	if err != nil {
		t.Fatalf("expected quorum success, got error: %v", err)
	}
	if d := time.Since(started); d > 200*time.Millisecond {
		t.Fatalf("expected quorum return before slow peer timeout, took %s", d)
	}
}

func TestFaultInjectionAcceptLeader(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	cfg := defaultOptions()
	cfg.testHooks = &testHooks{
		onAcceptLeader: func(uint64, uint64) error { return errors.New("injected accept") },
	}
	r := newRuntime(cfg)
	r.epoch = 4
	r.leaderID = 2

	err := r.acceptLeader(9, 10)
	if err == nil {
		t.Fatalf("expected injected accept error")
	}
	if r.epoch != 4 || r.leaderID != 2 {
		t.Fatalf("state should remain unchanged, epoch=%d leader=%d", r.epoch, r.leaderID)
	}
}

func TestFaultInjectionSaveState(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	cfg := defaultOptions()
	cfg.testHooks = &testHooks{
		onSaveState: func(persistedState) error { return errors.New("injected save") },
	}
	r := newRuntime(cfg)
	err := r.saveState(persistedState{NodeID: 1})
	if err == nil {
		t.Fatalf("expected injected save error")
	}
}

func TestSnapshotChunkSendFailureCleansUpAndRetrySucceeds(t *testing.T) {
	tests := []struct {
		name      string
		hookError string
		setHook   func(*testHooks, *atomic.Bool)
	}{
		{
			name:      "reset",
			hookError: "injected snapshot reset failure",
			setHook: func(h *testHooks, failFirst *atomic.Bool) {
				h.onBeforeSnapshotReset = func(uint64, peerInfo) error {
					if failFirst.Load() {
						failFirst.Store(false)
						return errors.New("injected snapshot reset failure")
					}
					return nil
				}
			},
		},
		{
			name:      "chunk-send",
			hookError: "injected snapshot chunk send failure",
			setHook: func(h *testHooks, failFirst *atomic.Bool) {
				h.onBeforeSnapshotSend = func(uint64, peerInfo, []byte) error {
					if failFirst.Load() {
						failFirst.Store(false)
						return errors.New("injected snapshot chunk send failure")
					}
					return nil
				}
			},
		},
		{
			name:      "done-marker",
			hookError: "injected snapshot done failure",
			setHook: func(h *testHooks, failFirst *atomic.Bool) {
				h.onBeforeSnapshotDone = func(uint64, peerInfo, badger.WALCursor) error {
					if failFirst.Load() {
						failFirst.Store(false)
						return errors.New("injected snapshot done failure")
					}
					return nil
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runSnapshotFailureCase(t, tc.setHook, tc.hookError)
		})
	}
}

func runSnapshotFailureCase(t *testing.T, setHook func(*testHooks, *atomic.Bool), expectedErr string) {
	dir := t.TempDir()
	cfgs := makeClusterConfigs(t, dir, 2)

	var failFirst atomic.Bool
	failFirst.Store(true)
	for i := range cfgs {
		h := &testHooks{}
		setHook(h, &failFirst)
		cfgs[i].testHooks = h
	}

	n1 := startTestNode(t, cfgs[0])
	n2 := startTestNode(t, cfgs[1])
	nodes := []*testNode{n1, n2}
	defer func() {
		for _, n := range nodes {
			n.close(t)
		}
	}()

	leaderID := waitForLeader(t, nodes, 0, 20*time.Second)
	var leader *testNode
	for _, n := range nodes {
		if n.id == leaderID {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("leader not found")
	}

	runTxnWorkloadOnLeader(t, nodes, leader.id, 2000, 500)

	peer, ok := leaderPeerByID(leader.rt)
	if !ok {
		t.Fatalf("leader peer not found")
	}
	epoch := leader.rt.currentEpoch()

	if _, err := leader.rt.recoverPeerFromSnapshot(epoch, peer); err == nil || err.Error() != expectedErr {
		t.Fatalf("expected injected snapshot failure %q, got: %v", expectedErr, err)
	}
	waitPeerRecoveryCleared(t, leader.rt, peer.ID, 3*time.Second)

	if _, err := leader.rt.recoverPeerFromSnapshot(epoch, peer); err != nil {
		t.Fatalf("expected retry snapshot recovery success, got: %v", err)
	}
}

func waitPeerRecoveryCleared(t *testing.T, r *Replication, peerID uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !r.isPeerRecovering(peerID) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("peer %d remained in recovering state", peerID)
}

func leaderPeerByID(r *Replication) (peerInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.peers {
		if p.ID == r.cfg.NodeID || !p.Alive || p.GRPCAddr == "" {
			continue
		}
		return p, true
	}
	return peerInfo{}, false
}
