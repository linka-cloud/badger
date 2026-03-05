package replication

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.linka.cloud/badger/v4"
)

type testNode struct {
	id  uint64
	rt  *Replication
	db  *badger.DB
	cfg Options
}

func reservePorts(t *testing.T, n int) []int {
	t.Helper()
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		out = append(out, ln.Addr().(*net.TCPAddr).Port)
		_ = ln.Close()
	}
	return out
}

func baseTestConfig(nodeID uint64, stateDir string, grpcPort, gossipPort int, join []string) Options {
	return Options{
		NodeID:                nodeID,
		Name:                  fmt.Sprintf("node-%d", nodeID),
		StateDir:              stateDir,
		GRPCAddr:              fmt.Sprintf("127.0.0.1:%d", grpcPort),
		MemberlistBindAddr:    fmt.Sprintf("127.0.0.1:%d", gossipPort),
		Join:                  join,
		BatchSize:             1024,
		ApplyTimeout:          2 * time.Second,
		HeartbeatInterval:     50 * time.Millisecond,
		LeaderTimeout:         800 * time.Millisecond,
		ElectionTimeout:       150 * time.Millisecond,
		StartupGrace:          200 * time.Millisecond,
		KnownUnreachableAfter: 500 * time.Millisecond,
		RequiredSeenRatio:     0.5,
		ValueLogFileSize:      16 << 20,
		CompactionInterval:    0,
		WALGCRatio:            0.01,
		WALGCMaxRuns:          8,
		SnapshotChunkSize:     1 << 20,
	}
}

func startTestNode(t *testing.T, cfg Options) *testNode {
	t.Helper()
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	rt := newRuntime(cfg)
	db, err := badger.Open(
		badger.DefaultOptions(cfg.StateDir).
			WithWAL(true).
			WithValueLogFileSize(cfg.ValueLogFileSize).
			WithReplication(rt),
	)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	return &testNode{id: cfg.NodeID, rt: rt, db: db, cfg: cfg}
}

func (n *testNode) close(t *testing.T) {
	t.Helper()
	n.rt.Close()
	if err := n.db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func waitForLeader(t *testing.T, nodes []*testNode, want uint64, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := uint64(0)
		count := 0
		for _, n := range nodes {
			if n.rt.Role() == badger.ReplicationRolePrimary {
				leaders = n.id
				count++
			}
		}
		if count == 1 {
			if want == 0 || want == leaders {
				return leaders
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("leader not found (want=%d)", want)
	return 0
}

func waitForConvergence(t *testing.T, nodes []*testNode, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var cursor badger.WALCursor
		ok := true
		for i, n := range nodes {
			c, err := n.db.Replication().Cursor()
			if err != nil {
				t.Fatalf("read cursor node %d: %v", n.id, err)
			}
			if i == 0 {
				cursor = c
				continue
			}
			if c != cursor {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge")
}

func keyExists(t *testing.T, db *badger.DB, key []byte) (bool, error) {
	t.Helper()
	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	return false, err
}

func waitForKeyState(t *testing.T, nodes []*testNode, key []byte, exists bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, n := range nodes {
			if n == nil {
				continue
			}
			if n.rt.bootstrapping.Load() {
				all = false
				continue
			}
			has, err := keyExists(t, n.db, key)
			if err != nil {
				t.Fatalf("read key on node %d: %v", n.id, err)
			}
			if has != exists {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("key state did not converge for key=%q exists=%t", string(key), exists)
}

func runTxnWorkload(t *testing.T, db *badger.DB, nodeID uint64, writes, deletes int) {
	t.Helper()
	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < writes; i++ {
			k := []byte(fmt.Sprintf("wl-%d-%d", nodeID, i))
			v := []byte(fmt.Sprintf("v-%d", i))
			if err := txn.Set(k, v); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write workload: %v", err)
	}
	if deletes == 0 {
		return
	}
	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < deletes; i++ {
			k := []byte(fmt.Sprintf("wl-%d-%d", nodeID, i))
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("delete workload: %v", err)
	}
}

func runTxnWorkloadOnLeader(t *testing.T, nodes []*testNode, keyNodeID uint64, writes, deletes int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		leader := currentLeader(nodes)
		if leader == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err := leader.db.Update(func(txn *badger.Txn) error {
			for i := 0; i < writes; i++ {
				k := []byte(fmt.Sprintf("wl-%d-%d", keyNodeID, i))
				v := []byte(fmt.Sprintf("v-%d", i))
				if err := txn.Set(k, v); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			if isReplicaWriteErr(err) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			t.Fatalf("write workload: %v", err)
		}
		if deletes > 0 {
			err = leader.db.Update(func(txn *badger.Txn) error {
				for i := 0; i < deletes; i++ {
					k := []byte(fmt.Sprintf("wl-%d-%d", keyNodeID, i))
					if err := txn.Delete(k); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				if isReplicaWriteErr(err) {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				t.Fatalf("delete workload: %v", err)
			}
		}
		return
	}
	t.Fatalf("workload did not complete before timeout")
}

func currentLeader(nodes []*testNode) *testNode {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if n.rt.Role() == badger.ReplicationRolePrimary {
			return n
		}
	}
	return nil
}

func isReplicaWriteErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "replica role") || strings.Contains(err.Error(), "not allowed")
}

func makeClusterConfigs(t *testing.T, dir string, n int) []Options {
	t.Helper()
	grpcPorts := reservePorts(t, n)
	gossipPorts := reservePorts(t, n)
	seed := fmt.Sprintf("127.0.0.1:%d", gossipPorts[0])
	cfgs := make([]Options, 0, n)
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		join := []string{}
		if i > 0 {
			join = []string{seed}
		}
		cfgs = append(cfgs, baseTestConfig(
			id,
			filepath.Join(dir, fmt.Sprintf("node-%d", id)),
			grpcPorts[i],
			gossipPorts[i],
			join,
		))
	}
	return cfgs
}
