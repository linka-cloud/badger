package replication

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestIntegrationClusterElectsLeaderAndReplicates(t *testing.T) {
	dir := t.TempDir()
	cfgs := makeClusterConfigs(t, dir, 2)

	nodes := []*testNode{
		startTestNode(t, cfgs[0]),
		startTestNode(t, cfgs[1]),
	}
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
		t.Fatalf("leader node not found")
	}

	runTxnWorkloadOnLeader(t, nodes, leader.id, 6000, 2000)
	keepKey := []byte(fmt.Sprintf("wl-%d-%d", leader.id, 5999))
	waitForKeyState(t, nodes, keepKey, true, 20*time.Second)
}

func TestIntegrationFailoverAndRejoin(t *testing.T) {
	dir := t.TempDir()
	cfgs := makeClusterConfigs(t, dir, 3)
	n1 := startTestNode(t, cfgs[0])
	n2 := startTestNode(t, cfgs[1])
	n3 := startTestNode(t, cfgs[2])
	nodes := []*testNode{n1, n2, n3}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.close(t)
			}
		}
	}()

	leaderID := waitForLeader(t, nodes, 0, 20*time.Second)
	var downIdx int
	for i, n := range nodes {
		if n.id == leaderID {
			downIdx = i
			break
		}
	}
	downNode := nodes[downIdx]
	downNode.close(t)
	nodes[downIdx] = nil

	live := make([]*testNode, 0, 2)
	for _, n := range nodes {
		if n != nil {
			live = append(live, n)
		}
	}
	newLeader := waitForLeader(t, live, 0, 20*time.Second)
	if newLeader == leaderID {
		t.Fatalf("leader should have changed after failover")
	}

	var active *testNode
	for _, n := range live {
		if n.id == newLeader {
			active = n
			break
		}
	}
	runTxnWorkloadOnLeader(t, live, active.id, 5000, 1500)
	keepKey := []byte(fmt.Sprintf("wl-%d-%d", active.id, 4999))
	delKey := []byte(fmt.Sprintf("wl-%d-%d", active.id, 100))
	waitForKeyState(t, live, keepKey, true, 25*time.Second)
	waitForKeyState(t, live, delKey, false, 25*time.Second)

	rejoinCfg := cfgs[downIdx]
	rejoinCfg.StateDir = filepath.Join(dir, filepath.Base(rejoinCfg.StateDir))
	rejoined := startTestNode(t, rejoinCfg)
	nodes[downIdx] = rejoined

	all := []*testNode{nodes[0], nodes[1], nodes[2]}
	waitForLeader(t, all, 0, 25*time.Second)
}

func TestIntegrationRecoveryAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	cfgs := makeClusterConfigs(t, dir, 3)
	for i := range cfgs {
		cfgs[i].ValueLogFileSize = 2 << 20
		cfgs[i].BatchSize = 4096
	}

	n1 := startTestNode(t, cfgs[0])
	n2 := startTestNode(t, cfgs[1])
	n3 := startTestNode(t, cfgs[2])
	nodes := []*testNode{n1, n2, n3}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.close(t)
			}
		}
	}()

	leaderID := waitForLeader(t, nodes, 0, 20*time.Second)
	var leader *testNode
	var laggerIdx int
	for i, n := range nodes {
		if n.id == leaderID {
			leader = n
		} else {
			laggerIdx = i
		}
	}
	lagger := nodes[laggerIdx]
	lagger.close(t)
	nodes[laggerIdx] = nil

	runTxnWorkloadOnLeader(t, nodes, leader.id, 12000, 4000)
	leader.rt.RunWALGC()
	leader.rt.RunWALGC()

	keepKey := []byte(fmt.Sprintf("wl-%d-%d", leader.id, 11999))
	delKey := []byte(fmt.Sprintf("wl-%d-%d", leader.id, 250))
	activeNodes := []*testNode{}
	for _, n := range nodes {
		if n != nil {
			activeNodes = append(activeNodes, n)
		}
	}
	waitForKeyState(t, activeNodes, keepKey, true, 35*time.Second)
	waitForKeyState(t, activeNodes, delKey, false, 35*time.Second)

	restarted := startTestNode(t, cfgs[laggerIdx])
	nodes[laggerIdx] = restarted

	all := []*testNode{nodes[0], nodes[1], nodes[2]}
	waitForLeader(t, all, 0, 30*time.Second)
}
