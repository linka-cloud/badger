package replication

import (
	"fmt"
	"testing"
	"time"
)

func TestSoakThreeNodeLargeTxnRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skip soak in short mode")
	}
	writes := soakIntEnv(t, "SOAK_WRITES", 1_000_000)
	deletes := soakIntEnv(t, "SOAK_DELETES", 500_000)
	clusters := soakIntEnv(t, "SOAK_CLUSTERS", 1)
	requiredNodes := soakIntEnv(t, "SOAK_REQUIRED_NODES", 2)
	strictChecks := soakBoolEnv(t, "SOAK_STRICT_CHECKS", false)
	leaderTimeout := soakDurationEnv(t, "SOAK_LEADER_TIMEOUT", 45*time.Second)
	keyTimeout := soakDurationEnv(t, "SOAK_KEY_TIMEOUT", 2*time.Minute)

	if writes <= 0 || deletes < 0 || clusters <= 0 {
		t.Fatalf("invalid soak config writes=%d deletes=%d clusters=%d", writes, deletes, clusters)
	}
	if requiredNodes <= 0 || requiredNodes > 3 {
		t.Fatalf("invalid SOAK_REQUIRED_NODES=%d", requiredNodes)
	}

	for i := 0; i < clusters; i++ {
		t.Run(fmt.Sprintf("cluster-%d", i+1), func(t *testing.T) {
			dir := t.TempDir()
			cfgs := makeClusterConfigs(t, dir, 3)
			nodes := []*testNode{
				startTestNode(t, cfgs[0]),
				startTestNode(t, cfgs[1]),
				startTestNode(t, cfgs[2]),
			}
			defer func() {
				for _, n := range nodes {
					n.close(t)
				}
			}()

			leaderID := waitForLeader(t, nodes, 0, leaderTimeout)
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

			runTxnWorkloadOnLeader(t, nodes, leader.id, writes, deletes)
			active := nodes
			keepKey := []byte(fmt.Sprintf("wl-%d-%d", leader.id, writes-1))
			if strictChecks {
				waitForKeyStateMin(t, active, keepKey, true, 3, keyTimeout)
			} else {
				waitForKeyStateMin(t, active, keepKey, true, requiredNodes, keyTimeout)
			}
			if deletes > 0 {
				delKey := []byte(fmt.Sprintf("wl-%d-%d", leader.id, deletes/2))
				if strictChecks {
					waitForKeyStateMin(t, active, delKey, false, 3, keyTimeout)
				} else {
					waitForKeyStateMin(t, active, delKey, false, requiredNodes, keyTimeout)
				}
			}
		})
	}
}
