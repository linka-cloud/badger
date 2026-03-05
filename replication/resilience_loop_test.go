package replication

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestIntegrationResilienceChurnLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skip soak in short mode")
	}
	iterations := soakIntEnv(t, "SOAK_ITERATIONS", 3)
	baseWrites := soakIntEnv(t, "SOAK_BASE_WRITES", 2000)
	baseDeletes := soakIntEnv(t, "SOAK_BASE_DELETES", 500)
	jitterWrites := soakIntEnv(t, "SOAK_JITTER_WRITES", 300)
	jitterDeletes := soakIntEnv(t, "SOAK_JITTER_DELETES", 100)
	leaderTimeout := soakDurationEnv(t, "SOAK_LEADER_TIMEOUT", 20*time.Second)
	keyTimeout := soakDurationEnv(t, "SOAK_KEY_TIMEOUT", 30*time.Second)
	requiredNodes := soakIntEnv(t, "SOAK_REQUIRED_NODES", 2)
	strictChecks := soakBoolEnv(t, "SOAK_STRICT_CHECKS", false)
	seed := int64(soakIntEnv(t, "SOAK_SEED", 7))
	rng := rand.New(rand.NewSource(seed))

	if iterations <= 0 || baseWrites <= 0 || baseDeletes < 0 {
		t.Fatalf("invalid soak params iterations=%d baseWrites=%d baseDeletes=%d", iterations, baseWrites, baseDeletes)
	}
	if requiredNodes <= 0 || requiredNodes > 3 {
		t.Fatalf("invalid SOAK_REQUIRED_NODES=%d", requiredNodes)
	}

	dir := t.TempDir()
	cfgs := makeClusterConfigs(t, dir, 3)
	nodes := []*testNode{
		startTestNode(t, cfgs[0]),
		startTestNode(t, cfgs[1]),
		startTestNode(t, cfgs[2]),
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.close(t)
			}
		}
	}()

	for iter := 0; iter < iterations; iter++ {
		leaderID := waitForLeader(t, nodes, 0, leaderTimeout)
		var leaderIdx int
		for i, n := range nodes {
			if n != nil && n.id == leaderID {
				leaderIdx = i
				break
			}
		}
		leader := nodes[leaderIdx]
		if leader == nil {
			t.Fatalf("leader node missing at iter %d", iter)
		}

		writes := baseWrites + iter*jitterWrites
		deletes := baseDeletes + iter*jitterDeletes
		runTxnWorkloadOnLeader(t, nodes, leader.id, writes, deletes)

		active := make([]*testNode, 0, len(nodes))
		for _, n := range nodes {
			if n != nil {
				active = append(active, n)
			}
		}
		minMatch := requiredNodes
		if strictChecks {
			minMatch = 3
		}
		keepKey := []byte(fmt.Sprintf("wl-%d-%d", leader.id, writes-1))
		delKey := []byte(fmt.Sprintf("wl-%d-%d", leader.id, deletes/2))
		waitForKeyStateMin(t, active, keepKey, true, minMatch, keyTimeout)
		waitForKeyStateMin(t, active, delKey, false, minMatch, keyTimeout)

		if rng.Intn(2) == 0 {
			leader.rt.RunWALGC()
		}

		followers := make([]int, 0, len(nodes)-1)
		for i, n := range nodes {
			if n != nil && i != leaderIdx {
				followers = append(followers, i)
			}
		}
		if len(followers) == 0 {
			t.Fatalf("no follower available to restart at iter %d", iter)
		}
		stopIdx := followers[rng.Intn(len(followers))]
		nodes[stopIdx].close(t)
		nodes[stopIdx] = nil

		time.Sleep(time.Duration(150+rng.Intn(250)) * time.Millisecond)
		nodes[stopIdx] = startTestNode(t, cfgs[stopIdx])
		waitForLeader(t, nodes, 0, leaderTimeout)
		waitForKeyStateMin(t, nodes, keepKey, true, minMatch, keyTimeout)
		waitForKeyStateMin(t, nodes, delKey, false, minMatch, keyTimeout)
	}
}
