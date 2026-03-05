package replication

import (
	"os"
	"strconv"
	"testing"
	"time"

	"go.linka.cloud/badger/v4"
)

func soakIntEnv(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
}

func soakDurationEnv(t *testing.T, key string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return d
}

func soakBoolEnv(t *testing.T, key string, def bool) bool {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return b
}

func waitForConvergenceMin(t *testing.T, nodes []*testNode, minMatch int, timeout time.Duration) {
	t.Helper()
	if minMatch <= 0 {
		t.Fatalf("minMatch must be > 0, got %d", minMatch)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		counts := make(map[badger.WALCursor]int)
		for _, n := range nodes {
			if n == nil {
				continue
			}
			c, err := n.db.Replication().Cursor()
			if err != nil {
				t.Fatalf("read cursor node %d: %v", n.id, err)
			}
			counts[c]++
			if counts[c] >= minMatch {
				return
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge with min match=%d", minMatch)
}

func waitForKeyStateMin(t *testing.T, nodes []*testNode, key []byte, exists bool, minMatch int, timeout time.Duration) {
	t.Helper()
	if minMatch <= 0 {
		t.Fatalf("minMatch must be > 0, got %d", minMatch)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		matched := 0
		for _, n := range nodes {
			if n == nil {
				continue
			}
			if n.rt.bootstrapping.Load() {
				continue
			}
			has, err := keyExists(t, n.db, key)
			if err != nil {
				t.Fatalf("read key on node %d: %v", n.id, err)
			}
			if has == exists {
				matched++
				if matched >= minMatch {
					return
				}
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("key state did not converge with min match=%d for key=%q exists=%t", minMatch, string(key), exists)
}
