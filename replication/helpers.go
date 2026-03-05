package replication

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"

	"go.linka.cloud/badger/v4"
	badgerpb "go.linka.cloud/badger/v4/pb"

	"go.linka.cloud/badger/replication/pb"
)

func pbCursorFromWALCursor(c badger.WALCursor) *badgerpb.WALCursor {
	if c.IsZero() {
		return nil
	}
	return &badgerpb.WALCursor{Fid: c.Fid, Offset: c.Offset, Len: c.Len}
}

func walCursorFromPBCursor(c *badgerpb.WALCursor) badger.WALCursor {
	if c == nil {
		return badger.WALCursor{}
	}
	return badger.WALCursor{Fid: c.Fid, Offset: c.Offset, Len: c.Len}
}

func (r *Replication) localNodeState() (nodeState, error) {
	r.mu.RLock()
	state := nodeState{ID: r.cfg.NodeID, Epoch: r.epoch, LeaderID: r.leaderID}
	r.mu.RUnlock()
	cur, err := r.db.Replication().Cursor()
	if err != nil {
		return nodeState{}, err
	}
	state.Cursor = cur
	return state, nil
}

func nodeStateFromPB(resp *replicationpb.GetNodeStateResponse) nodeState {
	if resp == nil {
		return nodeState{}
	}
	return nodeState{
		ID:       resp.GetNodeId(),
		Epoch:    resp.GetEpoch(),
		LeaderID: resp.GetLeaderId(),
		Cursor:   walCursorFromPBCursor(resp.GetCursor()),
	}
}

func isStatePreferred(a, b nodeState) bool {
	if b.Cursor.Less(a.Cursor) {
		return true
	}
	if a.Cursor.Less(b.Cursor) {
		return false
	}
	if a.Epoch != b.Epoch {
		return a.Epoch > b.Epoch
	}
	return a.ID > b.ID
}

func splitHostPort(v string) (string, int, error) {
	h, p, err := net.SplitHostPort(v)
	if err != nil {
		return "", 0, fmt.Errorf("invalid host:port %q: %w", v, err)
	}
	pi, err := strconv.Atoi(p)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in %q: %w", v, err)
	}
	return h, pi, nil
}

func parseJoin(v string) []string {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func ParseJoin(v string) []string {
	return parseJoin(v)
}

func defaultStateDir(nodeID uint64) string {
	return filepath.Join("badger-test-replication", "state", fmt.Sprintf("node-%d", nodeID))
}

func DefaultStateDir(nodeID uint64) string {
	return defaultStateDir(nodeID)
}
