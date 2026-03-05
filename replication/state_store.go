package replication

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type persistedMember struct {
	ID            uint64 `json:"id"`
	GRPCAddr      string `json:"grpc_addr"`
	GossipAddr    string `json:"gossip_addr,omitempty"`
	LastSeenUnix  int64  `json:"last_seen_unix"`
	LastEpochSeen uint64 `json:"last_epoch_seen"`
}

type persistedState struct {
	NodeID            uint64                     `json:"node_id"`
	Epoch             uint64                     `json:"epoch"`
	LastKnownLeaderID uint64                     `json:"last_known_leader_id"`
	KnownMembers      map[string]persistedMember `json:"known_members"`
}

type stateStore struct {
	path string
	mu   sync.Mutex
}

func newStateStore(dir string) *stateStore {
	return &stateStore{path: filepath.Join(dir, "cluster_state.json")}
}

func (s *stateStore) load() (persistedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return persistedState{KnownMembers: map[string]persistedMember{}}, nil
		}
		return persistedState{}, err
	}
	st := persistedState{}
	if err := json.Unmarshal(b, &st); err != nil {
		return persistedState{}, err
	}
	if st.KnownMembers == nil {
		st.KnownMembers = map[string]persistedMember{}
	}
	return st, nil
}

func (s *stateStore) save(st persistedState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
