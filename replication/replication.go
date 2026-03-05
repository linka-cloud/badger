package replication

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	replicationpb "go.linka.cloud/badger/replication/pb"
	"google.golang.org/grpc"

	"go.linka.cloud/badger/v4"
)

type peerInfo struct {
	ID       uint64
	GRPCAddr string
	Epoch    uint64
	Alive    bool
	LastSeen time.Time
}

type nodeState struct {
	ID       uint64
	Epoch    uint64
	LeaderID uint64
	Cursor   badger.WALCursor
}

func newRuntime(cfg Options) *Replication {
	ctx, cancel := context.WithCancel(context.Background())
	normalizeOptions(&cfg)
	return &Replication{
		cfg:              cfg,
		store:            newStateStore(cfg.StateDir),
		role:             badger.ReplicationRoleReplica,
		startupAt:        time.Now(),
		peers:            map[uint64]peerInfo{},
		knownMembers:     map[uint64]persistedMember{},
		leaderSubs:       map[uint64]chan string{},
		coordinatorPulse: make(chan struct{}, 1),
		replCh:           make(chan replicationTask, 1024),
		ctx:              ctx,
		cancel:           cancel,
		clients:          map[uint64]*grpcPeerClient{},
		recoveringPeers:  map[uint64]struct{}{},
	}
}

func New(opts ...Option) *Replication {
	cfg := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return newRuntime(cfg)
}

func (r *Replication) Options() Options {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg
}

type Replication struct {
	replicationpb.UnimplementedClusterControlServer

	cfg   Options
	db    *badger.DB
	store *stateStore

	mu               sync.RWMutex
	applyMu          sync.Mutex
	started          bool
	starting         bool
	reconcileRunning bool
	role             badger.ReplicationRole
	bootstrapping    atomic.Bool
	leaderID         uint64
	epoch            uint64
	lastHeartbeat    time.Time
	lastReconcile    time.Time
	electionRunning  bool
	startupAt        time.Time
	pending          []*badger.WALFrame
	peers            map[uint64]peerInfo
	knownMembers     map[uint64]persistedMember
	leaderSubs       map[uint64]chan string
	leaderSubsSeq    uint64
	lastLeaderAddr   string
	coordinatorPulse chan struct{}
	replCh           chan replicationTask

	ctx    context.Context
	cancel context.CancelFunc
	closed sync.Once

	grpcServer *grpc.Server
	ml         *memberlist.Memberlist

	clientMu sync.Mutex
	clients  map[uint64]*grpcPeerClient

	recoveryMu      sync.Mutex
	recoveringPeers map[uint64]struct{}
	recoveryRunMu   sync.RWMutex
}

type grpcPeerClient struct {
	addr   string
	conn   *grpc.ClientConn
	client replicationpb.ClusterControlClient
	stream grpc.BidiStreamingClient[replicationpb.ApplyFramesRequest, replicationpb.ApplyFramesResponse]
	mu     sync.Mutex
}

type replicationTask struct {
	batch    []*badger.WALFrame
	leaderID uint64
	epoch    uint64
	sync     bool
	done     chan error
}

func (r *Replication) Start(ctx context.Context, db *badger.DB) error {
	if ctx == nil {
		ctx = context.Background()
	}
	checkStartCtx := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	var startErr error
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return nil
	}
	if r.starting {
		r.mu.Unlock()
		return fmt.Errorf("runtime start already in progress")
	}
	r.starting = true
	r.db = db
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if startErr != nil {
			r.started = false
			r.starting = false
			return
		}
		r.started = true
		r.starting = false
	}()
	if err := checkStartCtx(); err != nil {
		startErr = err
		return startErr
	}

	st, err := r.store.load()
	if err != nil {
		startErr = fmt.Errorf("load cluster state: %w", err)
		return startErr
	}
	if r.cfg.testHooks != nil && r.cfg.testHooks.onLoadState != nil {
		if err := r.cfg.testHooks.onLoadState(&st); err != nil {
			startErr = fmt.Errorf("load state hook: %w", err)
			return startErr
		}
	}
	if err := checkStartCtx(); err != nil {
		startErr = err
		return startErr
	}
	r.mu.Lock()
	if st.NodeID == 0 {
		st.NodeID = r.cfg.NodeID
	}
	if st.NodeID != r.cfg.NodeID {
		r.mu.Unlock()
		startErr = fmt.Errorf("state node id mismatch: file=%d flag=%d", st.NodeID, r.cfg.NodeID)
		return startErr
	}
	r.epoch = st.Epoch
	r.leaderID = st.LastKnownLeaderID
	for _, m := range st.KnownMembers {
		r.knownMembers[m.ID] = m
	}
	subs, leader, changed := r.captureLeaderChange()
	r.mu.Unlock()
	if changed {
		publishLeaderChange(subs, leader)
	}
	if err := checkStartCtx(); err != nil {
		startErr = err
		return startErr
	}

	if err := r.startGRPC(); err != nil {
		startErr = err
		return startErr
	}
	if err := checkStartCtx(); err != nil {
		r.rollbackStartup()
		startErr = err
		return startErr
	}
	if err := r.startMemberlist(); err != nil {
		r.rollbackStartup()
		startErr = err
		return startErr
	}

	go r.loop(ctx)
	go r.replicationLoop(ctx)
	if r.cfg.CompactionInterval > 0 {
		go r.compactionLoop(ctx)
	}
	if r.cfg.Workload {
		go r.runWorkload(ctx)
	}
	return nil
}

func (r *Replication) rollbackStartup() {
	if r.ml != nil {
		_ = r.ml.Leave(500 * time.Millisecond)
		r.ml.Shutdown()
		r.ml = nil
	}
	if r.grpcServer != nil {
		r.grpcServer.Stop()
		r.grpcServer = nil
	}
}

func (r *Replication) Role() badger.ReplicationRole {
	if r.bootstrapping.Load() {
		return badger.ReplicationRolePrimary
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role
}

// lock must be held by caller.
func (r *Replication) alivePeers() []peerInfo {
	peers := make([]peerInfo, 0, len(r.peers))
	for _, p := range r.peers {
		if p.ID == r.cfg.NodeID || !p.Alive || p.GRPCAddr == "" {
			continue
		}
		peers = append(peers, p)
	}
	return peers
}

func (r *Replication) saveState(st persistedState) error {
	if r.cfg.testHooks != nil && r.cfg.testHooks.onSaveState != nil {
		if err := r.cfg.testHooks.onSaveState(st); err != nil {
			return err
		}
	}
	return r.store.save(st)
}
