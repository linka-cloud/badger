package badger

import (
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/shaj13/raft"
	"github.com/shaj13/raft/transport"
	"github.com/shaj13/raft/transport/raftgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftStartOption = raft.StartOption

var (
	WithFallback        = raft.WithFallback
	WithJoin            = raft.WithJoin
	WithForceJoin       = raft.WithForceJoin
	WithInitCluster     = raft.WithInitCluster
	WithForceNewCluster = raft.WithForceNewCluster
	WithRestore         = raft.WithRestore
	WithRestart         = raft.WithRestart
	WithMembers         = raft.WithMembers
)

var (
	ErrStateDirRequired = errors.New("raft: StateDir required")
	ErrAddrRequired     = errors.New("raft: Addr required")
)

var DefaultRaftOptions = &RaftOptions{
	TickInterval: 10 * time.Millisecond,
	DialOptions:  []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
}

type RaftOptions struct {
	StateDir         string
	TickInterval     time.Duration
	SnapshotInterval uint64
	DialOptions      []grpc.DialOption
	CallOptions      []grpc.CallOption
	ServerOptions    []grpc.ServerOption
	TLSConfig        *tls.Config
	StartOptions     []RaftStartOption
	Addr             string
	StateChange      func(raft.StateType)
}

func (o *RaftOptions) newNode(db *DB) (*raftNode, error) {
	if o == nil {
		return nil, nil
	}
	if o.StateDir == "" {
		return nil, ErrStateDirRequired
	}
	if o.Addr == "" {
		return nil, ErrAddrRequired
	}
	o.StartOptions = append(o.StartOptions, raft.WithAddress(o.Addr))

	ch := make(chan raft.StateType, 1)

	opts := []raft.Option{
		raft.WithStateDIR(o.StateDir),
		raft.WithPreVote(),
		raft.WithCheckQuorum(),
		raft.WithDisableProposalForwarding(),
		raft.WithStateChangeCh(ch),
	}
	if o.TickInterval != 0 {
		opts = append(opts, raft.WithTickInterval(o.TickInterval))
	}
	if o.SnapshotInterval != 0 {
		opts = append(opts, raft.WithSnapshotInterval(o.SnapshotInterval))
	}
	raftgrpc.Register(
		raftgrpc.WithCallOptions(o.CallOptions...),
		raftgrpc.WithDialOptions(o.DialOptions...),
	)

	n := &raftNode{db: db, sch: ch, rch: make(chan struct{})}
	n.node = raft.NewNode(n, transport.GRPC, opts...)

	var err error
	if o.TLSConfig != nil {
		o.ServerOptions = append(o.ServerOptions, grpc.Creds(credentials.NewTLS(o.TLSConfig)))
		n.lis, err = tls.Listen("tcp", o.Addr, o.TLSConfig)
	} else {
		n.lis, err = net.Listen("tcp", o.Addr)
	}
	if err != nil {
		return nil, err
	}

	n.srv = grpc.NewServer(o.ServerOptions...)
	raftgrpc.RegisterHandler(n.srv, n.node.Handler())

	return n, nil
}

func (o *RaftOptions) WithStateDir(dir string) *RaftOptions {
	o.StateDir = dir
	return o
}

func (o *RaftOptions) WithTickInterval(d time.Duration) *RaftOptions {
	o.TickInterval = d
	return o
}

func (o *RaftOptions) WithSnapshotInterval(i uint64) *RaftOptions {
	o.SnapshotInterval = i
	return o
}

func (o *RaftOptions) WithDialOptions(opts ...grpc.DialOption) *RaftOptions {
	o.DialOptions = append(o.DialOptions, opts...)
	return o
}

func (o *RaftOptions) WithCallOptions(opts ...grpc.CallOption) *RaftOptions {
	o.CallOptions = append(o.CallOptions, opts...)
	return o
}

func (o *RaftOptions) WithServerOptions(opts ...grpc.ServerOption) *RaftOptions {
	o.ServerOptions = append(o.ServerOptions, opts...)
	return o
}

func (o *RaftOptions) WithTLSConfig(cfg *tls.Config) *RaftOptions {
	o.TLSConfig = cfg
	return o
}

func (o *RaftOptions) WithStartOptions(opts ...RaftStartOption) *RaftOptions {
	o.StartOptions = append(o.StartOptions, opts...)
	return o
}

func (o *RaftOptions) WithAddr(addr string) *RaftOptions {
	o.Addr = addr
	return o
}

func (o *RaftOptions) WithStateChange(fn func(raft.StateType)) *RaftOptions {
	o.StateChange = fn
	return o
}
