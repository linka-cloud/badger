package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	"go.linka.cloud/badger/v4"
)

type Options struct {
	Name          string
	Addrs         []string
	GossipPort    int
	GRPCPort      int
	Tick          time.Duration
	EncryptionKey string

	serverCert []byte
	serverKey  []byte
	clientCert []byte
	clientKey  []byte
	clientCA   []byte
	tlsConfig  *tls.Config

	NodeID                uint64
	GRPCAddr              string
	MemberlistBindAddr    string
	MemberlistAdvertise   string
	Join                  []string
	StateDir              string
	BatchSize             int
	ApplyTimeout          time.Duration
	HeartbeatInterval     time.Duration
	LeaderTimeout         time.Duration
	ElectionTimeout       time.Duration
	StartupGrace          time.Duration
	KnownUnreachableAfter time.Duration
	RequiredSeenRatio     float64
	QuorumAcks            int
	Workload              bool
	WorkloadWrites        int
	WorkloadDeletes       int
	ValueLogFileSize      int64
	CompactionInterval    time.Duration
	WALGCRatio            float64
	WALGCMaxRuns          int
	SnapshotChunkSize     int
	ExtraServices         []func(registrar grpc.ServiceRegistrar)

	testHooks *testHooks
}

type testHooks struct {
	onLoadState             func(*persistedState) error
	onSaveState             func(persistedState) error
	onApplyFramesToPeer     func(context.Context, peerInfo, uint64, uint64, []*badger.WALFrame) error
	onSyncPeer              func(uint64, peerInfo) error
	onAcceptLeader          func(uint64, uint64) error
	onBeforeSnapshotSend    func(uint64, peerInfo, []byte) error
	onBeforeSnapshotDone    func(uint64, peerInfo, badger.WALCursor) error
	onBeforeSnapshotReset   func(uint64, peerInfo) error
	onAfterReplicationBatch func(replicationTask, error)
}

func defaultOptions() Options {
	return Options{
		BatchSize:             16384,
		ApplyTimeout:          time.Second,
		HeartbeatInterval:     50 * time.Millisecond,
		LeaderTimeout:         3 * time.Second,
		ElectionTimeout:       150 * time.Millisecond,
		StartupGrace:          2 * time.Second,
		KnownUnreachableAfter: 5 * time.Second,
		RequiredSeenRatio:     0.5,
		ValueLogFileSize:      64 << 20,
		WALGCRatio:            0.01,
		WALGCMaxRuns:          8,
		SnapshotChunkSize:     1 << 20,
	}
}

func normalizeOptions(o *Options) {
	if o == nil {
		return
	}
	if o.NodeID == 0 {
		o.NodeID = parseNodeIDFromName(o.Name)
	}
	if o.NodeID == 0 {
		o.NodeID = 1
	}
	if o.Name == "" {
		o.Name = strconv.FormatUint(o.NodeID, 10)
	}
	if o.GRPCPort <= 0 {
		o.GRPCPort = 19000 + int(o.NodeID)
	}
	if o.GossipPort <= 0 {
		o.GossipPort = 7945 + int(o.NodeID)
	}
	if o.GRPCAddr == "" {
		o.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", o.GRPCPort)
	}
	if o.MemberlistBindAddr == "" {
		o.MemberlistBindAddr = fmt.Sprintf("127.0.0.1:%d", o.GossipPort)
	}
	if len(o.Join) == 0 && len(o.Addrs) > 0 {
		o.Join = append([]string(nil), o.Addrs...)
	}
	if o.StateDir == "" {
		o.StateDir = defaultStateDir(o.NodeID)
	}
	if o.HeartbeatInterval <= 0 && o.Tick > 0 {
		o.HeartbeatInterval = o.Tick
	}
	if o.Tick > 0 {
		if o.ElectionTimeout <= 0 {
			o.ElectionTimeout = o.Tick
		}
		if o.LeaderTimeout <= 0 {
			o.LeaderTimeout = 20 * o.Tick
		}
	}
}

func parseNodeIDFromName(name string) uint64 {
	if name == "" {
		return 0
	}
	if id, err := strconv.ParseUint(name, 10, 64); err == nil {
		return id
	}
	idx := strings.LastIndexByte(name, '-')
	if idx < 0 || idx+1 >= len(name) {
		return 0
	}
	id, err := strconv.ParseUint(name[idx+1:], 10, 64)
	if err != nil {
		return 0
	}
	return id
}

type Option func(*Options)

func WithName(name string) Option {
	return func(o *Options) {
		o.Name = name
	}
}

func WithAddrs(addrs ...string) Option {
	return func(o *Options) {
		o.Addrs = addrs
	}
}

func WithGossipPort(port int) Option {
	return func(o *Options) {
		o.GossipPort = port
	}
}

func WithGRPCPort(port int) Option {
	return func(o *Options) {
		o.GRPCPort = port
	}
}

func WithTick(ms int) Option {
	return func(o *Options) {
		if ms > 100 {
			o.Tick = time.Duration(ms) * time.Millisecond
		}
	}
}

func WithServerCert(cert []byte) Option {
	return func(o *Options) {
		o.serverCert = cert
	}
}

func WithServerKey(key []byte) Option {
	return func(o *Options) {
		o.serverKey = key
	}
}

func WithClientCert(cert []byte) Option {
	return func(o *Options) {
		o.clientCert = cert
	}
}

func WithClientKey(key []byte) Option {
	return func(o *Options) {
		o.clientKey = key
	}
}

func WithClientCA(ca []byte) Option {
	return func(o *Options) {
		o.clientCA = ca
	}
}

func WithTLSConfig(config *tls.Config) Option {
	return func(o *Options) {
		o.tlsConfig = config
	}
}

func WithEncryptionKey(key string) Option {
	return func(o *Options) {
		o.EncryptionKey = key
	}
}

func WithExtraServices(services ...func(registrar grpc.ServiceRegistrar)) Option {
	return func(o *Options) {
		o.ExtraServices = append(o.ExtraServices, services...)
	}
}

func (o *Options) TLS() (*tls.Config, error) {
	if o.tlsConfig != nil {
		return o.tlsConfig, nil
	}
	if o.serverKey == nil && o.serverCert == nil {
		return nil, nil
	}
	if o.serverKey == nil {
		return nil, errors.New("missing server key")
	}
	if o.serverCert == nil {
		return nil, errors.New("missing server certificate")
	}
	cert, err := tls.X509KeyPair(o.serverCert, o.serverKey)
	if err != nil {
		return nil, err
	}
	var (
		clientCA   *x509.CertPool
		clientCert tls.Certificate
		auth       tls.ClientAuthType
	)
	if o.clientCA != nil {
		clientCA = x509.NewCertPool()
		if ok := clientCA.AppendCertsFromPEM(o.clientCA); !ok {
			return nil, fmt.Errorf("invalid client ca")
		}
		auth = tls.RequireAndVerifyClientCert
	}
	if o.clientCert != nil && o.clientKey != nil {
		clientCert, err = tls.X509KeyPair(o.clientCert, o.clientKey)
		if err != nil {
			return nil, err
		}
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert, clientCert},
		ClientCAs:    clientCA,
		ClientAuth:   auth,
	}, nil
}
