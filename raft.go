package badger

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/shaj13/raft"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

type wait struct {
	sync.WaitGroup
	err error
}

func (w *wait) Wait() error {
	w.WaitGroup.Wait()
	return w.err
}

func waitFor(fn func() error) waiter {
	w := &wait{}
	w.Add(1)
	go func() {
		defer w.Done()
		w.err = fn()
	}()
	return w
}

var _ raft.StateMachine = (*raftNode)(nil)

type raftNode struct {
	db    *DB
	node  *raft.Node
	lis   net.Listener
	srv   *grpc.Server
	ch    chan raft.StateType
	smu   sync.Mutex
	ready chan struct{}
}

func (n *raftNode) start() {
	if n == nil {
		return
	}
	go n.run()
	<-n.ready
	n.db.opt.Infof("raft: ready")
}

func (n *raftNode) run() {
	if n == nil {
		return
	}
	defer n.db.Close()

	maybeNotifyReady := func() {
		select {
		case <-n.ready:
		default:
			go func() {
				n.linearizableRead()
				close(n.ready)
			}()
		}
	}

	g := errgroup.Group{}
	g.Go(func() error {
		if err := n.srv.Serve(n.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			n.db.opt.Errorf("raft/gRPC failed to serve: %v", err)
			return err
		}
		n.db.opt.Infof("raft/gRPC: server stopped")
		return nil
	})
	g.Go(func() error {
		defer close(n.ch)
		if err := n.node.Start(n.db.opt.RaftOptions.StartOptions...); err != nil && !errors.Is(err, raft.ErrNodeStopped) {
			n.db.opt.Errorf("raft/Node failed to start: %v", err)
			return err
		}
		n.db.opt.Infof("raft/Node: stopped")
		return nil
	})
	g.Go(func() error {
		for s := range n.ch {
			maybeNotifyReady()
			if n.db.opt.RaftOptions.StateChange != nil {
				n.db.opt.RaftOptions.StateChange(s)
			}
		}
		return nil
	})
	g.Wait()
}

func (n *raftNode) linearizableRead() error {
	if n == nil {
		return nil
	}
	return n.node.LinearizableRead(context.Background())
}

func (n *raftNode) propose(entries []*Entry) (waiter, error) {
	if !n.leader() {
		return nil, raft.ErrNotLeader
	}
	var kvs pb.KVList
	for _, v := range entries {
		kvs.Kv = append(kvs.Kv, v.kv())
	}
	e := &pb.RaftEntry{
		Entry: &pb.RaftEntry_Kv{
			Kv: &kvs,
		},
	}
	b, err := e.Marshal()
	if err != nil {
		return nil, err
	}
	return waitFor(func() error {
		return n.node.Replicate(context.Background(), b)
	}), nil
}

func (n *raftNode) Apply(bytes []byte) error {
	e := &pb.RaftEntry{}
	if err := e.Unmarshal(bytes); err != nil {
		n.db.opt.Errorf("raft: Failed to unmarshal: %v", err)
		return err
	}
	switch e := e.Entry.(type) {
	case *pb.RaftEntry_Kv:
		return n.applyKVs(e.Kv)
	default:
		n.db.opt.Errorf("raft: unknown entry type: %T", e)
		return nil
	}
}

func (n *raftNode) applyKVs(kvs *pb.KVList) error {
	n.db.opt.Debugf("raft: Applying %d bytes", len(kvs.Kv))

	// TODO(adphi): maybe use KVLoader
	var (
		entries []*Entry
		version uint64
	)

	for _, v := range kvs.Kv {
		var userMeta, meta byte
		if len(v.UserMeta) > 0 {
			userMeta = v.UserMeta[0]
		}
		if len(v.Meta) > 0 {
			meta = v.Meta[0]
		}
		entries = append(entries, &Entry{
			Key:       y.KeyWithTs(v.Key, v.Version),
			Value:     v.Value,
			ExpiresAt: v.ExpiresAt,
			UserMeta:  userMeta,
			meta:      meta,
		})
		// only one version ?
		if v.Version > version {
			version = v.Version
		}
	}

	n.db.opt.Debugf("raft: applying %d entries at version %d", len(entries), version)
	w, err := n.db.sendToWriteCh(entries, false)
	if err != nil {
		n.db.opt.Errorf("raft: failed to send entries: %v", err)
		return err
	}
	if err := w.Wait(); err != nil {
		n.db.opt.Errorf("raft: failed to apply entries: %v", err)
		return err
	}

	select {
	case <-n.ready:
		// skip when this is a transaction, which can occur only
		// when raft is ready and this node is the current leader
		if n.leader() || n.db.opt.managedTxns {
			n.db.opt.Debugf("raft: skipping oracle update (version: %d)", version)
			return nil
		}
	default:
	}

	if v := n.db.orc.nextTs(); v > version {
		n.db.opt.Debugf("raft: skipping oracle update (tx: %d) (next: %d)", version, v)
		return nil
	}

	n.db.opt.Debugf("raft: updating oracle to %d", version)
	n.db.orc.nextTxnTs = version
	n.db.orc.txnMark.Done(n.db.orc.nextTxnTs)
	n.db.orc.readMark.Done(n.db.orc.nextTxnTs)
	n.db.orc.incrementNextTs()

	return nil

}

func (n *raftNode) Snapshot() (io.ReadCloser, error) {
	select {
	case <-n.ready:
	default:
		return nil, errors.New("raft node not ready")
	}
	n.smu.Lock()
	n.db.opt.Infof("raft: creating snapshot")
	readTs := n.db.MaxVersion()
	r, w := io.Pipe()
	start := time.Now()
	e, err := zstd.NewWriter(w)
	if err != nil {
		return nil, err
	}
	n.db.stopCompactions()
	go func() {
		defer n.smu.Unlock()
		defer w.Close()
		defer e.Close()
		defer n.db.startCompactions()
		n.db.opt.Infof("raft: starting snapshot stream with readTs %d", readTs)
		stream := n.db.NewStream()
		stream.LogPrefix = "raft/snapshot"
		stream.SinceTs = 0
		stream.readTs = readTs
		v, err := stream.Backup(e, 0)
		if err != nil {
			n.db.opt.Errorf("raft: failed to backup: %v", err)
			return
		}
		n.db.opt.Infof("raft: snapshot to %d done in %v", v, time.Since(start))
	}()
	return r, nil
}

func (n *raftNode) Restore(closer io.ReadCloser) error {
	n.db.opt.Infof("raft: restoring snapshot")
	defer closer.Close()
	start := time.Now()
	d, err := zstd.NewReader(closer)
	if err != nil {
		return err
	}
	defer d.Close()
	n.db.opt.Debugf("raft: dropping all")
	// TODO(adphi): create backup before in case of something goes wrong
	if err := n.db.DropAll(); err != nil {
		return err
	}
	n.db.opt.Debugf("raft: loading snapshot")
	if err := n.db.Load(d, 256); err != nil {
		n.db.opt.Errorf("raft: failed to load snapshot: %v", err)
		return err
	}
	// TODO(adphi): delete the backup
	n.db.opt.Infof("raft: snapshot restored at %d in %v", n.db.MaxVersion(), time.Since(start))
	return nil
}

func (n *raftNode) leader() bool {
	if n == nil {
		return true
	}
	return n.node.Leader() == n.node.Whoami()
}

func (n *raftNode) enabled() bool {
	return n != nil
}

func (n *raftNode) close() error {
	if n == nil {
		return nil
	}
	var err error
	if n.leader() {
		err = multierr.Append(err, n.node.Stepdown(context.Background()))
	}
	err = multierr.Combine(err, n.node.Shutdown(context.Background()))
	n.srv.GracefulStop()
	err = multierr.Combine(err, n.lis.Close())
	return err
}
