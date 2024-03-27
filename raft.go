package badger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/klauspost/compress/zstd"
	"github.com/shaj13/raft"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

type wait struct {
	wg  sync.WaitGroup
	err error
}

func (w *wait) Wait() error {
	w.wg.Wait()
	return w.err
}

func waitFor(fn func() error) waiter {
	w := &wait{}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.err = fn()
	}()
	return w
}

var _ raft.StateMachine = (*raftNode)(nil)

type raftNode struct {
	db   *DB
	node *raft.Node
	lis  net.Listener
	srv  *grpc.Server
	sch  chan raft.StateType
	smu  sync.Mutex
	rch  chan struct{}
}

func (n *raftNode) start() {
	if n == nil {
		return
	}
	go n.run()
	<-n.rch
	n.db.opt.Infof("raft: ready")
}

func (n *raftNode) run() {
	if n == nil {
		return
	}
	clean := func() {
		if !n.ready() {
			close(n.rch)
		}
		n.db.Close()
	}
	defer clean()

	maybeNotifyReady := func() {
		if !n.ready() {
			go func() {
				n.linearizableRead()
				close(n.rch)
			}()
		}
	}

	g := errgroup.Group{}
	g.Go(func() error {
		defer clean()
		if err := n.srv.Serve(n.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			n.db.opt.Errorf("raft/gRPC failed to serve: %v", err)
			return err
		}
		n.db.opt.Infof("raft/gRPC: server stopped")
		return nil
	})
	g.Go(func() error {
		defer clean()
		defer close(n.sch)
		if err := n.node.Start(n.db.opt.RaftOptions.StartOptions...); err != nil && !errors.Is(err, raft.ErrNodeStopped) {
			n.db.opt.Errorf("raft/Node failed to start: %v", err)
			return err
		}
		n.db.opt.Infof("raft/Node: stopped")
		return nil
	})
	g.Go(func() error {
		defer clean()
		for s := range n.sch {
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

	var version uint64
	entries := make([]*Entry, len(kvs.Kv))

	for i, v := range kvs.Kv {
		userMeta, meta := first(v.UserMeta), first(v.Meta)
		entries[i] = &Entry{
			Key:       y.KeyWithTs(v.Key, v.Version),
			Value:     v.Value,
			ExpiresAt: v.ExpiresAt,
			UserMeta:  userMeta,
			meta:      meta,
		}
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

	if n.ready() && (n.leader() || n.db.opt.managedTxns) {
		// skip when this is a transaction, which can occur only
		// when raft is ready and this node is the current leader
		n.db.opt.Debugf("raft: skipping oracle update (version: %d)", version)
		return nil
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
	if !n.ready() {
		return nil, fmt.Errorf("%w: raft node not ready", raft.ErrFailedPrecondition)
	}
	n.smu.Lock()
	n.db.opt.Infof("raft: creating snapshot")
	readTs := n.db.MaxVersion()
	r, w := io.Pipe()
	start := time.Now()
	e, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(n.db.opt.ZSTDCompressionLevel)))
	if err != nil {
		return nil, err
	}
	go func() {
		defer n.smu.Unlock()
		defer w.Close()
		defer e.Close()
		n.db.opt.Infof("raft: starting snapshot stream with readTs %d", readTs)
		stream := n.db.NewStream()
		stream.LogPrefix = "raft/snapshot"
		stream.SinceTs = 0
		stream.readTs = readTs
		stream.Send = func(buf *z.Buffer) error {
			list, err := BufferToKVList(buf)
			if err != nil {
				return err
			}
			out := list.Kv[:0]
			for _, kv := range list.Kv {
				if !kv.StreamDone {
					// Don't pick stream done changes.
					out = append(out, kv)
				}
			}
			list.Kv = out
			return writeTo(list, e)
		}
		if err := stream.Orchestrate(context.Background()); err != nil {
			n.db.opt.Errorf("raft: failed to backup: %v", err)
			return
		}
		n.db.opt.Infof("raft: snapshot to %d done in %v", readTs, time.Since(start))
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
	n.db.opt.Infof("raft: dropping all local data")
	// TODO(adphi): create backup before in case of something goes wrong
	if err := n.db.DropAll(); err != nil {
		return err
	}
	n.db.opt.Infof("raft: loading snapshot")
	if err := n.db.Load(d, 16); err != nil {
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

func (n *raftNode) ready() bool {
	if n == nil {
		return true
	}
	select {
	case <-n.rch:
		return true
	default:
		return false
	}
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
