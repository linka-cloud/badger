package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"go.linka.cloud/badger/v4"

	"go.linka.cloud/badger/replication/pb"
)

func (r *Replication) peerClientFor(ctx context.Context, p peerInfo) (*grpcPeerClient, error) {
	r.clientMu.Lock()
	defer r.clientMu.Unlock()
	if c, ok := r.clients[p.ID]; ok && c.addr == p.GRPCAddr {
		return c, nil
	}
	tlsCfg, err := r.cfg.TLS()
	if err != nil {
		return nil, fmt.Errorf("tls config: %w", err)
	}
	dialOpts := []grpc.DialOption{grpc.WithBlock()}
	if tlsCfg != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg.Clone())))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.DialContext(ctx, p.GRPCAddr, dialOpts...)
	if err != nil {
		return nil, err
	}
	c := &grpcPeerClient{addr: p.GRPCAddr, conn: conn, client: replicationpb.NewClusterControlClient(conn)}
	if old, ok := r.clients[p.ID]; ok {
		r.closePeerClient(old)
	}
	r.clients[p.ID] = c
	return c, nil
}

func (r *Replication) closePeerClient(pc *grpcPeerClient) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	if pc.stream != nil {
		_ = pc.stream.CloseSend()
		pc.stream = nil
	}
	pc.mu.Unlock()
	_ = pc.conn.Close()
}

func (r *Replication) applyFramesStream(ctx context.Context, p peerInfo, req *replicationpb.ApplyFramesRequest) (*replicationpb.ApplyFramesResponse, error) {
	pc, err := r.peerClientFor(ctx, p)
	if err != nil {
		return nil, err
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	stream, err := pc.client.ApplyFrames(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(req); err != nil {
		_ = stream.CloseSend()
		return nil, err
	}
	resp, err := stream.Recv()
	_ = stream.CloseSend()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (r *Replication) closePeerStream(peerID uint64) {
	r.clientMu.Lock()
	defer r.clientMu.Unlock()
	pc, ok := r.clients[peerID]
	if !ok {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.stream != nil {
		_ = pc.stream.CloseSend()
		pc.stream = nil
	}
}

func (r *Replication) callElection(ctx context.Context, p peerInfo, candidate nodeState) (*replicationpb.ElectionResponse, error) {
	pc, err := r.peerClientFor(ctx, p)
	if err != nil {
		return nil, err
	}
	return pc.client.Election(ctx, &replicationpb.ElectionRequest{
		CandidateId: candidate.ID,
		Epoch:       candidate.Epoch,
		Cursor:      pbCursorFromWALCursor(candidate.Cursor),
	})
}

func (r *Replication) callCoordinator(ctx context.Context, p peerInfo, leaderID, epoch uint64) (*replicationpb.CoordinatorResponse, error) {
	pc, err := r.peerClientFor(ctx, p)
	if err != nil {
		return nil, err
	}
	return pc.client.Coordinator(ctx, &replicationpb.CoordinatorRequest{LeaderId: leaderID, Epoch: epoch})
}

func (r *Replication) callHeartbeat(ctx context.Context, p peerInfo, leaderID, epoch uint64) (*replicationpb.HeartbeatResponse, error) {
	pc, err := r.peerClientFor(ctx, p)
	if err != nil {
		return nil, err
	}
	return pc.client.Heartbeat(ctx, &replicationpb.HeartbeatRequest{LeaderId: leaderID, Epoch: epoch})
}

func (r *Replication) callGetCursor(ctx context.Context, p peerInfo, leaderID, epoch uint64) (*replicationpb.CursorResponse, error) {
	pc, err := r.peerClientFor(ctx, p)
	if err != nil {
		return nil, err
	}
	return pc.client.Cursor(ctx, &replicationpb.CursorRequest{LeaderId: leaderID, Epoch: epoch})
}

func (r *Replication) callGetNodeState(ctx context.Context, p peerInfo) (*replicationpb.GetNodeStateResponse, error) {
	pc, err := r.peerClientFor(ctx, p)
	if err != nil {
		return nil, err
	}
	return pc.client.GetNodeState(ctx, &replicationpb.GetNodeStateRequest{})
}

func (r *Replication) Election(_ context.Context, req *replicationpb.ElectionRequest) (*replicationpb.ElectionResponse, error) {
	self, err := r.localNodeState()
	if err != nil {
		return nil, err
	}
	if req.GetEpoch() > self.Epoch {
		r.stepDown(req.GetEpoch(), 0)
		self.Epoch = req.GetEpoch()
	}
	candidate := nodeState{ID: req.GetCandidateId(), Epoch: req.GetEpoch(), Cursor: walCursorFromPBCursor(req.GetCursor())}
	ack := isStatePreferred(self, candidate)
	if ack {
		go r.startElection()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return &replicationpb.ElectionResponse{Ack: ack, ResponderId: r.cfg.NodeID, ResponderEpoch: r.epoch}, nil
}

func (r *Replication) Coordinator(_ context.Context, req *replicationpb.CoordinatorRequest) (*replicationpb.CoordinatorResponse, error) {
	r.mu.Lock()
	if req.GetEpoch() < r.epoch {
		epoch := r.epoch
		r.mu.Unlock()
		return &replicationpb.CoordinatorResponse{Accepted: false, ResponderId: r.cfg.NodeID, ResponderEpoch: epoch}, nil
	}
	r.epoch = req.GetEpoch()
	r.leaderID = req.GetLeaderId()
	r.lastHeartbeat = time.Now()
	if req.GetLeaderId() == r.cfg.NodeID {
		r.role = badger.ReplicationRolePrimary
	} else {
		r.role = badger.ReplicationRoleReplica
	}
	subs, leader, changed := r.captureLeaderChange()
	state := r.snapshotState()
	epoch := r.epoch
	r.mu.Unlock()
	if changed {
		publishLeaderChange(subs, leader)
	}
	go func() {
		if err := r.saveState(state); err != nil {
			log.Printf("persist state: %v", err)
		}
	}()
	select {
	case r.coordinatorPulse <- struct{}{}:
	default:
	}
	return &replicationpb.CoordinatorResponse{Accepted: true, ResponderId: r.cfg.NodeID, ResponderEpoch: epoch}, nil
}

func (r *Replication) Heartbeat(_ context.Context, req *replicationpb.HeartbeatRequest) (*replicationpb.HeartbeatResponse, error) {
	r.mu.Lock()
	if req.GetEpoch() < r.epoch {
		epoch := r.epoch
		r.mu.Unlock()
		return &replicationpb.HeartbeatResponse{Accepted: false, ResponderId: r.cfg.NodeID, ResponderEpoch: epoch}, nil
	}
	if req.GetEpoch() == r.epoch && r.leaderID != 0 && req.GetLeaderId() != r.leaderID {
		epoch := r.epoch
		r.mu.Unlock()
		return &replicationpb.HeartbeatResponse{Accepted: false, ResponderId: r.cfg.NodeID, ResponderEpoch: epoch}, nil
	}
	r.epoch = req.GetEpoch()
	r.leaderID = req.GetLeaderId()
	r.role = badger.ReplicationRoleReplica
	r.lastHeartbeat = time.Now()
	subs, leader, changed := r.captureLeaderChange()
	epoch := r.epoch
	r.mu.Unlock()
	if changed {
		publishLeaderChange(subs, leader)
	}
	return &replicationpb.HeartbeatResponse{Accepted: true, ResponderId: r.cfg.NodeID, ResponderEpoch: epoch}, nil
}

func (r *Replication) GetNodeState(_ context.Context, _ *replicationpb.GetNodeStateRequest) (*replicationpb.GetNodeStateResponse, error) {
	state, err := r.localNodeState()
	if err != nil {
		return &replicationpb.GetNodeStateResponse{NodeId: r.cfg.NodeID, Epoch: r.currentEpoch(), Error: err.Error()}, nil
	}
	return &replicationpb.GetNodeStateResponse{
		NodeId:   state.ID,
		Epoch:    state.Epoch,
		LeaderId: state.LeaderID,
		Cursor:   pbCursorFromWALCursor(state.Cursor),
	}, nil
}

func (r *Replication) Cursor(_ context.Context, req *replicationpb.CursorRequest) (*replicationpb.CursorResponse, error) {
	if err := r.acceptLeader(req.GetLeaderId(), req.GetEpoch()); err != nil {
		return &replicationpb.CursorResponse{Accepted: false, Error: err.Error()}, nil
	}
	cur, err := r.db.Replication().Cursor()
	if err != nil {
		return &replicationpb.CursorResponse{Accepted: false, Error: err.Error()}, nil
	}
	return &replicationpb.CursorResponse{
		Accepted:       true,
		ResponderId:    r.cfg.NodeID,
		ResponderEpoch: r.currentEpoch(),
		Cursor:         pbCursorFromWALCursor(cur),
	}, nil
}

func (r *Replication) ApplyFrames(stream replicationpb.ClusterControl_ApplyFramesServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp := &replicationpb.ApplyFramesResponse{ResponderId: r.cfg.NodeID, ResponderEpoch: r.currentEpoch()}
		applyStarted := time.Now()
		if err := r.acceptLeader(req.GetLeaderId(), req.GetEpoch()); err != nil {
			resp.Accepted = false
			resp.Error = err.Error()
			if err := stream.Send(resp); err != nil {
				return err
			}
			continue
		}
		frames := make([]*badger.WALFrame, 0, len(req.GetFrames()))
		for _, f := range req.GetFrames() {
			if f == nil {
				continue
			}
			frames = append(frames, (*badger.WALFrame)(f).CloneVT())
		}
		r.applyMu.Lock()
		cur, err := func() (c badger.WALCursor, err error) {
			defer func() {
				if rec := recover(); rec != nil {
					err = fmt.Errorf("panic applying frames: %v", rec)
					log.Printf("replication apply panic: node=%d leader=%d epoch=%d frames=%d panic=%v\n%s", r.cfg.NodeID, req.GetLeaderId(), req.GetEpoch(), len(frames), rec, debug.Stack())
				}
			}()
			return r.db.Replication().ApplyFrames(frames)
		}()
		r.applyMu.Unlock()
		if err != nil {
			resp.Accepted = false
			resp.Error = err.Error()
			log.Printf("replication apply rejected: node=%d leader=%d epoch=%d frames=%d err=%v", r.cfg.NodeID, req.GetLeaderId(), req.GetEpoch(), len(req.GetFrames()), err)
		} else {
			resp.Accepted = true
			resp.Cursor = pbCursorFromWALCursor(cur)
			if d := time.Since(applyStarted); d > 100*time.Millisecond {
				log.Printf("replication apply slow: node=%d leader=%d epoch=%d frames=%d duration=%s", r.cfg.NodeID, req.GetLeaderId(), req.GetEpoch(), len(req.GetFrames()), d.Truncate(time.Millisecond))
			}
		}
		resp.ResponderEpoch = r.currentEpoch()
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (r *Replication) InstallSnapshot(stream grpc.ClientStreamingServer[replicationpb.InstallSnapshotRequest, replicationpb.InstallSnapshotResponse]) error {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	var (
		started     bool
		finalCursor badger.WALCursor
		pr          *io.PipeReader
		pw          *io.PipeWriter
		loadErrCh   chan error
	)
	recoveryStarted := time.Now()
	respond := func(accepted bool, msg string) error {
		if accepted {
			log.Printf("recovery install snapshot complete: node=%d duration=%s cursor=(%d,%d,%d)", r.cfg.NodeID, time.Since(recoveryStarted).Truncate(time.Millisecond), finalCursor.Fid, finalCursor.Offset, finalCursor.Len)
		} else {
			log.Printf("recovery install snapshot failed: node=%d duration=%s err=%s", r.cfg.NodeID, time.Since(recoveryStarted).Truncate(time.Millisecond), msg)
		}
		return stream.SendAndClose(&replicationpb.InstallSnapshotResponse{
			Accepted:       accepted,
			Error:          msg,
			Cursor:         pbCursorFromWALCursor(finalCursor),
			ResponderId:    r.cfg.NodeID,
			ResponderEpoch: r.currentEpoch(),
		})
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if !started {
			if err := r.acceptLeader(req.GetLeaderId(), req.GetEpoch()); err != nil {
				return respond(false, err.Error())
			}
			r.bootstrapping.Store(true)
			defer r.bootstrapping.Store(false)
			if req.GetReset_() {
				if err := r.db.DropAll(); err != nil {
					return respond(false, fmt.Sprintf("drop all: %v", err))
				}
			}
			pr, pw = io.Pipe()
			loadErrCh = make(chan error, 1)
			go func() {
				loadErrCh <- r.db.Load(pr, 8)
			}()
			started = true
		}
		if len(req.GetChunk()) > 0 {
			if _, err := pw.Write(req.GetChunk()); err != nil {
				return respond(false, fmt.Sprintf("snapshot write: %v", err))
			}
		}
		if req.GetDone() {
			finalCursor = walCursorFromPBCursor(req.GetCursor())
			break
		}
	}

	if !started {
		return respond(false, "empty snapshot stream")
	}
	if err := pw.Close(); err != nil {
		return respond(false, fmt.Sprintf("close snapshot writer: %v", err))
	}
	if err := <-loadErrCh; err != nil {
		return respond(false, fmt.Sprintf("load snapshot: %v", err))
	}
	if !finalCursor.IsZero() {
		if err := r.db.Replication().SetCursor(finalCursor); err != nil {
			return respond(false, fmt.Sprintf("set cursor: %v", err))
		}
	}
	return respond(true, "")
}

func (r *Replication) acceptLeader(leaderID, epoch uint64) error {
	if r.cfg.testHooks != nil && r.cfg.testHooks.onAcceptLeader != nil {
		if err := r.cfg.testHooks.onAcceptLeader(leaderID, epoch); err != nil {
			return err
		}
	}
	r.mu.Lock()
	if epoch < r.epoch {
		currentEpoch := r.epoch
		r.mu.Unlock()
		return fmt.Errorf("stale epoch %d < %d", epoch, currentEpoch)
	}
	if epoch == r.epoch && r.leaderID != 0 && r.leaderID != leaderID {
		currentLeader := r.leaderID
		r.mu.Unlock()
		return fmt.Errorf("leader mismatch %d != %d", leaderID, currentLeader)
	}
	r.epoch = epoch
	r.leaderID = leaderID
	r.lastHeartbeat = time.Now()
	r.role = badger.ReplicationRoleReplica
	subs, leader, changed := r.captureLeaderChange()
	r.mu.Unlock()
	if changed {
		publishLeaderChange(subs, leader)
	}
	return nil
}

func (r *Replication) startGRPC() error {
	ln, err := net.Listen("tcp", r.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %s: %w", r.cfg.GRPCAddr, err)
	}
	tlsCfg, err := r.cfg.TLS()
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("tls config: %w", err)
	}
	opts := []grpc.ServerOption{}
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg.Clone())))
	}
	r.grpcServer = grpc.NewServer(opts...)
	replicationpb.RegisterClusterControlServer(r.grpcServer, r)
	for _, register := range r.cfg.ExtraServices {
		if register != nil {
			register(r.grpcServer)
		}
	}
	go func() {
		if err := r.grpcServer.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("grpc serve: %v", err)
		}
	}()
	return nil
}

func (r *Replication) close() {
	r.closed.Do(func() {
		r.cancel()
		if r.grpcServer != nil {
			r.grpcServer.Stop()
		}
		r.applyMu.Lock()
		r.applyMu.Unlock()
		r.recoveryRunMu.Lock()
		r.recoveryRunMu.Unlock()
		if r.ml != nil {
			_ = r.ml.Leave(500 * time.Millisecond)
			r.ml.Shutdown()
		}
		r.clientMu.Lock()
		for _, c := range r.clients {
			r.closePeerClient(c)
		}
		r.clientMu.Unlock()
		r.closeLeaderSubscribers()
	})
}

func (r *Replication) Close() {
	r.close()
}
