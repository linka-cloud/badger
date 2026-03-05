package replication

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	replicationpb "go.linka.cloud/badger/replication/pb"
)

func TestRPCFencingAndStreamContracts(t *testing.T) {
	dir := t.TempDir()
	ports := reservePorts(t, 2)
	cfg := baseTestConfig(1, dir, ports[0], ports[1], nil)
	n := startTestNode(t, cfg)
	defer n.close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	cli := replicationpb.NewClusterControlClient(conn)

	if _, err := cli.Coordinator(ctx, &replicationpb.CoordinatorRequest{LeaderId: 9, Epoch: 5}); err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	hb, err := cli.Heartbeat(ctx, &replicationpb.HeartbeatRequest{LeaderId: 9, Epoch: 4})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb.GetAccepted() {
		t.Fatalf("stale heartbeat epoch should be rejected")
	}

	cur, err := cli.Cursor(ctx, &replicationpb.CursorRequest{LeaderId: 8, Epoch: 5})
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	if cur.GetAccepted() {
		t.Fatalf("cursor with wrong leader at same epoch must be rejected")
	}

	apply, err := cli.ApplyFrames(ctx)
	if err != nil {
		t.Fatalf("apply stream: %v", err)
	}
	if err := apply.Send(&replicationpb.ApplyFramesRequest{LeaderId: 8, Epoch: 5}); err != nil {
		t.Fatalf("apply send: %v", err)
	}
	applyResp, err := apply.Recv()
	if err != nil {
		t.Fatalf("apply recv: %v", err)
	}
	if applyResp.GetAccepted() {
		t.Fatalf("apply with wrong leader should be rejected")
	}
	_ = apply.CloseSend()

	snap, err := cli.InstallSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot stream: %v", err)
	}
	if err := snap.Send(&replicationpb.InstallSnapshotRequest{LeaderId: 8, Epoch: 5, Reset_: true}); err != nil {
		t.Fatalf("snapshot send: %v", err)
	}
	resp, err := snap.CloseAndRecv()
	if err != nil && err != io.EOF {
		t.Fatalf("snapshot close recv: %v", err)
	}
	if resp != nil && resp.GetAccepted() {
		t.Fatalf("snapshot with wrong leader should be rejected")
	}
}
