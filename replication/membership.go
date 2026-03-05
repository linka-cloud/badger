package replication

import (
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/badger/v4"

	"go.linka.cloud/badger/replication/pb"
)

func (r *Replication) startMemberlist() error {
	bindHost, bindPort, err := splitHostPort(r.cfg.MemberlistBindAddr)
	if err != nil {
		return err
	}
	c := memberlist.DefaultLANConfig()
	c.Name = fmt.Sprintf("node-%d", r.cfg.NodeID)
	c.BindAddr = bindHost
	c.BindPort = bindPort
	if r.cfg.EncryptionKey != "" {
		c.SecretKey = memberlistSecretKey(r.cfg.EncryptionKey)
	}
	if r.cfg.MemberlistAdvertise != "" {
		aHost, aPort, err := splitHostPort(r.cfg.MemberlistAdvertise)
		if err != nil {
			return err
		}
		c.AdvertiseAddr = aHost
		c.AdvertisePort = aPort
	}
	c.Delegate = &membershipDelegate{rt: r}
	c.Events = &membershipEvents{rt: r}
	ml, err := memberlist.Create(c)
	if err != nil {
		return err
	}
	r.ml = ml
	r.registerLocalMember()
	join := r.memberlistJoinTargets()
	if len(join) > 0 {
		n, err := ml.Join(join)
		if err != nil {
			return err
		}
		log.Printf("joined %d members via %v", n, join)
	}
	return nil
}

func (r *Replication) memberlistJoinTargets() []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(r.cfg.Join)+len(r.knownMembers))
	for _, addr := range r.cfg.Join {
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}

	r.mu.RLock()
	for id, m := range r.knownMembers {
		if id == r.cfg.NodeID || m.GossipAddr == "" {
			continue
		}
		addr := m.GossipAddr
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	r.mu.RUnlock()
	return out
}

func (r *Replication) registerLocalMember() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.peers[r.cfg.NodeID] = peerInfo{ID: r.cfg.NodeID, GRPCAddr: r.cfg.GRPCAddr, Epoch: r.epoch, Alive: true, LastSeen: now}
	r.knownMembers[r.cfg.NodeID] = persistedMember{ID: r.cfg.NodeID, GRPCAddr: r.cfg.GRPCAddr, GossipAddr: r.cfg.MemberlistBindAddr, LastSeenUnix: now.Unix(), LastEpochSeen: r.epoch}
	st := r.snapshotState()
	go func() {
		if err := r.saveState(st); err != nil {
			log.Printf("persist state: %v", err)
		}
	}()
}

func (r *Replication) upsertMember(id uint64, grpcAddr string, gossipAddr string, epoch uint64, alive bool) {
	r.mu.Lock()
	now := time.Now()
	p := r.peers[id]
	p.ID = id
	if grpcAddr != "" {
		p.GRPCAddr = grpcAddr
	}
	p.Epoch = epoch
	p.Alive = alive
	p.LastSeen = now
	r.peers[id] = p
	k := r.knownMembers[id]
	k.ID = id
	k.GRPCAddr = p.GRPCAddr
	if gossipAddr != "" {
		k.GossipAddr = gossipAddr
	}
	k.LastSeenUnix = now.Unix()
	if epoch > k.LastEpochSeen {
		k.LastEpochSeen = epoch
	}
	r.knownMembers[id] = k
	subs, leader, changed := r.captureLeaderChange()
	st := r.snapshotState()
	role := r.role
	peer := p
	r.mu.Unlock()

	go func() {
		if err := r.saveState(st); err != nil {
			log.Printf("persist state: %v", err)
		}
	}()
	if changed {
		publishLeaderChange(subs, leader)
	}
	if alive && id != r.cfg.NodeID && role == badger.ReplicationRolePrimary {
		go func() {
			if !r.beginPeerRecovery(peer.ID) {
				return
			}
			defer r.endPeerRecovery(peer.ID)
			epoch := r.currentEpoch()
			log.Printf("membership peer upsert: leader=%d peer=%d triggering catch-up", r.cfg.NodeID, peer.ID)
			if err := r.syncPeerWithRetry(epoch, peer); err != nil {
				log.Printf("membership catch-up failed: leader=%d peer=%d err=%v", r.cfg.NodeID, peer.ID, err)
			}
		}()
	}
}

func (r *Replication) markMemberDead(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.peers[id]
	p.Alive = false
	r.peers[id] = p
}

type membershipDelegate struct {
	rt *Replication
}

func (d *membershipDelegate) NodeMeta(limit int) []byte {
	d.rt.mu.RLock()
	meta := &replicationpb.MemberMeta{NodeId: d.rt.cfg.NodeID, GrpcAddr: d.rt.cfg.GRPCAddr, Epoch: d.rt.epoch, LeaderId: d.rt.leaderID}
	d.rt.mu.RUnlock()
	b, _ := proto.Marshal(meta)
	if len(b) > limit {
		return nil
	}
	return b
}

func (d *membershipDelegate) NotifyMsg(_ []byte) {}

func (d *membershipDelegate) GetBroadcasts(_, _ int) [][]byte { return nil }

func (d *membershipDelegate) LocalState(_ bool) []byte { return nil }

func (d *membershipDelegate) MergeRemoteState(_ []byte, _ bool) {}

type membershipEvents struct {
	rt *Replication
}

func (e *membershipEvents) NotifyJoin(n *memberlist.Node) {
	id, meta := parseMemberNode(n)
	if id == 0 {
		return
	}
	e.rt.upsertMember(id, meta.GetGrpcAddr(), memberlistAddr(n), meta.GetEpoch(), true)
}

func (e *membershipEvents) NotifyLeave(n *memberlist.Node) {
	id, _ := parseMemberNode(n)
	if id == 0 {
		return
	}
	e.rt.markMemberDead(id)
}

func (e *membershipEvents) NotifyUpdate(n *memberlist.Node) {
	id, meta := parseMemberNode(n)
	if id == 0 {
		return
	}
	e.rt.upsertMember(id, meta.GetGrpcAddr(), memberlistAddr(n), meta.GetEpoch(), true)
}

func memberlistAddr(n *memberlist.Node) string {
	if n == nil {
		return ""
	}
	return net.JoinHostPort(n.Addr.String(), strconv.Itoa(int(n.Port)))
}

func memberlistSecretKey(key string) []byte {
	raw := []byte(key)
	switch len(raw) {
	case 16, 24, 32:
		out := make([]byte, len(raw))
		copy(out, raw)
		return out
	default:
		sum := sha256.Sum256(raw)
		out := make([]byte, len(sum))
		copy(out, sum[:])
		return out
	}
}

func parseMemberNode(n *memberlist.Node) (uint64, *replicationpb.MemberMeta) {
	meta := &replicationpb.MemberMeta{}
	if n == nil {
		return 0, meta
	}
	_ = proto.Unmarshal(n.Meta, meta)
	if meta.GetNodeId() == 0 {
		parts := strings.Split(n.Name, "-")
		if len(parts) > 1 {
			if id, err := strconv.ParseUint(parts[len(parts)-1], 10, 64); err == nil {
				meta.NodeId = id
			}
		}
	}
	if meta.GetGrpcAddr() == "" {
		meta.GrpcAddr = net.JoinHostPort(n.Addr.String(), "19000")
	}
	return meta.GetNodeId(), meta
}
