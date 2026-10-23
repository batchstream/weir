//go:build integration

package app

import (
	"context"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Separate commit and abort transactions are concurrently held across the one
// existing transition, alongside its original mutation. No extra CLI or fixture.
type replicaCLILuaTopology struct {
	lifetime   *replyLossLifetime
	transition string
	calls      [2]replicaCLILuaTopologyCall
}
type replicaCLILuaTopologyCall struct {
	ctx    context.Context
	cancel context.CancelFunc
	watch  *testmongo.LuaWatch
	done   chan replicaCLILuaReceipt
	joined chan struct{}
}

func replicaCLILuaTopologyBegin(t *testing.T, lifetime *replyLossLifetime, transition string) *replicaCLILuaTopology {
	t.Helper()
	s := &replicaCLILuaTopology{lifetime: lifetime, transition: transition}
	for i := range s.calls {
		c := &s.calls[i]
		id := "cli-lua-topology-" + transition + "-commit"
		if i == 1 {
			id = "cli-lua-topology-" + transition + "-abort"
		}
		op := testmongo.LuaOperation{Namespace: lifetime.fixture.DB + ".records", ID: id}
		var err error
		if i == 0 {
			c.watch, err = lifetime.proxy.WatchLuaCommit(op, false, backend.DefaultOptions().RetryAttempts)
		} else {
			c.watch, err = lifetime.proxy.WatchLua(op)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := lifetime.proxy.ArmLuaConflict(lifetime.primary, c.watch); err != nil {
			t.Fatal(err)
		}
		c.ctx, c.cancel = context.WithTimeout(t.Context(), 12*time.Second)
		c.done = make(chan replicaCLILuaReceipt, 1)
		c.joined = make(chan struct{})
		t.Cleanup(func() {
			c.cancel()
			select {
			case <-c.joined:
			case <-time.After(8 * time.Second):
				t.Error("topology SDK cleanup join")
			}
		})
		lifetime.proxy.SeedLua(t, c.ctx, c.watch)
		input := bson.D{{Key: "attempt", Value: int32(0)}}
		raw, err := bson.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		document := &pb.Document{ContentType: "application/bson", Data: raw}
		lua := &weirclient.LuaTransform{Source: []byte(testmongo.ReplicaLuaSource), Input: document}
		request := &weirclient.AtomicTransformRequest{Resource: lifetime.fixture.DB + "/records/s:" + id, Lua: lua}
		opts := weirclient.AtomicTransformOptions{StoreName: "mongo", Request: request}
		go func() {
			defer close(c.joined)
			result, err := lifetime.client.AtomicTransform(c.ctx, opts)
			receipt := replicaCLILuaReceipt{result: result, err: err}
			c.done <- receipt
		}()
		if err := testmongo.WaitLuaPhase(c.ctx, c.watch.SnapshotHeld(0)); err != nil {
			t.Fatal("CLI topology actual snapshot", err, c.watch.Snapshot())
		}
		if i == 1 {
			lifetime.proxy.ExternalLuaUpdate(t, c.ctx, c.watch)
		}
		if err := c.watch.ForwardSnapshot(0); err != nil {
			t.Fatal(err)
		}
		held := c.watch.CommitHeld
		if i == 1 {
			held = c.watch.AbortHeld
		}
		if err := testmongo.WaitLuaPhase(c.ctx, held()); err != nil {
			t.Fatal("CLI topology actual terminal reply", err, c.watch.Snapshot())
		}
		if i == 0 {
			if err := c.watch.ValidateCommitHeld(); err != nil {
				t.Fatal(err, c.watch.Snapshot())
			}
			expected := testmongo.LuaLedger{N: 1, Candidate: 1}
			lifetime.proxy.CheckLuaLedger(t, c.ctx, c.watch, expected)
		} else {
			if err := c.watch.ValidateAbortHeld(); err != nil {
				t.Fatal(err, c.watch.Snapshot())
			}
			expected := testmongo.LuaLedger{N: 20, External: 1}
			lifetime.proxy.CheckLuaLedger(t, c.ctx, c.watch, expected)
		}
	}
	s.held(t)
	return s
}
func (s *replicaCLILuaTopology) held(t *testing.T) {
	t.Helper()
	for i := range s.calls {
		c := &s.calls[i]
		if c.ctx.Err() != nil {
			t.Fatal("CLI topology original caller", c.ctx.Err())
		}
		select {
		case receipt := <-c.done:
			t.Fatal("CLI returned before topology/drop", receipt)
		default:
		}
	}
	replyLossGauges(t, s.lifetime, "Lua_topology_two_actual_replies_held", true)
}
func (s *replicaCLILuaTopology) drop(t *testing.T) {
	t.Helper()
	s.held(t)
	if err := s.calls[0].watch.DropFirstCommitReply(); err != nil {
		t.Fatal(err)
	}
	if err := s.calls[1].watch.DropAbortReply(); err != nil {
		t.Fatal(err)
	}
}

func (s *replicaCLILuaTopology) finish(t *testing.T) {
	t.Helper()
	for i := range s.calls {
		c := &s.calls[i]
		var receipt replicaCLILuaReceipt
		select {
		case receipt = <-c.done:
		case <-c.ctx.Done():
			t.Fatal("CLI topology original receipt", c.ctx.Err(), c.watch.Snapshot())
		}
		result := receipt.result
		if receipt.err != nil || result == nil || c.ctx.Err() != nil {
			t.Fatal("CLI topology live original caller", receipt.err, c.ctx.Err())
		}
		if i == 1 {
			if result.Outcome != weirclient.MutationNotApplied || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || !strings.Contains(result.GetFailure().GetMessage(), "rollback unconfirmed") {
				t.Fatal("CLI topology abort no rebuild", result)
			}
		} else {
			if result.Outcome != weirclient.MutationApplied || result.Failure != nil {
				t.Fatal("CLI topology original commit confirmed", result, c.watch.Snapshot())
			}
		}
		if err := testmongo.WaitLuaPhase(c.ctx, c.joined); err != nil {
			t.Fatal("CLI topology actual SDK join", err)
		}
		c.cancel()
	}
	replicaCLIIdle(t, s.lifetime.service, time.Now().Add(3*time.Second))
	if err := s.calls[0].watch.CompleteCommit(); err != nil {
		t.Fatal("CLI topology commit actual drop/confirmation accounting", err, s.calls[0].watch.Snapshot())
	}
	if err := s.calls[1].watch.Complete(false); err != nil {
		t.Fatal("CLI topology abort actual drop accounting", err, s.calls[1].watch.Snapshot())
	}
	abort := testmongo.LuaCase{Watch: s.calls[1].watch}
	commit := testmongo.LuaCommitCase{Watch: s.calls[0].watch}
	s.lifetime.topologyAborts = append(s.lifetime.topologyAborts, abort)
	s.lifetime.topologyCommits = append(s.lifetime.topologyCommits, commit)
	t.Logf("CLI Lua topology=%s same_PID=%d", s.transition, s.lifetime.pid)
}
