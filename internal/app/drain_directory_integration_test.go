//go:build integration

package app

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestDirectoryWithdrawalDoesNotDelayAdmittedWriteDrain(t *testing.T) {
	fixture := testmongo.Open(t)
	proxy := testmongo.StartProxy(t, fixture)
	gate := make(chan struct{})
	observation := &budgetObservation{}
	observation.hold(gate, 0)
	monitor := &event.CommandMonitor{Started: observation.start, Succeeded: observation.finish}
	proxy.Monitor = monitor
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	blackhole, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.WaitGroup
	stopped := make(chan struct{})
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			connection, err := blackhole.Accept()
			if err != nil {
				return
			}
			connections.Go(func() {
				defer connection.Close()
				<-stopped
			})
		}
	}()
	t.Cleanup(func() {
		_ = blackhole.Close()
		close(stopped)
		<-accepted
		connections.Wait()
	})
	backend := mongoFixtureConfig(t, proxy.URI())
	local := &Local{Backend: backend, Batching: BatchingConfig{MaxOperations: 1}}
	definition := StoreConfig{Name: "records", Local: local}
	cfg := emptyConfig(t)
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	cfg.Basic.Discovery.Seeds = []string{blackhole.Addr().String()}
	cfg.Routing.Stores = []StoreConfig{definition}
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close(context.Background()) })
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := endpointProcessClient(t, node.Addresses()[0])
	request := budgetPut(fixture.DB+"/records", "drain")
	result := make(chan *pb.Event, 1)
	callErrors := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	go func() {
		response, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("records", request))
		result <- response
		callErrors <- err
	}()
	budgetWait(t, "write admitted before drain", func() bool {
		active, _, _, _ := observation.snapshot()
		return active == 1
	})
	closed := make(chan error, 1)
	go func() { closed <- node.Close(ctx) }()
	until := time.Now().Add(200 * time.Millisecond)
	for !node.stores["records"].Snapshot().Draining {
		if time.Now().After(until) {
			t.Fatal("directory withdrawal consumed the business drain budget")
		}
		time.Sleep(time.Millisecond)
	}
	release.Do(func() { close(gate) })
	select {
	case response := <-result:
		if err := <-callErrors; err != nil || response.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("admitted write did not finish during peer withdrawal", response, err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted write was delayed behind an unavailable directory peer")
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}
