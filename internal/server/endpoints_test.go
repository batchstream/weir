package server

import (
	"bytes"
	"context"
	"fmt"
	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"google.golang.org/grpc/connectivity"
)

func TestEndpointCanonicalBounds(t *testing.T) {
	input := []string{"Peer.EXAMPLE.:07448", "[0:0::1]:7448", "[::ffff:127.0.0.1]:7448"}
	want := []string{"127.0.0.1:7448", "[::1]:7448", "peer.example:7448"}
	got, err := CanonicalEndpoints(input)
	if err != nil || !slices.Equal(got, want) {
		t.Fatal(got, err)
	}
	for _, bad := range [][]string{
		nil, {}, {""}, {"peer:0"}, {"peer:-1"}, {"peer:+1"}, {"peer:65536"}, {"peer:http"}, {"peer:"}, {"peer"},
		{"dns:///peer:1"}, {"https://peer:1"}, {"user@peer:1"}, {"peer:1/path"}, {"peer:1?x"}, {"peer:1#x"},
		{"peer :1"}, {"peer..name:1"}, {"-peer:1"}, {"peer-:1"}, {"_srv:1"}, {"中文:1"}, {"[peer]:1"},
		{"[fe80::1%lo0]:1"}, {"127.00.0.1:1"}, {"999.1.1.1:1"}, {strings.Repeat("a", 64) + ".test:1"},
		{"peer:1", "PEER.:01"}, {"127.0.0.1:1", "[::ffff:127.0.0.1]:1"}, {"[::1]:1", "[0:0::1]:01"},
		{"a:1", "b:1", "c:1", "d:1", "e:1", "f:1", "g:1", "h:1", "i:1"},
	} {
		if _, err := CanonicalEndpoints(bad); err == nil {
			t.Fatal("accepted invalid endpoints", bad)
		}
	}
	eight := []string{"a:1", "b:1", "c:1", "d:1", "e:1", "f:1", "g:1", "h:1"}
	if _, err := CanonicalEndpoints(eight); err != nil {
		t.Fatal(err)
	}
}

func TestEndpointRendezvousVectors(t *testing.T) {
	vectors := [][3]string{
		{"weir://mongo/db/records/s:one", "peer-a.example:7448", "4b4c225ce6d1fd8476d8aabf848f06139a9eb52986a2cefa4c46db16278ac0e3"},
		{"weir://mongo/db/records/s:one", "peer-b.example:7448", "653070b21895f9a64cd89b76dc36567dd7e6ccd17f6c51a2ab3aad539d08e50c"},
		{"bulk-0001", "[::1]:7448", "e5a2e602ee9a673893540e0e3f5fc60c9181def6493bd5c2f7495ef24a371b6a"},
	}
	for _, v := range vectors {
		if got := fmt.Sprintf("%x", affinityScore(v[0], v[1])); got != v[2] {
			t.Fatal(got, v)
		}
	}
	if affinityScore("ab", "c") == affinityScore("a", "bc") {
		t.Fatal("ambiguous framing")
	}
}

func awaitEndpoint(t *testing.T, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for !predicate() {
		if time.Now().After(until) {
			t.Fatal("endpoint condition timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func readyRemote(t *testing.T, r *RemoteWeir) {
	t.Helper()
	awaitEndpoint(t, func() bool {
		for _, e := range r.endpoints {
			if e.conn.GetState() != connectivity.Ready {
				return false
			}
		}
		return true
	})
}

func multipleRemote(t *testing.T, addresses []string) *RemoteWeir {
	t.Helper()
	cfg := RemoteConfig{Endpoints: addresses, Relays: 2}
	r, err := NewRemote(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	readyRemote(t, r)
	return r
}

func TestEndpointCloseColdAndMetrics(t *testing.T) {
	// Own and close the ephemeral port before using it as an unavailable peer.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	_ = l.Close()
	cfg := RemoteConfig{Endpoints: []string{address}, Relays: 1}
	r, err := NewRemote(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := r.selectClient(ctx, "key"); err == nil || time.Since(started) > 3*time.Second {
		t.Fatal("unbounded cold call", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { testmetrics.Gather(t, r); _ = r.Close() })
	}
	wg.Wait()
	if len(r.sockets) != 0 || len(r.resolutions) != 0 {
		t.Fatal("close leaked resources")
	}
	families := testmetrics.Gather(t, r)
	if testmetrics.Series(families) != 16 || testmetrics.Sum(families, "weir_remote_connectivity") != 1 {
		t.Fatal("metric series/state count")
	}
}

func TestEndpointMaximumGraphConnectionsAndCleanup(t *testing.T) {
	var addresses []string
	var servers []*Server
	for range 8 {
		_, rt := peerLocal(t, "records")
		local := Service{LocalStore: rt}
		limits := DefaultLimits()
		limits.Connections = 64
		opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true, limits: limits}
		srv, address := startPeerServer(t, opts)
		addresses = append(addresses, address)
		servers = append(servers, srv)
	}
	baseline := runtime.NumGoroutine()
	var remotes []*RemoteWeir
	for range 16 {
		cfg := RemoteConfig{Endpoints: addresses, Relays: 16}
		r, err := NewRemote(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		remotes = append(remotes, r)
	}
	var sockets, series int
	for _, r := range remotes {
		readyRemote(t, r)
		sockets += len(r.sockets)
		families := testmetrics.Gather(t, r)
		series += testmetrics.Series(families)
		if cap(r.sockets) != 16 || cap(r.slots) != 16 || testmetrics.Sum(families, "weir_remote_connectivity") != 8 {
			t.Fatal("maximum graph accounting")
		}
	}
	peak := runtime.NumGoroutine()
	if sockets != 128 || series != 16*16 {
		t.Fatal("unexpected channel/socket or series count", sockets, series)
	}
	var group sync.WaitGroup
	for _, r := range remotes {
		group.Go(func() { _ = r.Close() })
	}
	group.Wait()
	awaitEndpoint(t, func() bool {
		for _, srv := range servers {
			if len(srv.admission.connections) != 0 {
				return false
			}
		}
		return runtime.NumGoroutine() <= baseline+12
	})
	for _, r := range remotes {
		if len(r.sockets) != 0 {
			t.Fatal("socket leak")
		}
	}
	t.Logf("16 Services x 8 endpoints: channels=128 sockets=%d limit=256 remote series=%d; goroutines baseline=%d peak=%d after=%d (including 8 fixture servers)", sockets, series, baseline, peak, runtime.NumGoroutine())
}

func TestEndpointRoutePinnedAndNeverReplayedAfterDisconnect(t *testing.T) {
	var addresses []string
	var adapters []*peerAdapter
	var peers []*Server
	for range 3 {
		adapter, rt := peerLocal(t, "records")
		local := Service{LocalStore: rt}
		options := peerServerOptions{routes: map[string]Service{"records": local}, peer: true}
		peer, address := startPeerServer(t, options)
		addresses = append(addresses, address)
		adapters = append(adapters, adapter)
		peers = append(peers, peer)
	}
	remote := multipleRemote(t, addresses)
	service := Service{RemoteWeir: remote}
	options := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
	entry, address := startPeerServer(t, options)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("weir-request-id", "route-pinned"))
	selected, err := remote.selectClient(ctx, "route-pinned")
	if err != nil {
		t.Fatal(err)
	}
	selectedIndex := -1
	for _, endpoint := range remote.endpoints {
		if endpoint.client == selected {
			for i, address := range addresses {
				if endpoint.identity == address {
					selectedIndex = i
				}
			}
		}
	}
	if selectedIndex < 0 {
		t.Fatal("selected endpoint missing")
	}
	stream, err := client.Route(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 24; id++ {
		request := testMutation("body")
		request.Resource = fmt.Sprintf("data%d/s:key", id)
		value := &pb.Call_Mutate{Mutate: request}
		call := &pb.Call{Version: 1, Operation: value}
		result, err := roundTripRoute(stream, id, call)
		if err != nil || result.GetMutation().Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal(result, err)
		}
	}
	for i, adapter := range adapters {
		want := int32(0)
		if i == selectedIndex {
			want = 24
		}
		if adapter.commands.Load() != want {
			t.Fatal("stream moved downstream instance", i, adapter.commands.Load(), want)
		}
	}
	peers[selectedIndex].connections.Range(func(_, value any) bool { _ = value.(*limitedConn).Close(); return true })
	mutation := testMutation("after-disconnect")
	value := &pb.Call_Mutate{Mutate: mutation}
	call := &pb.Call{Version: 1, Operation: value}
	_, err = roundTripRoute(stream, 25, call)
	if err == nil {
		t.Fatal("disconnected Route migrated or replayed")
	}
	cancel()
	waitPeerIdle(t, entry)
	for i, adapter := range adapters {
		if i != selectedIndex && adapter.commands.Load() != 0 {
			t.Fatal("lost write replayed to another endpoint", i)
		}
	}
}

func roundTripRoute(stream grpc.BidiStreamingClient[pb.Request, pb.Response], id uint64, call *pb.Call) (*pb.Result, error) {
	payload, err := proto.Marshal(call)
	if err != nil {
		return nil, err
	}
	request := &pb.Request{Id: id, Destination: "records", Payload: payload}
	if err := stream.Send(request); err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	for {
		response, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if response.Id != id {
			return nil, io.ErrUnexpectedEOF
		}
		if response.End {
			break
		}
		_, _ = encoded.Write(response.Payload)
	}
	event := &pb.Event{}
	if err := protodelim.UnmarshalFrom(&encoded, event); err != nil {
		return nil, err
	}
	return event.GetResult(), nil
}
