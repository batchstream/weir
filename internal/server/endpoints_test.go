package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testmetrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
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

func TestEndpointSelectionLocalityDistributionAndMembership(t *testing.T) {
	var addresses []string
	var adapters []*peerAdapter
	var servers []*Server
	for range 3 {
		a, runtime := peerLocal(t, "records")
		local := Service{LocalStore: runtime}
		opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true}
		srv, address := startPeerServer(t, opts)
		addresses = append(addresses, address)
		adapters = append(adapters, a)
		servers = append(servers, srv)
	}
	r := multipleRemote(t, addresses)
	reversed := slices.Clone(addresses)
	slices.Reverse(reversed)
	other := multipleRemote(t, reversed)
	service := Service{RemoteWeir: r}
	opts := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
	_, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pair := &RemoteWeir{endpoints: r.endpoints[:2]}
	var moved int
	for i := 0; i < 120; i++ {
		req := testMutation("\x00opaque\xff")
		req.Resource = fmt.Sprintf("weir://records/data/s:k%d", i)
		selected, err := r.selectClient(ctx, req.Resource)
		if err != nil {
			t.Fatal(err)
		}
		selected2, err := other.selectClient(ctx, req.Resource)
		if err != nil {
			t.Fatal(err)
		}
		for j, e := range r.endpoints {
			if selected == e.client && selected2 != other.endpoints[j].client {
				t.Fatal("config order changed affinity")
			}
		}
		before, err := pair.selectClient(ctx, req.Resource)
		if err != nil {
			t.Fatal(err)
		}
		if before != selected {
			moved++
			if selected != r.endpoints[2].client {
				t.Fatal("adding member remapped existing winners")
			}
		}
		result, err := client.Mutate(ctx, req)
		if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal(result, err)
		}
		read := &pb.ReadRequest{Resource: req.Resource}
		got, err := client.Read(ctx, read)
		if err != nil || !bytes.Equal(got.GetDocument().GetData(), req.GetPut().Data) {
			t.Fatal("Read/Mutate locality or opaque data", got, err)
		}
	}
	if moved < 15 || moved > 70 {
		t.Fatal("unexpected membership distribution", moved)
	}
	for _, a := range adapters {
		if a.commands.Load() < 15 {
			t.Fatal("not distributed across three endpoints", a.commands.Load())
		}
	}
	ctxStop, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if err := servers[0].Shutdown(ctxStop); err != nil {
		t.Fatal(err)
	}
	awaitEndpoint(t, func() bool {
		for _, e := range r.endpoints {
			if e.identity == addresses[0] {
				return e.conn.GetState() != connectivity.Ready
			}
		}
		return false
	})
	before := adapters[0].commands.Load()
	for i := 0; i < 20; i++ {
		req := testMutation("future")
		req.Resource = fmt.Sprintf("weir://records/data/s:future%d", i)
		result, err := client.Mutate(ctx, req)
		if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal(result, err)
		}
	}
	if adapters[0].commands.Load() != before {
		t.Fatal("new call selected unavailable peer")
	}
	t.Logf("three endpoints mutation counts=%d,%d,%d; membership moves=%d/120", adapters[0].commands.Load(), adapters[1].commands.Load(), adapters[2].commands.Load(), moved)
}

func TestEndpointBulkPinnedAndServiceCredits(t *testing.T) {
	var addresses []string
	var adapters []*peerAdapter
	for range 3 {
		a, rt := peerLocal(t, "records")
		adapters = append(adapters, a)
		local := Service{LocalStore: rt}
		opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true}
		_, address := startPeerServer(t, opts)
		addresses = append(addresses, address)
	}
	r := multipleRemote(t, addresses)
	service := Service{RemoteWeir: r}
	opts := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
	entry, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("weir-request-id", "bulk-pinned"))
	bulk, err := client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := &pb.BulkOpen{Store: "weir://records"}
	first := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: first}
	if err := bulk.Send(frame); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 24; i++ {
		mutation := testMutation("body")
		mutation.Resource = fmt.Sprintf("weir://records/data/s:bulk%d", i)
		variant := &pb.BulkOperation_Mutate{Mutate: mutation}
		op := &pb.BulkOperation{Index: uint64(i), Operation: variant}
		of := &pb.BulkRequestFrame_Operation{Operation: op}
		frame := &pb.BulkRequestFrame{Frame: of}
		if err := bulk.Send(frame); err != nil {
			t.Fatal(err)
		}
		result, err := bulk.Recv()
		if err != nil || result.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal(result, err)
		}
	}
	if err := bulk.CloseSend(); err != nil {
		t.Fatal(err)
	}
	end, err := bulk.Recv()
	if err != nil || end.GetEnd().GetReceivedCount() != 24 || end.GetEnd().GetResultCount() != 24 {
		t.Fatal(end, err)
	}
	if _, err := bulk.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	var recipients int
	for _, a := range adapters {
		if a.commands.Load() > 0 {
			recipients++
			if a.commands.Load() != 24 {
				t.Fatal("Bulk moved endpoint")
			}
		}
	}
	if recipients != 1 {
		t.Fatal("Bulk fanout", recipients)
	}
	waitPeerIdle(t, entry)
	// The third reservation fails even with three READY endpoints.
	var held []*delivery
	for range 2 {
		d := &delivery{}
		if err := r.enter(d); err != nil {
			t.Fatal(err)
		}
		held = append(held, d)
	}
	d := &delivery{}
	if err := r.enter(d); err == nil {
		t.Fatal("per-endpoint relay multiplication")
	}
	for range held {
		<-r.slots
	}
	if cap(r.slots) != 2 || cap(r.sockets) != 6 {
		t.Fatal("bounds")
	}
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
	if testmetrics.Series(families) != 34 || testmetrics.Sum(families, "weir_remote_connectivity") != 1 {
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
	if sockets != 128 || series != 16*34 {
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

func TestEndpointStreamsNeverMigrate(t *testing.T) {
	for _, method := range []string{"Bulk", "Native", "Scan"} {
		t.Run(method, func(t *testing.T) {
			var addresses []string
			var peers []*Server
			var adapters []*peerAdapter
			for range 2 {
				a, rt := peerLocal(t, "records")
				local := Service{LocalStore: rt}
				opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true}
				peer, address := startPeerServer(t, opts)
				peers = append(peers, peer)
				addresses = append(addresses, address)
				adapters = append(adapters, a)
			}
			r := multipleRemote(t, addresses)
			service := Service{RemoteWeir: r}
			opts := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
			entry, address := startPeerServer(t, opts)
			_, client := peerClient(t, address)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("weir-request-id", "pinned-stream"))
			key := "weir://records/data"
			if method == "Bulk" {
				key = "pinned-stream"
			}
			chosen, err := r.selectClient(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			var selected int
			for _, e := range r.endpoints {
				if e.client == chosen {
					for i, a := range addresses {
						if a == e.identity {
							selected = i
						}
					}
				}
			}
			var bulk grpc.BidiStreamingClient[pb.BulkRequestFrame, pb.BulkResponseFrame]
			var native grpc.BidiStreamingClient[pb.NativeRequestFrame, pb.NativeResponseFrame]
			var scan grpc.ServerStreamingClient[pb.ScanResponseFrame]
			switch method {
			case "Bulk":
				bulk, err = client.Bulk(ctx)
				if err != nil {
					t.Fatal(err)
				}
				open := &pb.BulkOpen{Store: "weir://records"}
				variant := &pb.BulkRequestFrame_Open{Open: open}
				frame := &pb.BulkRequestFrame{Frame: variant}
				if err := bulk.Send(frame); err != nil {
					t.Fatal(err)
				}
				mv := &pb.BulkOperation_Mutate{Mutate: testMutation("already-applied")}
				operation := &pb.BulkOperation{Operation: mv}
				ov := &pb.BulkRequestFrame_Operation{Operation: operation}
				frame = &pb.BulkRequestFrame{Frame: ov}
				if err := bulk.Send(frame); err != nil {
					t.Fatal(err)
				}
				result, err := bulk.Recv()
				if err != nil || result.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
					t.Fatal(result, err)
				}
			case "Native":
				native, err = client.Native(ctx)
				if err != nil {
					t.Fatal(err)
				}
				desc := &pb.Document{MediaType: "application/octet-stream"}
				open := &pb.NativeOpen{Resource: key, Descriptor_: desc}
				variant := &pb.NativeRequestFrame_Open{Open: open}
				frame := &pb.NativeRequestFrame{Frame: variant}
				if err := native.Send(frame); err != nil {
					t.Fatal(err)
				}
				head, err := native.Recv()
				if err != nil || head.GetHead() == nil {
					t.Fatal(head, err)
				}
			case "Scan":
				req := &pb.ScanRequest{Resource: key}
				scan, err = client.Scan(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				first, err := scan.Recv()
				if err != nil || first.GetDocument() == nil {
					t.Fatal(first, err)
				}
			}
			peers[selected].connections.Range(func(_, value any) bool { _ = value.(*limitedConn).Close(); return true })
			var terminal bool
			switch method {
			case "Bulk":
				_ = bulk.CloseSend()
				for {
					frame, recvErr := bulk.Recv()
					err = recvErr
					if err != nil {
						break
					}
					terminal = terminal || frame.GetEnd() != nil
				}
			case "Native":
				_ = native.CloseSend()
				for {
					frame, recvErr := native.Recv()
					err = recvErr
					if err != nil {
						break
					}
					terminal = terminal || frame.GetEnd().GetCompletion() == pb.NativeCompletion_RESPONSE_COMPLETE
				}
			case "Scan":
				for {
					frame, recvErr := scan.Recv()
					err = recvErr
					if err != nil {
						break
					}
					terminal = terminal || frame.GetEnd() != nil
				}
			}
			if err == io.EOF || terminal {
				t.Fatal("lost stream became complete", method, terminal, err)
			}
			waitPeerIdle(t, entry)
			other := adapters[1-selected]
			if other.commands.Load() != 0 || other.native.Load() != 0 || other.scans.Load() != 0 {
				t.Fatal("in-flight stream migrated to available endpoint")
			}
			if method == "Bulk" && adapters[selected].commands.Load() != 1 || method == "Native" && adapters[selected].native.Load() != 1 {
				t.Fatal("attempt restarted")
			}
		})
	}
}
