//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type scanFixture struct {
	fixture
	metricsRemotes []*RemoteWeir
	replicas       []*store.Runtime
	backend        *testsearch.Backend
	root           string
}

func scanServer(t *testing.T, kind string, sl Limits) scanFixture {
	t.Helper()
	f := scanFixture{}
	var adapter execution.Adapter
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if kind == "mongo" {
		f.mongo = testmongo.Open(t)
		cfg := mongodb.Config{URI: f.mongo.URI, Store: "mongo", Database: f.mongo.DB, Collection: "records", Pool: 1}
		adapter, err = mongodb.Open(ctx, cfg)
		f.root = "weir://mongo/" + f.mongo.DB + "/records"
	} else {
		f.backend = testsearch.Open(t)
		cfg := search.Config{Store: "search", URL: f.backend.URL, Index: f.backend.Index, Profile: f.backend.Profile, Pool: 1}
		adapter, err = search.Open(ctx, cfg)
		f.root = "weir://search/" + f.backend.Index
	}
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()
	limits.Concurrency = 1
	limits.Collect = 0
	f.runtime, err = store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]*store.Runtime{kind: f.runtime}
	f.server, err = newLocalServer(t, routes, sl)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.address = listener.Addr().String()
	go func() { _ = f.server.Serve(listener) }()
	f.conn, err = grpc.NewClient(f.address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535))
	if err != nil {
		t.Fatal(err)
	}
	f.client = pb.NewWeirClient(f.conn)
	t.Cleanup(func() {
		_ = f.conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := f.server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return f
}
func (f scanFixture) seed(t *testing.T, sizes []int) {
	t.Helper()
	for i, size := range sizes {
		pad := strings.Repeat("x", size)
		if f.backend == nil {
			doc := bson.D{{Key: "_id", Value: fmt.Sprint(i)}, {Key: "n", Value: int64(9223372036854775807)}, {Key: "pad", Value: pad}}
			if _, err := f.mongo.Admin.Database(f.mongo.DB).Collection("records").InsertOne(context.Background(), doc); err != nil {
				t.Fatal(err)
			}
		} else {
			body := map[string]any{"n": int64(9223372036854775807), "pad": pad}
			raw, _ := json.Marshal(body)
			status, _ := f.backend.Do(t, "PUT", fmt.Sprintf("/%s/_doc/%d", f.backend.Index, i), string(raw))
			if status != 201 {
				t.Fatal(status)
			}
		}
	}
	if f.backend != nil {
		f.backend.Do(t, "POST", "/"+f.backend.Index+"/_refresh", "")
	}
}
func receiveScan(t *testing.T, stream grpc.ServerStreamingClient[pb.ScanResponseFrame]) (uint64, *pb.ScanEnd, error) {
	t.Helper()
	var count uint64
	for {
		frame, err := stream.Recv()
		if err != nil {
			return count, nil, err
		}
		if end := frame.GetEnd(); end != nil {
			if end.DocumentCount != count {
				t.Fatal("count mismatch", count, end)
			}
			_, err = stream.Recv()
			return count, end, err
		}
		if frame.GetDocument() == nil {
			t.Fatal("invalid Scan frame")
		}
		count++
	}
}
func waitScanReleased(t *testing.T, f scanFixture) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		snap := f.runtime.Snapshot()
		if snap.Active == 0 && snap.Retained == 0 && snap.Pending == 0 && snap.PendingBytes == 0 && snap.ResultBytes == 0 && snap.ScanSessions == 0 && len(f.server.slots) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Scan leaked", f.runtime.Snapshot(), len(f.server.slots))
}
