//go:build integration

package server

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fixture struct {
	mongo   *testmongo.Fixture
	server  *Server
	runtime *store.Runtime
	client  pb.WeirClient
	conn    *grpc.ClientConn
	address string
}

// Split the owned backend fixture's standard URI into explicit driver credentials.
func mongoFixtureConfig(t *testing.T, cfg mongodb.Config) mongodb.Config {
	t.Helper()
	parsed, err := url.Parse(cfg.URI)
	if err != nil {
		t.Fatal("invalid owned MongoDB fixture URI")
	}
	if parsed.User != nil {
		cfg.Username = parsed.User.Username()
		cfg.Password, _ = parsed.User.Password()
		parsed.User = nil
	}
	cfg.URI = parsed.String()
	return cfg
}

func setup(t *testing.T, batch bool) fixture {
	sl := DefaultLimits()
	sl.Stall = 300 * time.Millisecond
	sl.BulkLifetime = 5 * time.Second
	return setupWithLimits(t, batch, sl)
}

func setupWithLimits(t *testing.T, batch bool, sl Limits) fixture {
	t.Helper()
	backend := testmongo.Open(t)
	cfg := mongodb.Config{URI: backend.URI, Store: "mongo"}
	cfg = mongoFixtureConfig(t, cfg)
	l := store.DefaultLimits()
	if !batch {
		l.BatchOperations = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg.Pool = uint64(l.Concurrency)
	a, err := mongodb.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.New(a, l)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]*store.Runtime{"mongo": r}
	s, err := newLocalServer(t, routes, sl)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithStaticStreamWindowSize(64<<10), grpc.WithStaticConnWindowSize(256<<10))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	f := fixture{mongo: backend, server: s, runtime: r, client: pb.NewWeirClient(conn), conn: conn, address: listener.Addr().String()}
	return f
}
func resource(f fixture, id string) string { return "weir://mongo/" + f.mongo.DB + "/records/s:" + id }
func mutation(f fixture, id string, n int32) *pb.MutateRequest {
	doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}}
	raw, _ := bson.Marshal(doc)
	d := &pb.Document{MediaType: "application/bson", Data: raw}
	v := &pb.MutateRequest_Put{Put: d}
	m := &pb.MutateRequest{Resource: resource(f, id), Action: v}
	return m
}
func openFrame() *pb.BulkRequestFrame {
	o := &pb.BulkOpen{Store: "weir://mongo"}
	v := &pb.BulkRequestFrame_Open{Open: o}
	f := &pb.BulkRequestFrame{Frame: v}
	return f
}
func opFrame(op *pb.BulkOperation) *pb.BulkRequestFrame {
	v := &pb.BulkRequestFrame_Operation{Operation: op}
	f := &pb.BulkRequestFrame{Frame: v}
	return f
}
