package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	listen := flag.String("listen", "127.0.0.1:7447", "loopback gRPC address; no TLS/auth in milestone 1")
	uri := flag.String("mongo-uri", "mongodb://127.0.0.1:27028/?directConnection=true", "isolated MongoDB replica-set URI")
	db := flag.String("database", "weir_m1", "pre-created database")
	collection := flag.String("collection", "records", "pre-created collection")
	batch := flag.Bool("batch", true, "micro-batch compatible mutations")
	memory := flag.Uint64("memory-mib", 512, "overload budget; qualification starting point")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("milestone listener must use an explicit loopback IP")
	}
	if *memory < 64 || *memory > 65536 {
		return fmt.Errorf("invalid memory budget")
	}
	cfg := mongostore.Config{URI: *uri, Store: "mongo", Database: *db, Collection: *collection}
	limits := store.DefaultLimits()
	if !*batch {
		limits.BatchOperations = 1
	}
	startup, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	runtime, err := store.Open(startup, cfg, limits)
	if err != nil {
		return fmt.Errorf("MongoDB startup qualification failed: %w", err)
	}
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Close(ctx)
	}
	sl := server.DefaultLimits()
	srv, err := server.New(runtime, "mongo", sl)
	if err != nil {
		cleanup()
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		cleanup()
		return err
	}
	guard, cancelGuard := context.WithCancel(context.Background())
	defer cancelGuard()
	go runtime.Guard(guard, *memory<<20)
	exited := make(chan error, 1)
	go func() { exited <- srv.Serve(listener) }()
	fmt.Printf("Weir milestone 1 listening on %s; BSON CRUD/Bulk only, not production qualified\n", listener.Addr())
	signals, cancelSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelSignal()
	select {
	case <-signals.Done():
	case err = <-exited:
	}
	drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeErr := srv.Shutdown(drain)
	if err != nil {
		return err
	}
	return closeErr
}
