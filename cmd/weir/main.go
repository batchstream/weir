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

	"github.com/batchstream/weir/internal/app"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/searchstore"
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
	listen := flag.String("listen", "127.0.0.1:7447", "loopback gRPC address; no TLS/auth in local milestones")
	uri := flag.String("mongo-uri", "mongodb://127.0.0.1:27028/?directConnection=true", "isolated MongoDB replica-set URI")
	db := flag.String("database", "weir_m1", "pre-created database")
	collection := flag.String("collection", "records", "pre-created collection")
	batch := flag.Bool("batch", true, "micro-batch compatible mutations")
	memory := flag.Uint64("memory-mib", 512, "overload budget; qualification starting point")
	searchURL := flag.String("search-url", "", "optional qualified loopback search backend")
	searchIndex := flag.String("search-index", "records", "pre-created concrete index")
	searchProfile := flag.String("search-profile", "elasticsearch-8.17.0", "exact qualified search profile")
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
	assembly := app.Config{Mongo: cfg, Limits: limits}
	if *searchURL != "" {
		assembly.Search = &searchstore.Config{Store: "search", URL: *searchURL, Index: *searchIndex, Profile: *searchProfile}
	}
	routes, err := app.OpenStores(startup, assembly)
	if err != nil {
		return err
	}
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, runtime := range routes {
			_ = runtime.Close(ctx)
		}
	}
	sl := server.DefaultLimits()
	srv, err := server.New(routes, sl)
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
	go store.Guard(guard, routes, *memory<<20)
	exited := make(chan error, 1)
	go func() { exited <- srv.Serve(listener) }()
	fmt.Printf("Weir listening on %s; qualified local record Stores, not production ready\n", listener.Addr())
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
