package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/batchstream/weir/internal/app"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	diagnostics := flag.String("diagnostics", "", "optional loopback diagnostic HTTP address; disabled by default")
	listen := flag.String("listen", "127.0.0.1:7447", "loopback development gRPC address")
	uri := flag.String("mongo-uri", "mongodb://127.0.0.1:27028/?directConnection=true", "isolated MongoDB replica-set URI")
	db := flag.String("database", "weir_m1", "pre-created database")
	collection := flag.String("collection", "records", "pre-created collection")
	batch := flag.Bool("batch", true, "micro-batch compatible mutations")
	memory := flag.Uint64("memory-mib", 512, "overload budget; qualification starting point")
	searchURL := flag.String("search-url", "", "optional qualified loopback search backend")
	searchIndex := flag.String("search-index", "records", "pre-created concrete index")
	searchProfile := flag.String("search-profile", "elasticsearch-8.17.0", "exact qualified search profile")
	configFile := flag.String("config", "", "strict static JSON configuration; exclusive with other flags")
	flag.Parse()
	cfg := app.DefaultConfig()
	if *configFile != "" {
		mixed := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name != "config" {
				mixed = true
			}
		})
		if mixed {
			return fmt.Errorf("-config cannot be combined with local flags")
		}
		file, err := os.Open(*configFile)
		if err != nil {
			return fmt.Errorf("configuration unavailable")
		}
		defer file.Close()
		cfg, err = app.Decode(file)
		if err != nil {
			return err
		}
	} else {
		cfg.Diagnostics = *diagnostics
		cfg.Application = *listen
		cfg.MemoryMiB = *memory
		mongo := &app.Mongo{URI: *uri, Database: *db, Collection: *collection}
		local := &app.Local{Mongo: mongo}
		if !*batch {
			local.BatchOperations = 1
		}
		service := app.Service{Name: "mongo-local", Local: local}
		route := app.Route{Store: "mongo", Service: service.Name}
		cfg.Services = append(cfg.Services, service)
		cfg.Routes = append(cfg.Routes, route)
		if *searchURL != "" {
			search := &app.Search{URL: *searchURL, Index: *searchIndex, Profile: *searchProfile}
			local := &app.Local{Search: search}
			if !*batch {
				local.BatchOperations = 1
			}
			service := app.Service{Name: "search-local", Local: local}
			route := app.Route{Store: "search", Service: service.Name}
			cfg.Services = append(cfg.Services, service)
			cfg.Routes = append(cfg.Routes, route)
		}
	}
	startup, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	node, err := app.Open(startup, cfg)
	if err != nil {
		return err
	}
	node.Start()
	fmt.Printf("Weir listening on %v; static local/peer profile, not production ready\n", node.Addresses())
	if node.DiagnosticAddress() != "" {
		fmt.Printf("Diagnostics listening on %s\n", node.DiagnosticAddress())
	}
	signals, cancelSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelSignal()
	select {
	case <-signals.Done():
	case err = <-node.Errors:
	}
	drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeErr := node.Close(drain)
	if err != nil {
		return err
	}
	return closeErr
}
