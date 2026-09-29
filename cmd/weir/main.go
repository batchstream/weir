package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/batchstream/weir/internal/app"
	"github.com/batchstream/weir/internal/backend/search"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, output io.Writer) (resultErr error) {
	flags := flag.NewFlagSet("weir", flag.ContinueOnError)
	flags.SetOutput(output)
	version := flags.Bool("version", false, "print build identity without loading configuration or connecting")
	probe := flags.String("probe", "", "check live or ready using only loopback diagnostics; exit 0 healthy, 1 otherwise")
	probeAddress := flags.String("probe-address", "127.0.0.1:7449", "probe-only loopback IP:port")
	diagnostics := flags.String("diagnostics", "", "optional loopback diagnostic HTTP address; disabled by default")
	listen := flags.String("listen", "127.0.0.1:7447", "intranet gRPC listen IP:port; loopback by default")
	uri := flags.String("mongo-uri", "mongodb://127.0.0.1:27028/?directConnection=true", "isolated MongoDB replica-set URI")
	db := flags.String("database", "weir_m1", "pre-created database")
	collection := flags.String("collection", "records", "pre-created collection")
	batch := flags.Bool("batch", true, "micro-batch compatible mutations")
	memory := flags.Uint64("memory-mib", 512, "overload budget; qualification starting point")
	searchURL := flags.String("search-url", "", "optional qualified loopback search backend")
	searchIndex := flags.String("search-index", "records", "pre-created concrete index")
	searchProfile := flags.String("search-profile", search.ElasticsearchProfile, "exact qualified search profile")
	luaWorker := flags.String("lua-worker", "", "optional Lua worker executable for ProgramTransform")
	configFile := flags.String("config", "", "strict static JSON configuration; exclusive with other flags")
	checkConfig := flags.String("check-config", "", "validate a configuration file without opening listeners, resolving DNS or connecting")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *version {
		if flags.NFlag() != 1 {
			return fmt.Errorf("-version cannot be combined with other flags")
		}
		return printVersion(output)
	}
	checkMode := false
	flags.Visit(func(f *flag.Flag) { checkMode = checkMode || f.Name == "check-config" })
	if checkMode {
		if flags.NFlag() != 1 || *checkConfig == "" {
			return errors.New("-check-config requires a file and cannot be combined with other flags")
		}
		file, err := os.Open(*checkConfig)
		if err != nil {
			return errors.New("configuration unavailable")
		}
		defer file.Close()
		if _, err := app.Decode(file); err != nil {
			return err
		}
		_, err = io.WriteString(output, "configuration valid\n")
		return err
	}
	probeMode := false
	flags.Visit(func(f *flag.Flag) {
		probeMode = probeMode || f.Name == "probe" || f.Name == "probe-address"
	})
	if probeMode {
		mixed := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name != "probe" && f.Name != "probe-address" {
				mixed = true
			}
		})
		if mixed {
			return fmt.Errorf("-probe cannot be combined with server or version flags")
		}
		return runProbe(context.Background(), *probe, *probeAddress)
	}
	cfg := app.DefaultConfig()
	if *configFile != "" {
		mixed := false
		flags.Visit(func(f *flag.Flag) {
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
		cfg.LuaWorker = *luaWorker
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
	// The CLI owns signals from before assembly until all owned resources close.
	signals, cancelSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelSignal()
	startup, stop := context.WithTimeout(signals, 5*time.Second)
	defer stop()
	node, err := app.Open(startup, cfg)
	if err != nil {
		return err
	}
	defer func() {
		// A startup cancellation must not cancel the drain budget too.
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, node.Close(drain))
		// Close joins the listeners. Preserve their errors even if a signal won
		// the select below; normal shutdown contributes only nil errors.
		for {
			select {
			case err := <-node.Errors:
				resultErr = errors.Join(resultErr, err)
			default:
				return
			}
		}
	}()
	if err := node.Start(startup); err != nil {
		return err
	}
	stop()
	_, err = fmt.Fprintf(output, "Weir listening on %v; static local/peer profile, not production ready\n", node.Addresses())
	if err != nil {
		return errors.New("listener output failed")
	}
	if node.DiagnosticAddress() != "" {
		if _, err := fmt.Fprintf(output, "Diagnostics listening on %s\n", node.DiagnosticAddress()); err != nil {
			return errors.New("listener output failed")
		}
	}
	select {
	case <-signals.Done():
		return nil
	case err := <-node.Errors:
		return err
	}
}
