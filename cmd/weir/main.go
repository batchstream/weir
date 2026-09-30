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
	configFile := flags.String("config", "weir.json", "basic JSON configuration referencing a routing file")
	checkConfig := flags.String("check-config", "", "validate basic and routing configuration without opening listeners, resolving DNS or connecting")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	versionMode, checkMode, probeMode := false, false, false
	flags.Visit(func(f *flag.Flag) {
		versionMode = versionMode || f.Name == "version"
		checkMode = checkMode || f.Name == "check-config"
		probeMode = probeMode || f.Name == "probe" || f.Name == "probe-address"
	})
	if versionMode {
		if flags.NFlag() != 1 || !*version {
			return errors.New("-version requires true and cannot be combined with other flags")
		}
		return printVersion(output)
	}
	if checkMode {
		if flags.NFlag() != 1 || *checkConfig == "" {
			return errors.New("-check-config requires a file and cannot be combined with other flags")
		}
		if _, err := app.Load(*checkConfig); err != nil {
			return err
		}
		_, err := io.WriteString(output, "configuration valid\n")
		return err
	}
	if probeMode {
		mixed := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name != "probe" && f.Name != "probe-address" {
				mixed = true
			}
		})
		if mixed {
			return errors.New("-probe cannot be combined with configuration or version flags")
		}
		return runProbe(context.Background(), *probe, *probeAddress)
	}
	if *configFile == "" {
		return errors.New("-config requires a file")
	}
	cfg, err := app.Load(*configFile)
	if err != nil {
		return err
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
