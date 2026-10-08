package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/batchstream/weir/internal/app"
	"github.com/spf13/cobra"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	completion := cobra.CompletionOptions{DisableDefaultCmd: true}
	root := &cobra.Command{
		Use:               "weir",
		Short:             "Run and inspect a Weir node",
		Args:              cobra.NoArgs,
		SilenceUsage:      true,
		SilenceErrors:     true,
		CompletionOptions: completion,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}

	serveConfig := "weir.yaml"
	serveRoutes := ""
	serveCommand := &cobra.Command{
		Use:   "serve",
		Short: "Start the node using process configuration and optional local Stores",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if serveConfig == "" {
				return errors.New("--config requires a file")
			}

			cfg, err := app.Load(serveConfig, serveRoutes)
			if err != nil {
				return err
			}

			return serve(command.Context(), cfg, command.OutOrStdout())
		},
	}
	serveCommand.Flags().StringVarP(
		&serveConfig,
		"config",
		"c",
		"weir.yaml",
		"basic YAML configuration file",
	)
	serveCommand.Flags().StringVar(
		&serveRoutes,
		"routes",
		"",
		"optional local Store YAML configuration file",
	)

	checkConfig := "weir.yaml"
	checkRoutes := ""
	checkCommand := &cobra.Command{
		Use:   "check",
		Short: "Validate configuration without listeners, DNS or backend connections",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if checkConfig == "" {
				return errors.New("--config requires a file")
			}

			if _, err := app.Load(checkConfig, checkRoutes); err != nil {
				return err
			}

			_, err := io.WriteString(command.OutOrStdout(), "configuration valid\n")
			return err
		},
	}
	checkCommand.Flags().StringVarP(
		&checkConfig,
		"config",
		"c",
		"weir.yaml",
		"basic YAML configuration file",
	)
	checkCommand.Flags().StringVar(
		&checkRoutes,
		"routes",
		"",
		"optional local Store YAML configuration file",
	)

	versionCommand := &cobra.Command{
		Use:   "version",
		Short: "Print build identity",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return printVersion(command.OutOrStdout())
		},
	}

	probeAddress := "127.0.0.1:7449"
	probeCommand := &cobra.Command{
		Use:   "probe live|ready",
		Short: "Check loopback diagnostics",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return runProbe(command.Context(), args[0], probeAddress)
		},
	}
	probeCommand.Flags().StringVar(
		&probeAddress,
		"address",
		"127.0.0.1:7449",
		"loopback diagnostic IP:port",
	)

	root.AddCommand(serveCommand, checkCommand, versionCommand, probeCommand)
	root.SetOut(output)
	root.SetErr(output)

	if args == nil {
		args = []string{}
	}
	root.SetArgs(args)
	return root.ExecuteContext(context.Background())
}

func serve(ctx context.Context, cfg app.Config, output io.Writer) (resultErr error) {
	// The CLI owns signals from before assembly until all owned resources close.
	signals, cancelSignal := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancelSignal()

	startup, stop := context.WithTimeout(signals, time.Duration(cfg.Basic.Lifecycle.StartupTimeout))
	defer stop()

	node, err := app.Open(startup, cfg)
	if err != nil {
		return err
	}

	defer func() {
		// A startup cancellation must not cancel the drain budget too.
		drain, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Basic.Lifecycle.ShutdownTimeout))
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

	_, err = fmt.Fprintf(output, "Weir listening on %v; local execution and peer directory discovery\n", node.Addresses())
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
