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
	"github.com/batchstream/weir/internal/overload"
	"github.com/spf13/cobra"
)

// startupTimeout bounds assembly, backend qualification and listener startup.
const startupTimeout = 30 * time.Second

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
	serveCommand := &cobra.Command{
		Use:   "serve",
		Short: "Start the node and serve its configured Stores",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			cfg, err := load(serveConfig)
			if err != nil {
				return err
			}
			return serve(command.Context(), cfg, command.OutOrStdout())
		},
	}
	serveCommand.Flags().StringVarP(&serveConfig, "config", "c", "weir.yaml", "YAML configuration file")

	checkConfig := "weir.yaml"
	checkCommand := &cobra.Command{
		Use:   "check",
		Short: "Validate configuration without listeners, DNS or backend connections",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if _, err := load(checkConfig); err != nil {
				return err
			}
			_, err := io.WriteString(command.OutOrStdout(), "configuration valid\n")
			return err
		},
	}
	checkCommand.Flags().StringVarP(&checkConfig, "config", "c", "weir.yaml", "YAML configuration file")

	versionCommand := &cobra.Command{
		Use:   "version",
		Short: "Print build identity",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return printVersion(command.OutOrStdout())
		},
	}

	root.AddCommand(serveCommand, checkCommand, versionCommand)
	root.SetOut(output)
	root.SetErr(output)

	if args == nil {
		args = []string{}
	}
	root.SetArgs(args)
	return root.ExecuteContext(context.Background())
}

func load(filename string) (app.Config, error) {
	if filename == "" {
		var empty app.Config
		return empty, errors.New("--config requires a file")
	}
	return app.Load(filename)
}

func serve(ctx context.Context, cfg app.Config, output io.Writer) error {
	// The CLI owns the process: its Go memory limit and, from before assembly
	// until all owned resources close, its signals.
	overload.SetContainerMemoryLimit()
	signals, cancelSignal := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancelSignal()
	return serveContext(signals, cfg, output)
}

func serveContext(ctx context.Context, cfg app.Config, output io.Writer) (resultErr error) {
	startup, stop := context.WithTimeout(ctx, startupTimeout)
	defer stop()

	node, err := app.Open(startup, cfg)
	if err != nil {
		return err
	}
	defer func() {
		// A startup cancellation must not cancel the drain; Close bounds it
		// with the configured shutdown timeout.
		resultErr = errors.Join(resultErr, node.Close(context.Background()))

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

	_, err = fmt.Fprintf(output, "Weir listening on %s\n", node.Address())
	if err != nil {
		return errors.New("listener output failed")
	}

	if node.DiagnosticAddress() != "" {
		if _, err := fmt.Fprintf(output, "Diagnostics listening on %s\n", node.DiagnosticAddress()); err != nil {
			return errors.New("listener output failed")
		}
	}

	select {
	case <-ctx.Done():
		return nil
	case err := <-node.Errors:
		return err
	}
}
