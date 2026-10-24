//go:build integration

package app

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The only backend is this owned offline fixture. The Node, loopback gRPC
// transport, handler, runtime, execution and Close worker are real owners.
type retirementNodeAdapter struct {
	started     chan struct{}
	release     chan struct{}
	joined      chan struct{}
	closeJoined chan struct{}
	failure     error
}

func (a *retirementNodeAdapter) PrepareNative(context.Context, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "record fixture")
}
func (a *retirementNodeAdapter) PrepareRecord(_ context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	work := &execution.Plan{Command: record.Command(), Key: record.Key(), BatchKey: "records", Bytes: record.Bytes()}
	return work, nil
}
func (a *retirementNodeAdapter) ExecuteRecords(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	defer close(a.joined)
	close(a.started)
	<-ctx.Done()
	<-a.release
	events := make([]*pb.Event, len(plans))
	for i := range plans {
		failure := protocol.ContextFailure(ctx)
		read := protocol.ReadFailure(failure)
		value := &pb.Event_ReadResult{ReadResult: read}
		events[i] = &pb.Event{Value: value}
	}
	return events
}
func (a *retirementNodeAdapter) ExecuteNative(context.Context, *execution.Plan, execution.Emit) *pb.NativeEnd {
	failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "record fixture")
	return protocol.NativeFailure(false, failure)
}
func (a *retirementNodeAdapter) Close(context.Context) error {
	defer close(a.closeJoined)
	close(a.release)
	<-a.joined
	return a.failure
}

type retirementNodeFixture struct {
	node       *Node
	adapter    *retirementNodeAdapter
	callDone   <-chan error
	connection *grpc.ClientConn
}

func newRetirementNode(t *testing.T, failure error) retirementNodeFixture {
	t.Helper()
	cfg := emptyConfig(t)
	node, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &retirementNodeAdapter{started: make(chan struct{}), release: make(chan struct{}), joined: make(chan struct{}), closeJoined: make(chan struct{}), failure: failure}
	limits := store.DefaultLimits()
	runtime, err := store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	node.stores["records"] = runtime
	serverCfg := server.Config{Stores: node.stores, Limits: server.DefaultLimits(), Admission: node.admission}
	endpointServer, err := server.New(serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	node.server = endpointServer
	t.Cleanup(func() {
		if err := node.Close(context.Background()); !errors.Is(err, failure) {
			t.Error(err)
		}
	})
	if err := node.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient("passthrough:///"+node.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := pb.NewStoreServiceClient(connection)
	callDone := make(chan error, 1)
	go func() {
		request := &pb.ReadRequest{Resource: "db/records/s:a"}
		_, err := testutil.ExecuteRecord(t.Context(), client, testutil.RecordRequest("records", request))
		callDone <- err
	}()
	select {
	case <-adapter.started:
	case <-time.After(3 * time.Second):
		t.Fatal("real gRPC handler never dispatched the record")
	}
	fixture := retirementNodeFixture{node: node, adapter: adapter, callDone: callDone, connection: connection}
	return fixture
}

func closeRetirementNode(t *testing.T, fixture retirementNodeFixture) {
	t.Helper()
	node, adapter := fixture.node, fixture.adapter
	started := time.Now()
	err := node.Close(context.Background())
	elapsed := time.Since(started)
	if !errors.Is(err, adapter.failure) || elapsed < 6500*time.Millisecond || elapsed > 7500*time.Millisecond {
		t.Fatal("real Node default drain/retirement result lost", err, elapsed)
	}
	select {
	case <-adapter.joined:
	default:
		t.Fatal("Node returned before execution joined")
	}
	select {
	case <-adapter.closeJoined:
	default:
		t.Fatal("Node returned before adapter Close worker joined")
	}
	select {
	case <-fixture.callDone:
	case <-time.After(time.Second):
		t.Fatal("gRPC client call not terminal")
	}
	if stats := node.stores["records"].Stats(); stats.Active != 0 || stats.Queued != 0 {
		t.Fatal("Node retained execution ownership", stats)
	}
	// Server.Shutdown uses WaitForHandlers, and its shared counters must reflect
	// actual handler and transport retirement rather than a socket-only proxy.
	families := testmetrics.Registry(t, node.registry)
	if testmetrics.Sum(families, "weir_ingress_sessions") != 0 {
		t.Fatal("Node returned before real handlers joined")
	}
	listener, err := net.Listen("tcp", node.Address())
	if err != nil {
		t.Fatal("Node did not release its listener", err)
	}
	_ = listener.Close()
	t.Logf("actual Node Close elapsed=%s error=%v runtime=%+v", elapsed, err, node.stores["records"].Stats())
}

func TestOfflineRetirementNodeJoinsDefaultOwners(t *testing.T) {
	fixture := newRetirementNode(t, nil)
	closeRetirementNode(t, fixture)
}

func TestOfflineRetirementNodeChild(t *testing.T) {
	mode := os.Getenv("WEIR_RETIREMENT_CHILD")
	if mode == "" {
		t.Skip("owned self-subprocess only")
	}
	var expected error
	if mode == "nonzero" {
		expected = errors.New("injected actual adapter close failure")
	}
	fixture := newRetirementNode(t, expected)
	termination := make(chan os.Signal, 1)
	signal.Notify(termination, syscall.SIGTERM)
	defer signal.Stop(termination)
	fmt.Fprintln(os.Stdout, "retirement-node-ready")
	<-termination
	closeRetirementNode(t, fixture)
	if expected != nil {
		os.Exit(7)
	}
}

func TestOfflineRetirementSIGTERMActualNode(t *testing.T) {
	for _, mode := range []string{"clean", "nonzero"} {
		t.Run(mode, func(t *testing.T) {
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			child := exec.Command(binary, "-test.run=^TestOfflineRetirementNodeChild$", "-test.v", "-test.timeout=20s")
			for _, item := range os.Environ() {
				if !strings.HasPrefix(item, "WEIR_") && !strings.HasPrefix(item, "DOCKER_") {
					child.Env = append(child.Env, item)
				}
			}
			child.Env = append(child.Env, "WEIR_RETIREMENT_CHILD="+mode)
			output, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			stderr := new(bytes.Buffer)
			child.Stderr = stderr
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			ready, readDone := make(chan struct{}), make(chan struct{})
			var stdout strings.Builder
			go func() {
				defer close(readDone)
				scanner := bufio.NewScanner(output)
				for scanner.Scan() {
					line := scanner.Text()
					stdout.WriteString(line + "\n")
					if line == "retirement-node-ready" {
						close(ready)
					}
				}
			}()
			exited := make(chan error, 1)
			reaped := make(chan struct{})
			go func() { exited <- child.Wait(); close(reaped) }()
			var kill sync.Once
			stop := func() { kill.Do(func() { _ = child.Process.Kill() }); <-reaped; <-readDone }
			t.Cleanup(stop)
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				stop()
				t.Fatal("owned Node child not ready", stdout.String(), stderr.String())
			}
			started := time.Now()
			if err := child.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			var waitErr error
			timedOut := false
			select {
			case waitErr = <-exited:
			case <-time.After(8 * time.Second):
				timedOut = true
				stop()
				waitErr = <-exited
			}
			elapsed := time.Since(started)
			<-readDone
			code := child.ProcessState.ExitCode()
			t.Logf("mode=%s actual_SIGTERM_elapsed=%s exit=%d timed_out=%t wait_error=%v\n%s\n%s", mode, elapsed, code, timedOut, waitErr, stdout.String(), stderr.String())
			expectedCode := 0
			if mode == "nonzero" {
				expectedCode = 7
			}
			if timedOut || elapsed > 8*time.Second || code != expectedCode || (waitErr != nil) != (mode == "nonzero") {
				t.Fatal("actual terminal state concealed or retirement exceeded 8s")
			}
		})
	}
}
