//go:build integration

package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testmongo"
	"github.com/batchstream/weir/internal/testpeer"
	"github.com/batchstream/weir/internal/testsearch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type process struct {
	command *exec.Cmd
	address string
	stderr  *bytes.Buffer
	done    chan error
	once    sync.Once
}

func (p *process) stop(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		_ = p.command.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-p.done:
			if err != nil {
				t.Errorf("Weir process exit: %v %s", err, p.stderr.String())
			}
		case <-time.After(8 * time.Second):
			_ = p.command.Process.Kill()
			<-p.done
			t.Error("Weir process drain exceeded bound")
		}
	})
}

func startProcess(t *testing.T, binary string, cfg Config) *process {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(t.TempDir(), "node.json")
	if err := os.WriteFile(name, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, binary, "-config", name)
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := new(bytes.Buffer)
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{command: command, stderr: stderr, done: make(chan error, 1)}
	line := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		if scanner.Scan() {
			line <- scanner.Text()
		} else {
			line <- ""
		}
	}()
	t.Cleanup(func() { p.stop(t) })
	select {
	case message := <-line:
		start, end := strings.Index(message, "["), strings.Index(message, "]")
		if start < 0 || end <= start {
			go func() { p.done <- command.Wait() }()
			t.Fatal("no process readiness", message)
		}
		p.address = strings.Fields(message[start+1 : end])[0]
	case <-time.After(10 * time.Second):
		go func() { p.done <- command.Wait() }()
		t.Fatal("process startup timeout")
	}
	go func() { p.done <- command.Wait() }()
	return p
}
func TestIndependentWeirProcesses(t *testing.T) {
	// testmongo.Open gates the entire smoke before starting child processes.
	_, database := testmongo.Open(t)
	search := testsearch.Open(t)
	dir := t.TempDir()
	binaries := make(map[string]string)
	for _, name := range []string{"weir", "weir-example", "weir-native-example"} {
		binary := filepath.Join(dir, name)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./cmd/"+name)
		cmd.Dir = "../.."
		output, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("build %s: %v %s", name, err, output)
		}
		binaries[name] = binary
	}
	ca := testpeer.NewCA(t)
	identities := make(map[string]*Identity)
	for _, name := range []string{"a", "b", "c"} {
		_, certificate, key := ca.Identity(t, name+".weir.test")
		identity := &Identity{Certificate: filepath.Join(dir, name+".crt"), PrivateKey: filepath.Join(dir, name+".private"), CA: filepath.Join(dir, "ca.crt")}
		for file, data := range map[string][]byte{identity.Certificate: certificate, identity.PrivateKey: key, identity.CA: ca.PEM} {
			if err := os.WriteFile(file, data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		identities[name] = identity
	}
	mongo := &Mongo{URI: testmongo.URI, Database: database, Collection: "records"}
	mongoLocal := &Local{Mongo: mongo}
	backend := &Search{URL: search.URL, Index: search.Index, Profile: search.Profile}
	searchLocal := &Local{Search: backend}
	mongoService := Service{Name: "mongo", Local: mongoLocal}
	searchService := Service{Name: "search", Local: searchLocal}
	mongoRoute := Route{Store: "mongo", Service: "mongo"}
	searchRoute := Route{Store: "search", Service: "search"}
	c := DefaultConfig()
	c.Peer = "127.0.0.1:0"
	c.Identity = identities["c"]
	c.Services = []Service{mongoService, searchService}
	c.Routes = []Route{mongoRoute, searchRoute}
	for _, name := range []string{"mongo", "search"} {
		grant := Grant{Identity: "b.weir.test", Store: name, Operations: []string{"read", "mutate", "scan", "native"}}
		c.Allow = append(c.Allow, grant)
	}
	final := startProcess(t, binaries["weir"], c)
	b := DefaultConfig()
	b.Peer = "127.0.0.1:0"
	b.Identity = identities["b"]
	for _, name := range []string{"mongo", "search"} {
		remote := &Remote{Endpoint: final.address, ServerName: "c.weir.test", Relays: 4}
		service := Service{Name: name, Remote: remote}
		route := Route{Store: name, Service: name}
		grant := Grant{Identity: "a.weir.test", Store: name, Operations: []string{"read", "mutate", "scan", "native"}}
		b.Services = append(b.Services, service)
		b.Routes = append(b.Routes, route)
		b.Allow = append(b.Allow, grant)
	}
	middle := startProcess(t, binaries["weir"], b)
	a := DefaultConfig()
	a.Application = "127.0.0.1:0"
	a.Identity = identities["a"]
	for _, name := range []string{"mongo", "search"} {
		remote := &Remote{Endpoint: middle.address, ServerName: "b.weir.test", Relays: 4}
		service := Service{Name: name, Remote: remote}
		route := Route{Store: name, Service: name}
		a.Services = append(a.Services, service)
		a.Routes = append(a.Routes, route)
	}
	first := startProcess(t, binaries["weir"], a)
	for _, kind := range []string{"mongo", "search"} {
		for _, example := range []string{"weir-example", "weir-native-example"} {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			command := exec.CommandContext(ctx, binaries[example], "-address", first.address, "-store", kind, "-database", database, "-index", search.Index)
			output, err := command.CombinedOutput()
			cancel()
			if err != nil {
				t.Fatalf("%s/%s: %v %s", kind, example, err, output)
			}
			t.Logf("three independent Weir processes, %s/%s: %s", kind, example, strings.TrimSpace(string(output)))
		}
	}
	t.Log(fmt.Sprintf("process IDs A=%d B=%d C=%d; profile=%s; TLS identities are test-generated", first.command.Process.Pid, middle.command.Process.Pid, final.command.Process.Pid, search.Profile))
	c.Peer, b.Peer, a.Application = final.address, middle.address, first.address
	conn, err := grpc.NewClient("passthrough:///"+first.address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewWeirClient(conn)
	request := &pb.ReadRequest{Resource: "weir://mongo/" + database + "/records/s:example"}
	for _, node := range []struct {
		name string
		old  *process
		cfg  Config
	}{{"C", final, c}, {"B", middle, b}, {"A", first, a}} {
		node.old.stop(t)
		replacement := startProcess(t, binaries["weir"], node.cfg)
		until := time.Now().Add(5 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			result, err := client.Read(ctx, request)
			cancel()
			if err == nil && result.GetDocument() != nil {
				break
			}
			if time.Now().After(until) {
				t.Fatal("fresh Read did not recover after process replacement", node.name, err)
			}
			// Each probe is a distinct read-only call; no failed write is retried.
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("%s drained and exited; replacement PID=%d; existing clients recovered for a fresh Read", node.name, replacement.command.Process.Pid)
	}
}
