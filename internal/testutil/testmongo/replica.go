//go:build integration

package testmongo

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type ReplicaOptions struct{ TLS bool }

type replicaMember struct {
	address string
	process *exec.Cmd
	done    chan error
	log     *os.File
	stopped bool
}

// Replica owns exactly three foreground processes. It never adopts an endpoint.
// This is a functional fixture, not a database or cgroup qualification profile.
type Replica struct {
	URI, SetName, DB                      string
	Username, Password, CAFile, BadCAFile string
	Admin                                 *mongo.Client
	members                               [3]replicaMember
	clients                               []*mongo.Client
	root                                  string
	roots                                 *x509.CertPool
	adminPassword                         string
	secure                                bool
	once                                  sync.Once
}

func OpenReplica(t *testing.T, opts ReplicaOptions) *Replica {
	t.Helper()
	if os.Getenv("WEIR_MONGO_REPLICA_INTEGRATION") != "1" {
		t.Fatal("owned replica tests require WEIR_MONGO_REPLICA_INTEGRATION=1")
	}
	binary := replicaBinary(t)
	parent := filepath.Join(testutil.Root(t), ".testdata")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(parent, "mongo-replica-")
	if err != nil {
		t.Fatal(err)
	}
	f := &Replica{root: root, secure: opts.TLS, SetName: "weir_" + filepath.Base(root), DB: "weir_replica"}
	t.Cleanup(func() { f.Close(t) })
	if err := os.WriteFile(filepath.Join(root, ".weir-owner"), []byte(f.SetName), 0600); err != nil {
		t.Fatal(err)
	}
	reservations := make([]net.Listener, 0, len(f.members))
	defer func() {
		for _, listener := range reservations {
			_ = listener.Close()
		}
	}()
	for i := range f.members {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		reservations = append(reservations, listener)
		port := listener.Addr().(*net.TCPAddr).Port
		host := "127.0.0.1"
		if f.secure && i == 0 {
			host = "localhost"
		}
		f.members[i].address = net.JoinHostPort(host, strconv.Itoa(port))
	}
	if f.secure {
		f.writeMaterial(t)
	}
	startup, cancel := context.WithTimeout(t.Context(), 75*time.Second)
	defer cancel()
	for i := range f.members {
		// Keep every other port reserved until its own process is started.
		if err := reservations[i].Close(); err != nil {
			t.Fatal(err)
		}
		f.start(t, binary, i)
	}
	first := f.connect(t, 0, false)
	initiate := bson.D{{Key: "replSetInitiate", Value: f.configuration()}}
	if err := first.Database("admin").RunCommand(startup, initiate).Err(); err != nil {
		t.Fatal("owned replica initialization", err)
	}
	primary, _, err := f.observePrimary(startup, -1, !f.secure)
	if err != nil {
		t.Fatal("owned external replica did not become usable", err)
	}
	if f.secure {
		admin := f.directOptions(primary, false)
		client, err := mongo.Connect(admin)
		if err != nil {
			t.Fatal(err)
		}
		f.clients = append(f.clients, client)
		f.adminPassword = replicaSecret(t)
		roles := bson.A{bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}}}
		command := bson.D{{Key: "createUser", Value: "weir_admin"}, {Key: "pwd", Value: f.adminPassword}, {Key: "roles", Value: roles}}
		if err := client.Database("admin").RunCommand(startup, command).Err(); err != nil {
			t.Fatal("create owned administrator", err)
		}
	}
	for i := range f.members {
		f.connect(t, i, f.secure)
	}
	adminOptions := f.directOptions(primary, f.secure)
	adminOptions.SetHosts(f.Addresses()).SetDirect(false).SetReplicaSet(f.SetName)
	f.Admin, err = mongo.Connect(adminOptions)
	if err != nil {
		t.Fatal(err)
	}
	f.clients = append(f.clients, f.Admin)
	if err := f.Admin.Database(f.DB).CreateCollection(startup, "records"); err != nil {
		t.Fatal("owned collection", err)
	}
	if f.secure {
		f.Username, f.Password = "weir_app", replicaSecret(t)
		roles := bson.A{bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: f.DB}}}
		command := bson.D{{Key: "createUser", Value: f.Username}, {Key: "pwd", Value: f.Password}, {Key: "roles", Value: roles}}
		if err := f.Admin.Database("admin").RunCommand(startup, command).Err(); err != nil {
			t.Fatal("create owned application user", err)
		}
	}
	f.URI = f.SeedURI(f.Addresses())
	t.Logf("owned external MongoDB 8.0.32 replica root=%s TLS=%t members=%v", root, f.secure, f.Addresses())
	return f
}

func replicaBinary(t *testing.T) string {
	t.Helper()
	name := os.Getenv("WEIR_MONGO_REPLICA_BINARY")
	if !filepath.IsAbs(name) {
		t.Fatal("explicit absolute pinned native MongoDB binary required")
	}
	expected, size := "", int64(0)
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "darwin/arm64":
		expected, size = "440012d251e0ba135f519e4e8b178e5f406d4ce2c98fdbb15766363187ae57c1", 189751584
	case "linux/amd64":
		expected, size = "15c7caf565edeaca228c76fbe6c4503b1cff7ae6517f344cc555552423b3209a", 222890240
	default:
		t.Fatal("no pinned native MongoDB fixture for this platform")
	}
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Size() != size {
		t.Fatal("pinned MongoDB binary identity mismatch")
	}
	file, err := os.Open(name)
	if err != nil {
		t.Fatal("cannot open explicit MongoDB binary")
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || hex.EncodeToString(hash.Sum(nil)) != expected {
		t.Fatal("pinned MongoDB binary digest mismatch")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	version := exec.CommandContext(ctx, name, "--version")
	output, err := version.Output()
	if err != nil || !strings.HasPrefix(string(output), "db version v8.0.32\n") {
		t.Fatal("native MongoDB 8.0.32 preflight failed")
	}
	t.Logf("native Mongo preflight binary=%s SHA256=%s PID=%d wait_complete=true exit_code=%d", name, expected, version.Process.Pid, version.ProcessState.ExitCode())
	return name
}

func (f *Replica) Addresses() []string {
	addresses := make([]string, len(f.members))
	for i := range f.members {
		addresses[i] = f.members[i].address
	}
	return addresses
}

func (f *Replica) SeedURI(seeds []string) string {
	query := "replicaSet=" + f.SetName
	if f.secure {
		query += "&authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
	}
	return "mongodb://" + strings.Join(seeds, ",") + "/?" + query
}

func (f *Replica) configuration() bson.D {
	members := make(bson.A, 0, len(f.members))
	for i, member := range f.members {
		entry := bson.D{{Key: "_id", Value: i}, {Key: "host", Value: member.address}}
		members = append(members, entry)
	}
	configuration := bson.D{{Key: "_id", Value: f.SetName}, {Key: "members", Value: members}}
	return configuration
}

func (f *Replica) start(t *testing.T, binary string, i int) {
	t.Helper()
	member := &f.members[i]
	directory := filepath.Join(f.root, strconv.Itoa(i))
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	var err error
	member.log, err = os.OpenFile(filepath.Join(f.root, fmt.Sprintf("member-%d.log", i)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(member.address)
	args := []string{"--dbpath", directory, "--bind_ip", "127.0.0.1", "--port", port, "--replSet", f.SetName, "--wiredTigerCacheSizeGB", "0.25"}
	if f.secure {
		args = append(args, "--auth", "--keyFile", filepath.Join(f.root, "keyfile"), "--tlsMode", "requireTLS", "--tlsCertificateKeyFile", filepath.Join(f.root, fmt.Sprintf("member-%d.pem", i)), "--tlsCAFile", f.CAFile, "--tlsAllowConnectionsWithoutCertificates")
	}
	member.process = exec.Command(binary, args...)
	member.process.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=" + f.root, "TMPDIR=" + f.root}
	member.process.Stdout, member.process.Stderr = member.log, member.log
	if err := member.process.Start(); err != nil {
		t.Fatal("cannot start owned replica member", err)
	}
	member.done = make(chan error, 1)
	go func() { member.done <- member.process.Wait() }()
	t.Logf("owned replica member=%d PID=%d address=%s root=%s", i, member.process.Process.Pid, member.address, f.root)
}

func (f *Replica) directOptions(i int, authenticated bool) *options.ClientOptions {
	opts := options.Client().SetHosts([]string{f.members[i].address}).SetDirect(true).SetRetryReads(false).SetRetryWrites(false).SetMaxAdaptiveRetries(0).SetEnableOverloadRetargeting(false).SetMaxPoolSize(2).SetMaxConnecting(1).SetConnectTimeout(time.Second).SetServerSelectionTimeout(time.Second).SetWriteConcern(writeconcern.Majority())
	if f.secure {
		config := &tls.Config{RootCAs: f.roots}
		opts.SetTLSConfig(config)
		if authenticated {
			credential := options.Credential{Username: "weir_admin", Password: f.adminPassword, PasswordSet: true, AuthSource: "admin", AuthMechanism: "SCRAM-SHA-256"}
			opts.SetAuth(credential)
		}
	}
	return opts
}

func (f *Replica) connect(t *testing.T, i int, authenticated bool) *mongo.Client {
	t.Helper()
	opts := f.directOptions(i, authenticated)
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	f.clients = append(f.clients, client)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		command := bson.D{{Key: "hello", Value: 1}}
		err = client.Database("admin").RunCommand(ctx, command).Err()
		cancel()
		if err == nil {
			return client
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("owned member unavailable during startup")
	return nil
}

// WaitUsable probes the real members directly, not Weir's topology. The boundary
// is the earliest observed primary that acknowledges an independent majority write.
// Database transition time remains separate from subsequent Weir reaction time.
func (f *Replica) WaitUsable(ctx context.Context, exclude int) (int, time.Time, error) {
	return f.observePrimary(ctx, exclude, true)
}

func (f *Replica) observePrimary(ctx context.Context, exclude int, majority bool) (int, time.Time, error) {
	var zero time.Time
	for ctx.Err() == nil {
		for i := range f.members {
			if i == exclude || f.members[i].stopped {
				continue
			}
			opts := f.directOptions(i, f.adminPassword != "")
			client, err := mongo.Connect(opts)
			if err != nil {
				return -1, zero, err
			}
			probe, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
			var hello struct {
				Primary bool   `bson:"isWritablePrimary"`
				SetName string `bson:"setName"`
			}
			command := bson.D{{Key: "hello", Value: 1}}
			err = client.Database("admin").RunCommand(probe, command).Decode(&hello)
			if err == nil && hello.Primary && hello.SetName == f.SetName {
				if majority {
					document := bson.D{{Key: "at", Value: time.Now().UnixNano()}}
					_, err = client.Database(f.DB).Collection("service_probe").InsertOne(probe, document)
				}
				if err == nil {
					usable := time.Now()
					cancel()
					cleanup, stop := context.WithTimeout(context.Background(), time.Second)
					closeErr := client.Disconnect(cleanup)
					stop()
					if closeErr != nil {
						return -1, zero, closeErr
					}
					return i, usable, nil
				}
			}
			cancel()
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			closeErr := client.Disconnect(cleanup)
			stop()
			if closeErr != nil {
				return -1, zero, closeErr
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(25 * time.Millisecond):
		}
	}
	return -1, zero, ctx.Err()
}

func (f *Replica) StepDown(ctx context.Context, i int) error {
	opts := f.directOptions(i, f.secure)
	client, err := mongo.Connect(opts)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = client.Disconnect(cleanup)
	}()
	command := bson.D{{Key: "replSetStepDown", Value: 30}}
	return client.Database("admin").RunCommand(ctx, command).Err()
}

func (f *Replica) LosePrimary(t *testing.T, i int) {
	t.Helper()
	member := &f.members[i]
	if member.stopped || member.process == nil || member.done == nil {
		t.Fatal("member not owned and running")
	}
	if err := member.process.Process.Kill(); err != nil {
		t.Fatal("owned member loss", err)
	}
	select {
	case <-member.done:
		member.stopped = true
	case <-time.After(5 * time.Second):
		t.Fatal("owned member did not reap")
	}
}

func (f *Replica) Close(t *testing.T) {
	t.Helper()
	f.once.Do(func() {
		for _, client := range f.clients {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := client.Disconnect(ctx)
			cancel()
			if err != nil && !errors.Is(err, mongo.ErrClientDisconnected) {
				t.Error("owned admin disconnect", err)
			}
		}
		for i := range f.members {
			member := &f.members[i]
			if member.done != nil && !member.stopped {
				_ = member.process.Process.Signal(syscall.SIGTERM)
			}
		}
		reaped := true
		for i := range f.members {
			member := &f.members[i]
			if member.done != nil && !member.stopped {
				select {
				case <-member.done:
					member.stopped = true
				case <-time.After(8 * time.Second):
					_ = member.process.Process.Kill()
					select {
					case <-member.done:
						member.stopped = true
					case <-time.After(2 * time.Second):
						reaped = false
						t.Error("owned member did not reap after kill")
					}
				}
			}
			if member.stopped {
				t.Logf("owned replica member=%d PID=%d wait_complete=true terminal_state=%v", i, member.process.Process.Pid, member.process.ProcessState)
			}
			if member.log != nil {
				_ = member.log.Close()
			}
		}
		if !reaped {
			return
		}
		marker, err := os.ReadFile(filepath.Join(f.root, ".weir-owner"))
		if err != nil || string(marker) != f.SetName {
			t.Error("owned root marker mismatch; preserving data")
			return
		}
		for i := range f.members {
			if err := os.RemoveAll(filepath.Join(f.root, strconv.Itoa(i))); err != nil {
				t.Error("owned data cleanup", err)
			}
			if f.secure {
				if err := os.Remove(filepath.Join(f.root, fmt.Sprintf("member-%d.pem", i))); err != nil {
					t.Error("owned leaf cleanup", err)
				}
			}
		}
		if f.secure {
			for _, name := range []string{"ca.pem", "bad-ca.pem", "keyfile"} {
				if err := os.Remove(filepath.Join(f.root, name)); err != nil {
					t.Error("owned material cleanup", err)
				}
			}
		}
		t.Logf("owned replica cleanup confirmed: three processes reaped; data and temporary secrets removed; logs=%s", f.root)
	})
}

func replicaSecret(t *testing.T) string {
	t.Helper()
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func (f *Replica) writeMaterial(t *testing.T) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: randomSerial(), Subject: pkix.Name{CommonName: "Owned Weir replica CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	f.roots = x509.NewCertPool()
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	f.roots.AddCert(parsed)
	f.CAFile = filepath.Join(f.root, "ca.pem")
	block := &pem.Block{Type: "CERTIFICATE", Bytes: der}
	if err := os.WriteFile(f.CAFile, pem.EncodeToMemory(block), 0600); err != nil {
		t.Fatal(err)
	}
	for i, member := range f.members {
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		host, _, _ := net.SplitHostPort(member.address)
		leaf := &x509.Certificate{SerialNumber: randomSerial(), Subject: pkix.Name{CommonName: "Owned Weir replica member"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		if ip := net.ParseIP(host); ip != nil {
			leaf.IPAddresses = []net.IP{ip}
		} else {
			leaf.DNSNames = []string{host}
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leaf, parsed, &leafKey.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		privateDER, err := x509.MarshalECPrivateKey(leafKey)
		if err != nil {
			t.Fatal(err)
		}
		certBlock := &pem.Block{Type: "CERTIFICATE", Bytes: leafDER}
		keyBlock := &pem.Block{Type: "EC PRIVATE KEY", Bytes: privateDER}
		material := append(pem.EncodeToMemory(certBlock), pem.EncodeToMemory(keyBlock)...)
		if err := os.WriteFile(filepath.Join(f.root, fmt.Sprintf("member-%d.pem", i)), material, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// A syntactically valid but unrelated trust root is sufficient for rejection.
	otherKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &otherKey.PublicKey, otherKey)
	if err != nil {
		t.Fatal(err)
	}
	otherBlock := &pem.Block{Type: "CERTIFICATE", Bytes: otherDER}
	f.BadCAFile = filepath.Join(f.root, "bad-ca.pem")
	if err := os.WriteFile(f.BadCAFile, pem.EncodeToMemory(otherBlock), 0600); err != nil {
		t.Fatal(err)
	}
	material, err := temporaryKeyFile()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.root, "keyfile"), material, 0600); err != nil {
		t.Fatal(err)
	}
}
