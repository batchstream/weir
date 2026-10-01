//go:build integration

package testmongo

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type SecureFixture struct {
	Fixture
	Client *mongo.Client

	startup        context.Context
	owned          bool
	root           string
	process        *exec.Cmd
	processEnd     chan error
	logFile        *os.File
	port           int
	caFile         string
	adminURI       string
	BadPassURI     string
	WrongHostURI   string
	BadCAURI       string
	MissingPassURI string
	DeniedURI      string
}

var secureFixtureSequence atomic.Uint64

func OpenSecure(t *testing.T) *SecureFixture {
	t.Helper()
	if os.Getenv("WEIR_M10_INTEGRATION") != "1" {
		t.Fatal("secure integration requires WEIR_M10_INTEGRATION=1")
	}
	startup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	fixture := &SecureFixture{startup: startup}
	t.Cleanup(func() { fixture.cleanup(t) })
	fixture.prepareRoot(t)
	fixture.startMongo(t)
	fixture.configureUsers(t)
	fixture.Client = fixture.connect(t, fixture.URI)
	return fixture
}

func (fixture *SecureFixture) prepareRoot(t *testing.T) {
	projectDirectory := testutil.Root(t)
	if err := os.MkdirAll(filepath.Join(projectDirectory, ".testdata"), 0700); err != nil {
		t.Fatal("cannot create fixture parent")
	}
	fixture.root = filepath.Join(projectDirectory, ".testdata", fmt.Sprintf("mongo-m10-%d-%d", os.Getpid(), secureFixtureSequence.Add(1)))
	if err := os.Mkdir(fixture.root, 0700); err != nil {
		t.Fatal("cannot create unique MongoDB fixture directory")
	}
	fixture.owned = true
	marker := filepath.Join(fixture.root, ".weir-owner")
	if err := os.WriteFile(marker, []byte("weir-milestone-10 mongodb-8.0.32 loopback TLS-SCRAM\n"), 0600); err != nil {
		t.Fatal("cannot mark MongoDB fixture ownership")
	}
	fixture.caFile = filepath.Join(fixture.root, "ca.pem")
	if err := fixture.writeCertificates(); err != nil {
		t.Fatal("cannot prepare temporary MongoDB TLS fixture")
	}
	var err error
	fixture.logFile, err = os.OpenFile(filepath.Join(fixture.root, "mongod.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal("cannot create MongoDB fixture log")
	}
	fixture.DB = fmt.Sprintf("weir_m10_%d_%d", os.Getpid(), secureFixtureSequence.Load())
	fixture.writeOtherCA(t)
}

func (fixture *SecureFixture) writeCertificates() error {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "Weir M10 test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		// The explicit packaging fixture reaches this owned Darwin process from Linux.
		DNSNames:    []string{"host.docker.internal"},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	serverBlock := &pem.Block{Type: "CERTIFICATE", Bytes: serverDER}
	keyBlock := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)}
	serverPEM := pem.EncodeToMemory(serverBlock)
	serverPEM = append(serverPEM, pem.EncodeToMemory(keyBlock)...)
	pair := tls.Certificate{Certificate: [][]byte{serverDER}, PrivateKey: serverKey}
	fixture.serverTLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	fixture.clientTLS = &tls.Config{RootCAs: roots, ServerName: "127.0.0.1"}
	if err := os.WriteFile(filepath.Join(fixture.root, "server.pem"), serverPEM, 0600); err != nil {
		return err
	}
	caBlock := &pem.Block{Type: "CERTIFICATE", Bytes: caDER}
	caPEM := pem.EncodeToMemory(caBlock)
	if err := os.WriteFile(fixture.caFile, caPEM, 0600); err != nil {
		return err
	}
	key, err := temporaryKeyFile()
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(fixture.root, "keyfile"), key, 0600)
}

func randomSerial() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		panic("cryptographic random source unavailable")
	}
	return serial
}

func temporaryKeyFile() ([]byte, error) {
	material := make([]byte, 512)
	if _, err := rand.Read(material); err != nil {
		return nil, err
	}
	return []byte(base64.RawStdEncoding.EncodeToString(material)), nil
}

func (fixture *SecureFixture) writeOtherCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal("cannot generate negative-test CA")
	}
	template := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "Untrusted M10 test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal("cannot create negative-test CA")
	}
	path := filepath.Join(fixture.root, "untrusted-ca.pem")
	block := &pem.Block{Type: "CERTIFICATE", Bytes: der}
	data := pem.EncodeToMemory(block)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal("cannot write negative-test CA")
	}
	return path
}

func (fixture *SecureFixture) uri(username, password, host, caFile string) string {
	query := url.Values{}
	query.Set("directConnection", "true")
	query.Set("serverMonitoringMode", "poll")
	query.Set("tls", "true")
	query.Set("tlsCAFile", caFile)
	var credentials *url.Userinfo
	if username == "" {
		credentials = nil
	} else {
		query.Set("authMechanism", "SCRAM-SHA-256")
		query.Set("authSource", "admin")
		if password == "" {
			credentials = url.User(username)
		} else {
			credentials = url.UserPassword(username, password)
		}
	}
	requestURI := url.URL{
		Scheme:   "mongodb",
		User:     credentials,
		Host:     net.JoinHostPort(host, strconv.Itoa(fixture.port)),
		Path:     "/",
		RawQuery: query.Encode(),
	}
	return requestURI.String()
}

func (fixture *SecureFixture) randomSecret(t *testing.T) string {
	t.Helper()
	material := make([]byte, 32)
	if _, err := rand.Read(material); err != nil {
		t.Fatal("cannot generate temporary MongoDB credential")
	}
	return base64.RawURLEncoding.EncodeToString(material)
}

func (fixture *SecureFixture) startMongo(t *testing.T) {
	t.Helper()
	projectDirectory := testutil.Root(t)
	binary := filepath.Join(projectDirectory, ".tools", "mongodb-macos-aarch64--8.0.32", "bin", "mongod")
	version := exec.CommandContext(fixture.startup, binary, "--version")
	versionOutput, err := version.Output()
	if err != nil || !strings.Contains(string(versionOutput), "v8.0.32") {
		t.Fatal("the pinned MongoDB 8.0.32 fixture binary is unavailable")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("cannot reserve MongoDB fixture port")
	}
	address := listener.Addr().(*net.TCPAddr)
	fixture.port = address.Port
	if err := listener.Close(); err != nil {
		t.Fatal("cannot release MongoDB fixture port")
	}
	dataDirectory := filepath.Join(fixture.root, "data")
	if err := os.Mkdir(dataDirectory, 0700); err != nil {
		t.Fatal("cannot create isolated MongoDB data directory")
	}
	arguments := []string{
		"--dbpath", dataDirectory,
		"--bind_ip", "127.0.0.1",
		"--port", strconv.Itoa(fixture.port),
		"--replSet", "weir_m10",
		"--auth",
		"--keyFile", filepath.Join(fixture.root, "keyfile"),
		"--tlsMode", "requireTLS",
		"--tlsCertificateKeyFile", filepath.Join(fixture.root, "server.pem"),
		"--tlsCAFile", fixture.caFile,
		"--tlsAllowConnectionsWithoutCertificates",
		"--setParameter", "enableTestCommands=1",
		"--logpath", filepath.Join(fixture.root, "mongod.log"),
	}
	fixture.process = exec.Command(binary, arguments...)
	fixture.processEnd = make(chan error, 1)
	if err := fixture.process.Start(); err != nil {
		t.Fatal("cannot start isolated MongoDB fixture")
	}
	go func() { fixture.processEnd <- fixture.process.Wait() }()
	unauthenticatedURI := fixture.uri("", "", "127.0.0.1", fixture.caFile)
	client := fixture.connect(t, unauthenticatedURI)
	fixture.initializeReplicaSet(t, client)
	fixture.createFirstAdmin(t, client)
	fixture.disconnect(t, client)
}

func (fixture *SecureFixture) connect(t *testing.T, uri string) *mongo.Client {
	t.Helper()
	options := options.Client().
		ApplyURI(uri).
		SetRetryReads(false).
		SetRetryWrites(false).
		SetMaxAdaptiveRetries(0).
		SetEnableOverloadRetargeting(false).
		SetMaxPoolSize(4).
		SetMaxConnecting(2).
		SetServerSelectionTimeout(2 * time.Second).
		SetConnectTimeout(2 * time.Second)
	if err := options.Validate(); err != nil {
		t.Fatal("secure fixture client options are invalid")
	}
	client, err := mongo.Connect(options)
	if err != nil {
		t.Fatal("cannot configure secure fixture client")
	}
	t.Cleanup(func() { fixture.disconnect(t, client) })
	deadline := time.Now().Add(15 * time.Second)
	var connectionError error
	for time.Now().Before(deadline) && fixture.startup.Err() == nil {
		ctx, cancel := context.WithTimeout(fixture.startup, time.Second)
		connectionError = client.Ping(ctx, nil)
		cancel()
		if connectionError == nil {
			return client
		}
		var commandError mongo.CommandError
		if errors.As(connectionError, &commandError) {
			fixture.disconnect(t, client)
			t.Fatalf("secure fixture MongoDB command failed (code=%d name=%s)", commandError.Code, commandError.Name)
		}
		time.Sleep(100 * time.Millisecond)
	}
	fixture.disconnect(t, client)
	t.Fatalf("secure fixture MongoDB connection failed (%T)", connectionError)
	return nil
}

func (fixture *SecureFixture) initializeReplicaSet(t *testing.T, client *mongo.Client) {
	t.Helper()
	configuration := bson.D{
		{Key: "_id", Value: "weir_m10"},
		{
			Key: "members",
			Value: bson.A{bson.D{
				{Key: "_id", Value: 0},
				{Key: "host", Value: net.JoinHostPort("127.0.0.1", strconv.Itoa(fixture.port))},
			}},
		},
	}
	command := bson.D{{Key: "replSetInitiate", Value: configuration}}
	if err := fixture.command(client, "admin", command); err != nil {
		t.Fatal("cannot initialize isolated MongoDB replica set")
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && fixture.startup.Err() == nil {
		ctx, cancel := context.WithTimeout(fixture.startup, time.Second)
		var hello struct {
			Writable bool `bson:"isWritablePrimary"`
		}
		command := bson.D{{Key: "hello", Value: 1}}
		err := client.Database("admin").RunCommand(ctx, command).Decode(&hello)
		cancel()
		if err == nil && hello.Writable {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("isolated MongoDB replica set did not become primary")
}

func (fixture *SecureFixture) createCollection(t *testing.T, client *mongo.Client) {
	t.Helper()
	command := bson.D{{Key: "create", Value: "records"}}
	if err := fixture.command(client, fixture.DB, command); err != nil {
		t.Fatal("cannot create isolated MongoDB collection")
	}
}

func (fixture *SecureFixture) createFirstAdmin(t *testing.T, client *mongo.Client) {
	t.Helper()
	password := fixture.randomSecret(t)
	roles := bson.A{bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}}}
	command := bson.D{{Key: "createUser", Value: "weir_admin"}, {Key: "pwd", Value: password}, {Key: "roles", Value: roles}}
	if err := fixture.command(client, "admin", command); err != nil {
		t.Fatal("cannot create temporary MongoDB fixture administrator")
	}
	fixture.adminURI = fixture.uri("weir_admin", password, "127.0.0.1", fixture.caFile)
}

func (fixture *SecureFixture) configureUsers(t *testing.T) {
	t.Helper()
	admin := fixture.connect(t, fixture.adminURI)
	fixture.createCollection(t, admin)
	role := bson.D{
		{Key: "role", Value: "weirApplication"},
		{Key: "privileges", Value: bson.A{
			bson.D{
				{Key: "resource", Value: bson.D{{Key: "db", Value: fixture.DB}, {Key: "collection", Value: "records"}}},
				{Key: "actions", Value: bson.A{"find", "insert", "update", "remove"}},
			},
			bson.D{
				{Key: "resource", Value: bson.D{{Key: "db", Value: fixture.DB}, {Key: "collection", Value: ""}}},
				{Key: "actions", Value: bson.A{"listCollections"}},
			},
		}},
		{Key: "roles", Value: bson.A{}},
	}
	createRole := bson.D{{Key: "createRole", Value: "weirApplication"}}
	for _, field := range role[1:] {
		createRole = append(createRole, field)
	}
	if err := fixture.command(admin, fixture.DB, createRole); err != nil {
		var commandError mongo.CommandError
		if errors.As(err, &commandError) {
			t.Fatalf("cannot create minimal MongoDB fixture role (code=%d name=%s)", commandError.Code, commandError.Name)
		}
		t.Fatalf("cannot create minimal MongoDB fixture role (%T)", err)
	}
	password := fixture.randomSecret(t)
	user := bson.D{
		{Key: "createUser", Value: "weir_app"},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "weirApplication"}, {Key: "db", Value: fixture.DB}}}},
	}
	if err := fixture.command(admin, "admin", user); err != nil {
		t.Fatal("cannot create temporary MongoDB fixture application user")
	}
	fixture.URI = fixture.uri("weir_app", password, "127.0.0.1", fixture.caFile)
	fixture.BadPassURI = fixture.uri("weir_app", fixture.randomSecret(t), "127.0.0.1", fixture.caFile)
	fixture.WrongHostURI = fixture.uri("weir_app", password, "localhost", fixture.caFile)
	fixture.BadCAURI = fixture.uri("weir_app", password, "127.0.0.1", filepath.Join(fixture.root, "untrusted-ca.pem"))
	fixture.MissingPassURI = fixture.uri("weir_app", "", "127.0.0.1", fixture.caFile)
	deniedPassword := fixture.randomSecret(t)
	deniedUser := bson.D{
		{Key: "createUser", Value: "weir_denied"},
		{Key: "pwd", Value: deniedPassword},
		{Key: "roles", Value: bson.A{}},
	}
	if err := fixture.command(admin, "admin", deniedUser); err != nil {
		t.Fatal("cannot create temporary MongoDB fixture negative user")
	}
	fixture.DeniedURI = fixture.uri("weir_denied", deniedPassword, "127.0.0.1", fixture.caFile)
	fixture.Admin = admin
}

func (fixture *SecureFixture) disconnect(t *testing.T, client *mongo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil && !errors.Is(err, mongo.ErrClientDisconnected) {
		t.Fatal("cannot close temporary MongoDB client")
	}
}

func (fixture *SecureFixture) cleanup(t *testing.T) {
	if fixture.Client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = fixture.Client.Disconnect(ctx)
		cancel()
	}
	if fixture.process != nil && fixture.process.Process != nil {
		_ = fixture.process.Process.Signal(syscall.SIGTERM)
		select {
		case <-fixture.processEnd:
		case <-time.After(5 * time.Second):
			_ = fixture.process.Process.Kill()
			select {
			case <-fixture.processEnd:
			case <-time.After(2 * time.Second):
				t.Error("owned mongod did not exit after kill")
				return
			}
		}
	}
	if fixture.logFile != nil {
		_ = fixture.logFile.Close()
	}
	if fixture.root == "" || !fixture.owned {
		return
	}
	marker := filepath.Join(fixture.root, ".weir-owner")
	contents, err := os.ReadFile(marker)
	if err != nil || string(contents) != "weir-milestone-10 mongodb-8.0.32 loopback TLS-SCRAM\n" {
		t.Error("MongoDB fixture ownership marker changed; preserving the directory")
		return
	}
	if err := os.RemoveAll(filepath.Join(fixture.root, "data")); err != nil {
		t.Error("cannot remove owned fixture data")
	}
	for _, name := range []string{"server.pem", "ca.pem", "untrusted-ca.pem", "keyfile"} {
		if err := os.Remove(filepath.Join(fixture.root, name)); err != nil && !os.IsNotExist(err) {
			t.Error("cannot remove owned temporary TLS material")
		}
	}
	t.Logf("MongoDB fixture log preserved at %s (port %d stopped)", filepath.Join(fixture.root, "mongod.log"), fixture.port)
}

func (fixture *SecureFixture) command(client *mongo.Client, database string, command bson.D) error {
	ctx, cancel := context.WithTimeout(fixture.startup, 2*time.Second)
	defer cancel()
	return client.Database(database).RunCommand(ctx, command).Err()
}
