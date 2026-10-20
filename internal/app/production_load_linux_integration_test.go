//go:build integration

package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testexecution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func nativeLoadInputs(t *testing.T) *testexecution.ValidatedInputs {
	t.Helper()
	if os.Getenv("WEIR_NATIVE_LOAD") != "1" {
		t.Skip("requires owned native Linux formal load controller")
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Fatal("formal load requires native Linux amd64")
	}
	suite := "load"
	if os.Getenv("WEIR_NATIVE_FAULT") == "1" {
		suite = "fault"
	}
	inputs := loadProductionExecution(t, suite)
	if suite == "load" && !inputs.Profile().NativePlainLoad() && !inputs.Profile().NativeTLSLoad() || suite == "fault" && !inputs.Profile().NativePlainFault() && !inputs.Profile().NativeTLSFault() {
		t.Fatal("only exact native formal backend transport cells are implemented")
	}
	if (os.Getenv("WEIR_NATIVE_TLS") != "") != (inputs.Profile().Backends[0].Transport == "tls") {
		t.Fatal("native fixture transport disagrees with validated profile")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, inputs.CLIPath(), "version").Output()
	if err != nil {
		t.Fatal("native CLI version cross-check", err)
	}
	if err := inputs.CheckVersion(raw); err != nil {
		t.Fatal(err)
	}
	return inputs
}

func nativeLoadRecord(t *testing.T, record any) {
	t.Helper()
	if t.Failed() || t.Skipped() {
		return
	}
	file, err := os.OpenFile(os.Getenv("WEIR_NATIVE_LOAD_COMPLETION"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewEncoder(file).Encode(record)
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		t.Fatal("native load completion", err, closeErr)
	}
}

func nativeLoadBase(inputs *testexecution.ValidatedInputs, stage string) finiteCompletion {
	record := finiteCompletion{Schema: 1, Stage: stage, Profile: inputs.Profile().ID, Revision: inputs.HarnessSource().Revision, DriverPID: os.Getpid()}
	return record
}

func TestProductionLinuxTLS(t *testing.T) {
	inputs := nativeLoadInputs(t)
	directory := os.Getenv("WEIR_NATIVE_TLS")
	if directory == "" {
		t.Fatal("new controller-owned TLS directory required")
	}
	opts := testsearch.NativeTLSOptions{Directory: directory, Owner: os.Getenv("WEIR_NATIVE_LOAD_OWNER"), Product: inputs.Profile().Backends[1].Product}
	testsearch.PrepareNativeTLS(t, opts)
	record := nativeLoadBase(inputs, "tls")
	record.Cases = []string{"exact-loopback-certificates", "stock-security-configuration"}
	nativeLoadRecord(t, record)
}

func TestProductionLinuxLoadPreflight(t *testing.T) {
	inputs := nativeLoadInputs(t)
	record := nativeLoadBase(inputs, "preflight")
	record.Cases = []string{"source", "cli", "kernel-driver"}
	nativeLoadRecord(t, record)
}

type nativeMongoOracleConfig struct {
	URI        string
	Initialize bool
	TLSConfig  *tls.Config
	Credential options.Credential
}

// The original fixture/CLI/proxy URI stays credential-free and unchanged. Only
// the independent oracle's driver input omits URI authentication options: ApplyURI
// validates SCRAM usernames before separate SetAuth credentials can be supplied.
func nativeMongoOracleOptions(cfg nativeMongoOracleConfig) (*options.ClientOptions, error) {
	parsed, err := url.Parse(cfg.URI)
	if err != nil || parsed.User != nil {
		return nil, errors.New("invalid owned Mongo oracle URI")
	}
	query := parsed.Query()
	if cfg.TLSConfig != nil {
		if query.Get("authMechanism") != "SCRAM-SHA-256" || query.Get("authSource") != "admin" || query.Get("tls") != "true" {
			return nil, errors.New("complete owned SCRAM/TLS oracle profile required")
		}
		if !cfg.Initialize && (cfg.Credential.AuthMechanism != "SCRAM-SHA-256" || cfg.Credential.AuthSource != "admin" || cfg.Credential.Username != "weir_native" || cfg.Credential.Password == "" || !cfg.Credential.PasswordSet) {
			return nil, errors.New("explicit owned SCRAM oracle credentials required")
		}
		query.Del("authMechanism")
		query.Del("authSource")
		parsed.RawQuery = query.Encode()
	} else if query.Has("authMechanism") || query.Has("authSource") || query.Has("tls") {
		return nil, errors.New("verified TLS oracle configuration required")
	}
	connectionURI := parsed.String()
	if cfg.Initialize {
		connectionURI = "mongodb://" + parsed.Host + "/"
	}
	opts := options.Client().ApplyURI(connectionURI).SetDirect(cfg.Initialize).SetRetryReads(false).SetRetryWrites(false).SetMaxAdaptiveRetries(0).SetMaxPoolSize(8).SetServerSelectionTimeout(time.Second)
	if cfg.TLSConfig != nil {
		opts.SetTLSConfig(cfg.TLSConfig)
		if !cfg.Initialize {
			opts.SetAuth(cfg.Credential)
		}
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}

func TestExecutionProfileContractNativeMongoOracleOptions(t *testing.T) {
	owner := "weir-finite-" + strings.Repeat("a", 32)
	plainURI := "mongodb://127.0.0.1:27017/?replicaSet=" + owner
	tlsURI := plainURI + "&authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
	roots := x509.NewCertPool()
	trust := &tls.Config{RootCAs: roots, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12}
	credential := options.Credential{AuthMechanism: "SCRAM-SHA-256", AuthSource: "admin", Username: "weir_native", Password: strings.Repeat("a", 48), PasswordSet: true}
	for _, secure := range []bool{false, true} {
		for _, initialize := range []bool{false, true} {
			cfg := nativeMongoOracleConfig{URI: plainURI, Initialize: initialize}
			if secure {
				cfg.URI, cfg.TLSConfig, cfg.Credential = tlsURI, trust, credential
			}
			original := cfg.URI
			opts, err := nativeMongoOracleOptions(cfg)
			if err != nil || opts.Validate() != nil {
				t.Fatal("oracle options failed backend-free validation", secure, initialize, err)
			}
			parsed, err := url.Parse(opts.GetURI())
			if err != nil || parsed.User != nil || parsed.Query().Has("authSource") || parsed.Query().Has("authMechanism") || cfg.URI != original {
				t.Fatal("oracle URI handling leaked credentials or changed original fixture URI")
			}
			if len(opts.Hosts) != 1 || opts.Hosts[0] != "127.0.0.1:27017" || opts.Direct == nil || *opts.Direct != initialize {
				t.Fatal("oracle endpoint/direct topology changed")
			}
			if initialize {
				if opts.Auth != nil || opts.ReplicaSet != nil || parsed.RawQuery != "" {
					t.Fatal("localhost-exception bootstrap attempted authentication/discovery")
				}
			} else if opts.ReplicaSet == nil || *opts.ReplicaSet != owner {
				t.Fatal("oracle lost the exact replica-set identity")
			}
			if secure {
				if opts.TLSConfig != trust || opts.TLSConfig.RootCAs != roots || opts.TLSConfig.ServerName != "127.0.0.1" {
					t.Fatal("oracle lost dedicated verified TLS configuration")
				}
				if !initialize && (opts.Auth == nil || opts.Auth.Username != credential.Username || opts.Auth.Password != credential.Password || !opts.Auth.PasswordSet || opts.Auth.AuthSource != "admin" || opts.Auth.AuthMechanism != "SCRAM-SHA-256") {
					t.Fatal("oracle lost separate explicit SCRAM credentials")
				}
			} else if opts.TLSConfig != nil || opts.Auth != nil {
				t.Fatal("plain oracle transport changed")
			}
		}
	}
	for _, initialize := range []bool{false, true} {
		for _, field := range []string{"authMechanism", "authSource", "tls"} {
			for _, value := range []string{"", "wrong"} {
				parsed, err := url.Parse(tlsURI)
				if err != nil {
					t.Fatal(err)
				}
				query := parsed.Query()
				if value == "" {
					query.Del(field)
				} else {
					query.Set(field, value)
				}
				parsed.RawQuery = query.Encode()
				cfg := nativeMongoOracleConfig{URI: parsed.String(), Initialize: initialize, TLSConfig: trust, Credential: credential}
				if _, err := nativeMongoOracleOptions(cfg); err == nil {
					t.Fatal("wrong/missing original SCRAM/TLS field accepted", field, initialize)
				}
			}
		}
	}
	for _, field := range []string{"username", "password", "password-set", "source", "mechanism", "trust"} {
		cfg := nativeMongoOracleConfig{URI: tlsURI, TLSConfig: trust, Credential: credential}
		switch field {
		case "username":
			cfg.Credential.Username = ""
		case "password":
			cfg.Credential.Password = ""
		case "password-set":
			cfg.Credential.PasswordSet = false
		case "source":
			cfg.Credential.AuthSource = "wrong"
		case "mechanism":
			cfg.Credential.AuthMechanism = "wrong"
		case "trust":
			cfg.TLSConfig = nil
		}
		if _, err := nativeMongoOracleOptions(cfg); err == nil {
			t.Fatal("missing/wrong separate oracle configuration accepted", field)
		}
	}
}

// The controller creates and owns this stock dependency process. This driver
// initializes its single-member replica set and independently checks its identity.
func nativeLoadMongo(t *testing.T, initialize bool) *testmongo.Fixture {
	t.Helper()
	uri, set := os.Getenv("WEIR_NATIVE_LOAD_MONGO"), os.Getenv("WEIR_NATIVE_LOAD_OWNER")
	parsed, err := url.Parse(uri)
	expectedQuery := "replicaSet=" + set
	if os.Getenv("WEIR_NATIVE_TLS") != "" {
		expectedQuery += "&authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
	}
	if err != nil || parsed.Scheme != "mongodb" || parsed.User != nil || parsed.Path != "/" || parsed.RawQuery != expectedQuery || !regexp.MustCompile(`^weir-finite-[0-9a-f]{32}$`).MatchString(set) {
		t.Fatal("invalid owned native Mongo endpoint")
	}
	if err := finiteLoopback(parsed.Host); err != nil {
		t.Fatal(err)
	}
	oracle := nativeMongoOracleConfig{URI: uri, Initialize: initialize}
	if directory := os.Getenv("WEIR_NATIVE_TLS"); directory != "" {
		_, clientTLS, material := testsearch.NativeTLS(t, directory)
		oracle.TLSConfig = clientTLS
		if !initialize {
			credential := options.Credential{AuthMechanism: "SCRAM-SHA-256", AuthSource: "admin", Username: material.MongoUsername, Password: material.MongoPassword, PasswordSet: true}
			oracle.Credential = credential
		}
	}
	opts, err := nativeMongoOracleOptions(oracle)
	if err != nil {
		t.Fatal("invalid owned native Mongo oracle options", err)
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Disconnect(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if initialize {
		member := bson.D{{Key: "_id", Value: 0}, {Key: "host", Value: parsed.Host}}
		configuration := bson.D{{Key: "_id", Value: set}, {Key: "members", Value: bson.A{member}}}
		command := bson.D{{Key: "replSetInitiate", Value: configuration}}
		if err := client.Database("admin").RunCommand(ctx, command).Err(); err != nil {
			t.Fatal("owned replica initialization", err)
		}
	}
	command := bson.D{{Key: "hello", Value: 1}}
	for {
		var hello struct {
			Set     string   `bson:"setName"`
			Primary bool     `bson:"isWritablePrimary"`
			Hosts   []string `bson:"hosts"`
		}
		err := client.Database("admin").RunCommand(ctx, command).Decode(&hello)
		if err == nil && hello.Primary && hello.Set == set && len(hello.Hosts) == 1 && hello.Hosts[0] == parsed.Host {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("owned replica identity/readiness", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	fixture := &testmongo.Fixture{Admin: client, DB: strings.ReplaceAll(set, "-", "_"), URI: uri}
	if directory := os.Getenv("WEIR_NATIVE_TLS"); directory != "" {
		server, upstream, _ := testsearch.NativeTLS(t, directory)
		fixture.SetOwnedProxyTLS(server, upstream)
	}
	verifyLegacyMongoVersion(t, fixture)
	if initialize && os.Getenv("WEIR_NATIVE_TLS") != "" {
		_, _, material := testsearch.NativeTLS(t, os.Getenv("WEIR_NATIVE_TLS"))
		// The stock localhost exception bootstraps exactly this fresh replica.
		role := bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}}
		create := bson.D{{Key: "createUser", Value: material.MongoUsername}, {Key: "pwd", Value: material.MongoPassword}, {Key: "roles", Value: bson.A{role}}, {Key: "mechanisms", Value: bson.A{"SCRAM-SHA-256"}}}
		if err := client.Database("admin").RunCommand(ctx, create).Err(); err != nil {
			t.Fatal("owned native SCRAM bootstrap failed")
		}
	}
	return fixture
}

func TestProductionLinuxLoadMongo(t *testing.T) {
	inputs := nativeLoadInputs(t)
	nativeLoadMongo(t, true)
	record := nativeLoadBase(inputs, "mongo")
	record.Cases = []string{"replica-init", "version", "advertised-loopback"}
	nativeLoadRecord(t, record)
}

type nativeLoadCompletion struct {
	finiteCompletion
	WarmupSeconds      int `json:"warmup_seconds"`
	MeasurementSeconds int `json:"measurement_seconds"`
	ArrivalRPC         int `json:"arrival_rpc"`
}

func TestProductionLinuxLoad(t *testing.T) {
	inputs := nativeLoadInputs(t)
	runNativeProductionLoad(t, inputs)
}

func runNativeProductionLoad(t *testing.T, inputs *testexecution.ValidatedInputs) {
	t.Helper()
	address, diagnostics := os.Getenv("WEIR_NATIVE_LOAD_APP"), os.Getenv("WEIR_NATIVE_LOAD_DIAGNOSTICS")
	for _, endpoint := range []string{address, diagnostics} {
		if err := finiteLoopback(endpoint); err != nil {
			t.Fatal(err)
		}
	}
	base := nativeLoadBase(inputs, "load")
	record := nativeLoadCompletion{finiteCompletion: base, WarmupSeconds: 30, MeasurementSeconds: 600, ArrivalRPC: 200}
	// Registered first, so database/index and SDK cleanup precede completion.
	t.Cleanup(func() { nativeLoadRecord(t, record) })
	fixture := nativeLoadMongo(t, false)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := fixture.Admin.Database(fixture.DB).Drop(ctx); err != nil {
			t.Error(err)
		}
	})
	owned := testsearch.OwnedEndpoint{URL: os.Getenv("WEIR_NATIVE_LOAD_ORACLE"), Owner: os.Getenv("WEIR_NATIVE_LOAD_OWNER"), Product: inputs.Profile().Backends[1].Product, TLSDirectory: os.Getenv("WEIR_NATIVE_TLS")}
	search := testsearch.OpenOwned(t, owned)
	datasets := seedProductionQualificationDatasets(t, fixture, search)
	process := &memoryRecoveryCLI{address: address, diagnostic: diagnostics, capacity: 1 << 30, automatic: true, stores: 2}
	runProductionQualification(t, process, datasets, inputs.Profile().Duration())
	for _, dataset := range datasets {
		record.Documents += len(dataset.expected)
	}
	record.Cases = []string{"Resolve", "fixed-arrival", "slow-consumers", "burst", "latency", "memory", "ownership-zero", "oracle"}
	if err := inputs.RecheckCLI(); err != nil {
		t.Fatal(err)
	}
	nativeLoadInputs(t)
}
