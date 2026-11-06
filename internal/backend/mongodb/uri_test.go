package mongodb

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValidateMongoConfigProfiles(t *testing.T) {
	cfg := Config{}
	for _, uri := range []string{
		"mongodb://127.0.0.1:27028/?directConnection=true",
		"mongodb://127.0.0.1/",
		"mongodb://[::1]:27028/",
	} {
		cfg.URI = uri
		if err := ValidateConfig(cfg); err != nil {
			t.Fatal("supported unauthenticated profile rejected", err)
		}
	}

	cfg.URI = "mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true"
	cfg.CAFile = "/missing/ca.pem"
	for _, pair := range []struct{ username, password string }{
		{" user:@/%?#用户 ", " pass:@/%?#🔐 "},
		{strings.Repeat("u", 8192), strings.Repeat("p", 16384)},
		{strings.Repeat("界", 42) + "uu", strings.Repeat("界", 85) + "p"},
	} {
		cfg.Username, cfg.Password = pair.username, pair.password
		if err := ValidateConfig(cfg); err != nil {
			t.Fatal("supported credential values rejected or static validation accessed CA/DNS", err)
		}
	}

	// SCRAM over an isolated plaintext network must say so with tls=false.
	// Option names are case-insensitive, as elsewhere in the URI.
	for _, uri := range []string{
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=false",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&TLS=false",
	} {
		plaintext := Config{URI: uri, Username: "user", Password: "password"}
		if err := ValidateConfig(plaintext); err != nil {
			t.Fatal("explicit tls=false SCRAM rejected", uri, err)
		}
	}
}

func TestValidateMongoConfigRejectsOutsideProfile(t *testing.T) {
	base := Config{
		URI: "mongodb://127.0.0.1:27028/",
	}
	var cases []Config
	for _, uri := range []string{
		"",
		"mongodb+srv://mongo.example.test/",
		"mongodb://127.0.0.1:0/",
		"mongodb://127.0.0.1:65536/",
		"mongodb://127.0.0.1:27028,127.0.0.2:27028/",
		"mongodb://127.0.0.1:27028/catalog/",
		"mongodb://127.0.0.1:27028/#private-sentinel",
		" mongodb://127.0.0.1:27028/",
		"mongodb://user:private-sentinel@127.0.0.1:27028/",
		"mongodb://@127.0.0.1:27028/",
		"mongodb://127.0.0.1:27028/?directConnection=false",
		"mongodb://127.0.0.1:27028/?serverMonitoringMode=stream",
		"mongodb://127.0.0.1:27028/?retryWrites=true",
		"mongodb://127.0.0.1:27028/?authSource=admin",
		"mongodb://127.0.0.1:27028/?authMechanism=SCRAM-SHA-256",
		"mongodb://127.0.0.1:27028/?tls=true",
		"mongodb://127.0.0.1:27028/?tlsCAFile=/missing/ca-private-sentinel.pem",
		"mongodb://127.0.0.1:27028/?authSource=",
		"mongodb://127.0.0.1:27028/?tls=",
		"mongodb://127.0.0.1:27028/?tlsCAFile=",
	} {
		invalid := base
		invalid.URI = uri
		cases = append(cases, invalid)
	}

	secure := base
	secure.URI = "mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true"
	secure.Username, secure.Password = "user-private-sentinel", "password-private-sentinel"
	for _, uri := range []string{
		"mongodb://user:private-sentinel@unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true",
		"mongodb://unresolved.invalid:27028/",
		"mongodb://unresolved.invalid:27028/?authSource=admin&tls=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=MONGODB-OIDC&authSource=admin&tls=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&tls=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=invalid.name&tls=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsInsecure=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=FALSE",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=0",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tls=false",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=false&tls=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&ssl=false",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=false&tlsInsecure=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=private-sentinel%0Apath",
		secure.URI + "&tlsCAFile=" + strings.Repeat("a", 2049),
	} {
		invalid := secure
		invalid.URI = uri
		cases = append(cases, invalid)
	}
	// A CA file cannot apply without TLS, and tls=false needs credentials.
	plaintext := secure
	plaintext.URI = "mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=false"
	plaintext.CAFile = "/missing/ca-private-sentinel.pem"
	cases = append(cases, plaintext)
	anonymous := base
	anonymous.URI = "mongodb://127.0.0.1:27028/?tls=false"
	cases = append(cases, anonymous)
	for _, pair := range []struct{ username, password string }{
		{"", "password-private-sentinel"},
		{"user-private-sentinel", ""},
		{"user\x00private-sentinel", "password"},
		{"user", "password\nprivate-sentinel"},
		{"user\x7fprivate-sentinel", "password"},
		{"user", "password\u0085private-sentinel"},
		{"user\xffprivate-sentinel", "password"},
		{"user", "password\xffprivate-sentinel"},
	} {
		invalid := secure
		invalid.Username, invalid.Password = pair.username, pair.password
		cases = append(cases, invalid)
	}
	for i, cfg := range cases {
		err := ValidateConfig(cfg)
		if err == nil || strings.Contains(err.Error(), "sentinel") {
			t.Fatal("unsupported configuration accepted or details leaked", i, err)
		}
		adapter, openErr := Open(context.Background(), cfg)
		if adapter != nil || openErr == nil || openErr.Error() != err.Error() {
			t.Fatal("Open must validate the same configuration before startup IO", i, openErr)
		}
	}
}

func TestMongoDriverAuthenticationOptions(t *testing.T) {
	cfg := Config{
		URI:      "mongodb://unresolved.invalid:27028/?AUTHMechanism=SCRAM-SHA-256&AuthSource=admin&tls=true&directConnection=true",
		Username: " user:@/%?#用户 ",
		Password: " pass:@/%?#🔐 ",
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	uri, err := parseConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := connectionOptions(cfg, uri)
	if err != nil {
		t.Fatal("explicit driver authentication failed without backend access", err)
	}
	if opts.Auth == nil || opts.Auth.Username != cfg.Username || opts.Auth.Password != cfg.Password ||
		opts.Auth.AuthMechanism != "SCRAM-SHA-256" || opts.Auth.AuthSource != "admin" || !opts.Auth.PasswordSet {
		t.Fatal("driver authentication must preserve separate credential values and profile")
	}
	if opts.GetURI() != "" {
		t.Fatal("explicit driver options must not retain or reparse the raw URI")
	}
	if opts.TLSConfig == nil || opts.TLSConfig.InsecureSkipVerify || opts.TLSConfig.ServerName != "" {
		t.Fatal("driver must verify each member's own certificate")
	}
}

// tls=false keeps SCRAM authentication and configures no TLS at all.
func TestMongoDriverPlaintextAuthenticationOptions(t *testing.T) {
	cfg := Config{
		URI:      "mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=false&directConnection=true",
		Username: "user",
		Password: "password",
	}
	uri, err := parseConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := connectionOptions(cfg, uri)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Auth == nil || opts.Auth.AuthMechanism != "SCRAM-SHA-256" || opts.Auth.AuthSource != "admin" || !opts.Auth.PasswordSet {
		t.Fatal("tls=false must keep SCRAM authentication")
	}
	if opts.TLSConfig != nil {
		t.Fatal("tls=false must not configure TLS")
	}
}

func TestMongoDriverUnauthenticatedOptions(t *testing.T) {
	cfg := Config{URI: "mongodb://127.0.0.1:27028/"}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	uri, err := parseConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := connectionOptions(cfg, uri)
	if err != nil || opts.Auth != nil || opts.TLSConfig != nil {
		t.Fatal("unauthenticated driver profile must not inject credentials or TLS", err)
	}
}

func TestMongoReplicaProfiles(t *testing.T) {
	cases := []struct {
		uri    string
		hosts  []string
		set    string
		direct bool
	}{
		{"mongodb://one.example:27017,[2001:db8::1]:27018,127.0.0.1:27019/?replicaSet=rs_1", []string{"one.example:27017", "[2001:db8::1]:27018", "127.0.0.1:27019"}, "rs_1", false},
		{"mongodb://[::1]:27017,127.0.0.1:27018/?replicaSet=rs-1&directConnection=false", []string{"[::1]:27017", "127.0.0.1:27018"}, "rs-1", false},
		{"mongodb://localhost:27017/?replicaSet=rs", []string{"localhost:27017"}, "rs", false},
		{"mongodb://localhost:27017/", []string{"localhost:27017"}, "", true},
	}
	for _, tc := range cases {
		cfg := Config{URI: tc.uri}
		uri, err := parseConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		opts, err := connectionOptions(cfg, uri)
		if err != nil || !reflect.DeepEqual(opts.Hosts, tc.hosts) || opts.Direct == nil || *opts.Direct != tc.direct {
			t.Fatal("driver profile mismatch", err)
		}
		if tc.set != "" && (opts.ReplicaSet == nil || *opts.ReplicaSet != tc.set) {
			t.Fatal("missing driver set name")
		}
		if tc.set != "" && (opts.HeartbeatInterval == nil || *opts.HeartbeatInterval != time.Second) {
			t.Fatal("replica discovery requires bounded polling")
		}
		if tc.direct && opts.HeartbeatInterval != nil {
			t.Fatal("direct profile must retain the driver heartbeat default")
		}
		if err := opts.Validate(); err != nil {
			t.Fatal("driver rejected profile", err)
		}
	}
}

func TestMongoReplicaProfileRejectsAmbiguity(t *testing.T) {
	for _, uri := range []string{
		"mongodb://a:1,b:2/?replicaSet=rs&directConnection=true",
		"mongodb://a:1/?replicaSet=",
		"mongodb://a:1/?replicaSet=rs&loadBalanced=true",
		"mongodb://a:1/?replicaSet=rs&readPreference=secondary",
		"mongodb://a:1?replicaSet=rs",
		"mongodb://a:1/#",
		"mongodb://a:1/path?replicaSet=rs",
		"mongodb://a:1/" + strings.Repeat("x", 4096),
	} {
		cfg := Config{URI: uri, CAFile: "/missing/private-sentinel"}
		err := ValidateConfig(cfg)
		if err == nil || strings.Contains(err.Error(), "sentinel") {
			t.Fatal("invalid profile accepted or exposed", err)
		}
		cfg.CAFile = ""
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("invalid topology accepted", uri)
		}
	}
}

func TestMongoURIValidationDoesNotReplaceConfigPolicy(t *testing.T) {
	uri := "mongodb://[::1]:27017,mongo.invalid:27018/?replicaSet=rs&authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
	if _, err := parseURI(uri); err != nil {
		t.Fatal("URI validation must not need credentials, trust files or DNS", err)
	}
	cfg := Config{URI: uri}
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("URI-only validation cannot authorize missing actual credentials")
	}
	cfg.Username, cfg.Password = "new-test-user", "new-test-password"
	cfg.CAFile = "/missing/new-test-ca.pem"
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal("complete configuration must validate without opening the CA", err)
	}
	for _, query := range []string{"authSource=", "tls=", "authSource=admin", "authMechanism=SCRAM-SHA-256&authSource=admin", "tls=true", "authMechanism=SCRAM-SHA-256&authSource=&tls=true"} {
		if _, err := parseURI("mongodb://mongo.invalid:27017/?" + query); err == nil {
			t.Fatal("present empty or incomplete authentication options accepted", query)
		}
	}
}
