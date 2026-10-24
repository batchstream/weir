package mongodb

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValidateMongoConfigProfiles(t *testing.T) {
	cfg := Config{Store: "mongo"}
	for _, uri := range []string{
		"mongodb://127.0.0.1:27028/?directConnection=true&serverMonitoringMode=poll",
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
}

func TestValidateMongoConfigRejectsOutsideProfile(t *testing.T) {
	base := Config{
		URI:   "mongodb://127.0.0.1:27028/",
		Store: "mongo",
	}
	var cases []Config
	for _, uri := range []string{
		"",
		"mongodb+srv://mongo.example.test/",
		"mongodb://127.0.0.1/",
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
		"mongodb://127.0.0.1:27028/?authMechanism=",
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
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=false",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsInsecure=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authMechanism=SCRAM-SHA-256&authSource=admin&tls=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&authsource=admin&tls=true",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=",
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=private-sentinel%0Apath",
		secure.URI + "&tlsCAFile=" + strings.Repeat("a", 2049),
	} {
		invalid := secure
		invalid.URI = uri
		cases = append(cases, invalid)
	}
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
	for _, field := range []string{"store"} {
		invalid := base
		switch field {
		case "store":
			invalid.Store = "invalid/private-sentinel"
		}
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
		URI:   "mongodb://unresolved.invalid:27028/?AUTHMechanism=SCRAM-SHA-256&AuthSource=admin&tls=true&directConnection=true",
		Store: "mongo",

		Username: " user:@/%?#用户 ",
		Password: " pass:@/%?#🔐 ",
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	dialer := newConnectionOwner()
	defer dialer.close()
	profile, err := parseConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := connectionOptions(cfg, dialer, profile)
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
	if opts.TLSConfig != nil || dialer.tlsConfig == nil || dialer.tlsConfig.InsecureSkipVerify || dialer.tlsConfig.ServerName != "" {
		t.Fatal("bounded dialer must retain verified TLS ownership")
	}
}

func TestMongoDriverUnauthenticatedOptions(t *testing.T) {
	cfg := Config{URI: "mongodb://127.0.0.1:27028/", Store: "mongo"}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	dialer := newConnectionOwner()
	defer dialer.close()
	profile, err := parseConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := connectionOptions(cfg, dialer, profile)
	if err != nil || opts.Auth != nil || opts.TLSConfig != nil || dialer.tlsConfig != nil {
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
		{"mongodb://ONE.example:027017,[2001:0db8::1]:27018,127.0.0.1:27019/?replicaSet=rs_1", []string{"one.example:27017", "[2001:db8::1]:27018", "127.0.0.1:27019"}, "rs_1", false},
		{"mongodb://[::1]:27017,[::ffff:127.0.0.1]:27018/?replicaSet=rs-1&directConnection=false", []string{"[::1]:27017", "127.0.0.1:27018"}, "rs-1", false},
		{"mongodb://localhost:27017/?replicaSet=rs", []string{"localhost:27017"}, "rs", false},
		{"mongodb://localhost:27017/", []string{"localhost:27017"}, "", true},
	}
	for _, tc := range cases {
		cfg := Config{URI: tc.uri, Store: "mongo"}
		profile, err := parseConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(profile.hosts, tc.hosts) || profile.replicaSet != tc.set || profile.direct != tc.direct {
			t.Fatal("profile mismatch", profile)
		}
		dialer := newConnectionOwner()
		opts, err := connectionOptions(cfg, dialer, profile)
		dialer.close()
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
	// Distinct seed names, not a seven-member or socket-count promise.
	cfg := Config{Store: "mongo", URI: "mongodb://a:1,b:2,c:3,d:4,e:5,f:6,g:7/?replicaSet=" + strings.Repeat("r", 64)}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestMongoReplicaProfileRejectsAmbiguity(t *testing.T) {
	for _, uri := range []string{
		"mongodb://a:1,b:2/?replicaSet=rs&directConnection=true",
		"mongodb://a:1/?replicaSet=",
		"mongodb://a:1/?replicaSet=rs&ReplicaSet=rs",
		"mongodb://a:1/?replicaSet=rs.name",
		"mongodb://a:1/?replicaSet=" + strings.Repeat("r", 65),
		"mongodb://a:1/?replicaSet=rs%0A",
		"mongodb://a:1/?replicaSet=rs&loadBalanced=true",
		"mongodb://a:1/?replicaSet=rs&readPreference=secondary",
		"mongodb://A:01,a:1/?replicaSet=rs",
		"mongodb://[::ffff:127.0.0.1]:1,127.0.0.1:01/?replicaSet=rs",
		"mongodb://[2001:db8::1]:1,[2001:0db8::1]:1/?replicaSet=rs",
		"mongodb://a:1,b:2,c:3,d:4,e:5,f:6,g:7,h:8/?replicaSet=rs",
		"mongodb://a:1,/?replicaSet=rs",
		"mongodb://[localhost]:1/?replicaSet=rs",
		"mongodb://[127.0.0.1]:1/?replicaSet=rs",
		"mongodb://[fe80::1%25lo0]:1/?replicaSet=rs",
		"mongodb://a%2eb:1/?replicaSet=rs",
		"mongodb://a:1?replicaSet=rs",
		"mongodb://a:1/#",
		"mongodb://a:1/path?replicaSet=rs",
		"mongodb://a:1/" + strings.Repeat("x", maxMongoURIBytes),
	} {
		cfg := Config{URI: uri, Store: "mongo", CAFile: "/missing/private-sentinel"}
		err := ValidateConfig(cfg)
		if err == nil || strings.Contains(err.Error(), "sentinel") {
			t.Fatal("invalid profile accepted or exposed", err)
		}
		cfg.CAFile = ""
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("invalid topology accepted")
		}
	}
}

func TestMongoURIValidationDoesNotReplaceConfigPolicy(t *testing.T) {
	uri := "mongodb://[::1]:27017,mongo.invalid:27018/?replicaSet=rs&authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
	if _, err := parseURI(uri); err != nil {
		t.Fatal("URI validation must not need credentials, trust files or DNS", err)
	}
	cfg := Config{Store: "mongo", URI: uri}
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("URI-only validation cannot authorize missing actual credentials")
	}
	cfg.Username, cfg.Password = "new-test-user", "new-test-password"
	cfg.CAFile = "/missing/new-test-ca.pem"
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal("complete configuration must validate without opening the CA", err)
	}
	cfg.Store = "INVALID"
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("URI validation cannot replace Store validation")
	}
	for _, query := range []string{"authSource=", "authMechanism=", "tls=", "authSource=admin", "authMechanism=SCRAM-SHA-256&authSource=admin", "tls=true", "authMechanism=SCRAM-SHA-256&authSource=&tls=true"} {
		if _, err := parseURI("mongodb://mongo.invalid:27017/?" + query); err == nil {
			t.Fatal("present empty or incomplete authentication options accepted")
		}
	}
}
