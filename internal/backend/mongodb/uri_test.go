package mongodb

import (
	"context"
	"net/url"
	"strings"
	"testing"
)

func TestValidateMongoConfigProfiles(t *testing.T) {
	cfg := Config{Store: "mongo", Database: "catalog", Collection: "records", Pool: 1}
	for _, uri := range []string{
		"mongodb://127.0.0.1:27028/?directConnection=true&serverMonitoringMode=poll",
		"mongodb://[::1]:27028/",
	} {
		cfg.URI = uri
		if err := ValidateConfig(cfg); err != nil {
			t.Fatal("supported unauthenticated profile rejected", err)
		}
	}

	cfg.URI = "mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=%2Fmissing%2Fca.pem"
	for _, pair := range []struct{ username, password string }{
		{" user:@/%?#用户 ", " pass:@/%?#🔐 "},
		{strings.Repeat("u", 128), strings.Repeat("p", 256)},
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
		URI:        "mongodb://127.0.0.1:27028/",
		Store:      "mongo",
		Database:   "catalog",
		Collection: "records",
		Pool:       1,
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
		strings.Repeat("a", maxMongoURIBytes+1),
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
		"mongodb://unresolved.invalid:27028/?authMechanism=SCRAM-SHA-256&authSource=invalid-name&tls=true",
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
		{strings.Repeat("u", 129), "password-private-sentinel"},
		{"user-private-sentinel", strings.Repeat("p", 257)},
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
	for _, field := range []string{"store", "database", "collection", "pool-zero", "pool-large"} {
		invalid := base
		switch field {
		case "store":
			invalid.Store = "invalid/private-sentinel"
		case "database":
			invalid.Database = "invalid-private-sentinel"
		case "collection":
			invalid.Collection = "invalid-private-sentinel"
		case "pool-zero":
			invalid.Pool = 0
		case "pool-large":
			invalid.Pool = 33
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
		URI:        "mongodb://unresolved.invalid:27028/?AUTHMechanism=SCRAM-SHA-256&AuthSource=admin&tls=true&directConnection=true",
		Store:      "mongo",
		Database:   "catalog",
		Collection: "records",
		Pool:       1,
		Username:   " user:@/%?#用户 ",
		Password:   " pass:@/%?#🔐 ",
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	dialer := newBoundedDialer(1, 1)
	defer dialer.close()
	opts, err := connectionOptions(cfg, dialer)
	if err != nil {
		t.Fatal("explicit driver authentication failed without backend access", err)
	}
	if opts.Auth == nil || opts.Auth.Username != cfg.Username || opts.Auth.Password != cfg.Password ||
		opts.Auth.AuthMechanism != "SCRAM-SHA-256" || opts.Auth.AuthSource != "admin" || !opts.Auth.PasswordSet {
		t.Fatal("driver authentication must preserve separate credential values and profile")
	}
	parsed, err := url.Parse(opts.GetURI())
	if err != nil || parsed.User != nil ||
		strings.Contains(opts.GetURI(), cfg.Username) || strings.Contains(opts.GetURI(), cfg.Password) {
		t.Fatal("driver URI must not contain credentials")
	}
	for key := range parsed.Query() {
		if strings.EqualFold(key, "authSource") || strings.EqualFold(key, "authMechanism") {
			t.Fatal("driver URI must not request authentication before SetAuth")
		}
	}
	if opts.TLSConfig != nil || dialer.tlsConfig == nil || dialer.tlsConfig.InsecureSkipVerify {
		t.Fatal("bounded dialer must retain verified TLS ownership")
	}
}

func TestMongoDriverUnauthenticatedOptions(t *testing.T) {
	cfg := Config{URI: "mongodb://127.0.0.1:27028/", Store: "mongo", Database: "catalog", Collection: "records", Pool: 1}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	dialer := newBoundedDialer(1, 1)
	defer dialer.close()
	opts, err := connectionOptions(cfg, dialer)
	if err != nil || opts.Auth != nil || opts.TLSConfig != nil || dialer.tlsConfig != nil {
		t.Fatal("unauthenticated driver profile must not inject credentials or TLS", err)
	}
}
