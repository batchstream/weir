package app

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/backend/mongodb"
	"go.yaml.in/yaml/v3"
)

func credentialTestConfig(t *testing.T, backend string) Config {
	t.Helper()
	local := &Local{}
	credentials := Credentials{
		Username: "user-secret-sentinel",
		Password: " password-secret-sentinel:@/%?汉 ",
	}
	local.Backend.Authentication = &credentials
	if backend == "mongodb" {
		local.Backend.MongoDB = &Mongo{URI: "mongodb://unresolved.invalid:27017/?authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"}
	} else {
		local.Backend.TLS = &BackendTLS{CAFile: "/missing/ca-secret-sentinel.pem"}
		local.Backend.Search = &Search{URL: "https://unresolved.invalid:9200"}
	}
	service := StoreConfig{Name: backend, Local: local}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{service}

	return cfg
}

func writeCredentialTestDocuments(t *testing.T, cfg Config) (string, string) {
	t.Helper()
	root := t.TempDir()
	basicDirectory := filepath.Join(root, "basic")
	routingDirectory := filepath.Join(root, "routing")
	for _, directory := range []string{basicDirectory, routingDirectory} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	basic, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	basicFilename := filepath.Join(basicDirectory, "weir.yaml")
	routingFilename := filepath.Join(routingDirectory, "routes.yaml")
	if err := os.WriteFile(basicFilename, basic, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(routingFilename, routing, 0600); err != nil {
		t.Fatal(err)
	}
	return basicFilename, routingFilename
}

func TestCredentialConfigurationUsesSharedAuthentication(t *testing.T) {
	for _, backend := range []string{"mongodb", "search"} {
		t.Run(backend, func(t *testing.T) {
			cfg := credentialTestConfig(t, backend)
			local := cfg.Routing.Stores[0].Local
			var source any = local.Backend.Authentication
			expected := map[string]any{
				"username":      "user-secret-sentinel",
				"password":      " password-secret-sentinel:@/%?汉 ",
				"username_file": "",
				"password_file": "",
			}
			jsonRaw, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			yamlRaw, err := yaml.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			for _, raw := range [][]byte{jsonRaw, yamlRaw} {
				var fields map[string]any
				if err := yaml.Unmarshal(raw, &fields); err != nil || !reflect.DeepEqual(fields, expected) {
					t.Fatal("serialized backend changed its credential field layout", err)
				}
			}
		})
	}
}

func TestLoadCredentialSources(t *testing.T) {
	for _, backend := range []string{"mongodb", "search"} {
		for _, mode := range []string{"inline", "username-file", "password-file", "both-files"} {
			for _, pathMode := range []string{"relative", "absolute", "symlink"} {
				if mode == "inline" && pathMode != "relative" {
					continue
				}
				t.Run(backend+"/"+mode+"/"+pathMode, func(t *testing.T) {
					cfg := credentialTestConfig(t, backend)
					fields := cfg.Routing.Stores[0].Local.credentials()
					username, password := fields.Username, fields.Password
					if mode == "username-file" || mode == "both-files" {
						fields.Username, fields.UsernameFile = "", "username.txt"
					}
					if mode == "password-file" || mode == "both-files" {
						fields.Password, fields.PasswordFile = "", "password.txt"
					}
					if pathMode == "absolute" {
						for _, filename := range []*string{&fields.UsernameFile, &fields.PasswordFile} {
							if *filename != "" {
								*filename = filepath.Join(t.TempDir(), *filename)
							}
						}
					}
					basicFilename, routingFilename := writeCredentialTestDocuments(t, cfg)
					for _, input := range [][2]string{
						{fields.UsernameFile, username + "\n"},
						{fields.PasswordFile, password + "\r\n"},
					} {
						filename := input[0]
						if filename == "" {
							continue
						}
						if !filepath.IsAbs(filename) {
							filename = filepath.Join(filepath.Dir(routingFilename), filename)
							decoy := filepath.Join(filepath.Dir(basicFilename), input[0])
							if err := os.WriteFile(decoy, []byte("invalid\x00"), 0600); err != nil {
								t.Fatal(err)
							}
						}
						if pathMode == "symlink" {
							target := filepath.Join(t.TempDir(), filepath.Base(filename))
							if err := os.Symlink(target, filename); err != nil {
								t.Fatal(err)
							}
							filename = target
						}
						if err := os.WriteFile(filename, []byte(input[1]), 0600); err != nil {
							t.Fatal(err)
						}
					}
					loaded, err := Load(basicFilename, routingFilename)
					if err != nil {
						t.Fatal("all inline/file combinations must load without backend, DNS or CA-file IO", err)
					}
					resolved := loaded.Routing.Stores[0].Local.credentials()
					if resolved.Username != username || resolved.Password != password || resolved.UsernameFile != "" || resolved.PasswordFile != "" {
						t.Fatal("credential resolution changed values or retained a file reference")
					}
					if err := loaded.Validate(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestCredentialFileTextPreservesValues(t *testing.T) {
	for _, limit := range []int{128, 4096} {
		cases := []struct {
			name  string
			input string
			want  string
		}{
			{"plain", "value", "value"},
			{"lf", "value\n", "value"},
			{"crlf", "value\r\n", "value"},
			{"spaces", " value \r\n", " value "},
			{"only-spaces", "  \n", "  "},
			{"utf8", "口令\n", "口令"},
			{"exact-bound", strings.Repeat("a", limit), strings.Repeat("a", limit)},
			{"exact-bound-lf", strings.Repeat("a", limit) + "\n", strings.Repeat("a", limit)},
			{"exact-bound-crlf", strings.Repeat("a", limit) + "\r\n", strings.Repeat("a", limit)},
			{"exact-utf8-bound", strings.Repeat("é", limit/2) + "\r\n", strings.Repeat("é", limit/2)},
			{"empty", "", ""},
			{"only-lf", "\n", ""},
			{"only-crlf", "\r\n", ""},
			{"bare-cr", "value\r", ""},
			{"multiple-lf", "value\n\n", ""},
			{"multiple-crlf", "value\r\n\r\n", ""},
			{"internal-lf", "val\nue", ""},
			{"null-byte", "val\x00ue", ""},
			{"tab", "val\tue", ""},
			{"del", "val\x7fue", ""},
			{"unicode-control", "val\u0085ue", ""},
			{"invalid-utf8", "val\xffue", ""},
			{"long-ascii", strings.Repeat("a", limit+1), strings.Repeat("a", limit+1)},
			{"long-utf8", strings.Repeat("é", limit/2) + "a\r\n", strings.Repeat("é", limit/2) + "a"},
			{"large-file", strings.Repeat("a", 1<<20), strings.Repeat("a", 1<<20)},
		}
		for _, tc := range cases {
			t.Run(tc.name+"/"+strconv.Itoa(limit), func(t *testing.T) {
				filename := filepath.Join(t.TempDir(), "input.txt")
				if err := os.WriteFile(filename, []byte(tc.input), 0600); err != nil {
					t.Fatal(err)
				}
				value, err := resolveCredentialValue("", filename, "")
				if tc.want == "" {
					if err == nil || err.Error() != "invalid credential file content" {
						t.Fatal("invalid text must be rejected without exposing its value or path", err)
					}
				} else if err != nil || value != tc.want {
					t.Fatal("valid text lost bytes or failed its inclusive size bound", err)
				}
			})
		}
	}
}

func TestLoadLargeCredentialsFromTemporaryFiles(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	directory := t.TempDir()
	for range 2 {
		directory = filepath.Join(directory, strings.Repeat("d", 210))
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	username := strings.Repeat("u", 8192)
	password := strings.Repeat("p", 1<<20)
	fields := cfg.Routing.Stores[0].Local.credentials()
	fields.Username, fields.Password = "", ""
	fields.UsernameFile = filepath.Join(directory, "username.txt")
	fields.PasswordFile = filepath.Join(directory, "password.txt")
	if err := os.WriteFile(fields.UsernameFile, []byte(username+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fields.PasswordFile, []byte(password+"\r\n"), 0600); err != nil {
		t.Fatal(err)
	}
	basicFile, routingFile := writeCredentialTestDocuments(t, cfg)
	loaded, err := Load(basicFile, routingFile)
	if err != nil {
		t.Fatal("large generated credentials retained an artificial path/text threshold", err)
	}
	resolved := loaded.Routing.Stores[0].Local.credentials()
	if resolved.Username != username || resolved.Password != password || resolved.UsernameFile != "" || resolved.PasswordFile != "" {
		t.Fatal("large credential values were changed or file sources not cleared")
	}
}

func TestCredentialFilesUnavailable(t *testing.T) {
	for _, mode := range []string{"missing", "directory", "dangling-symlink", "directory-symlink"} {
		t.Run(mode, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "secret-sentinel.txt")
			switch mode {
			case "directory":
				if err := os.Mkdir(filename, 0700); err != nil {
					t.Fatal(err)
				}
			case "dangling-symlink":
				if err := os.Symlink("missing.txt", filename); err != nil {
					t.Fatal(err)
				}
			case "directory-symlink":
				if err := os.Symlink(".", filename); err != nil {
					t.Fatal(err)
				}
			}
			_, err := resolveCredentialValue("", filename, "")
			if err == nil || err.Error() != "credential file unavailable" {
				t.Fatal("unavailable and non-regular files must fail without leaking paths", err)
			}
		})
	}
}

func TestCredentialSourceValidation(t *testing.T) {
	cases := []struct {
		name         string
		username     string
		password     string
		usernameFile string
		passwordFile string
	}{
		{"username-conflict", "user", "password", "user.txt", ""},
		{"password-conflict", "user", "password", "", "password.txt"},
		{"missing-username", "", "password", "", ""},
		{"missing-password", "user", "", "", ""},
		{"missing-password-source", "", "", "user.txt", ""},
		{"username-control", "user\n", "password", "", ""},
		{"password-control", "user", "password\x00", "", ""},
		{"invalid-utf8", "user\xff", "password", "", ""},
		{"blank-path", "", "password", " \t ", ""},
		{"path-control", "", "password", "user\n.txt", ""},
		{"path-utf8", "", "password", "user\xff.txt", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			credentials := Credentials{
				Username:     tc.username,
				Password:     tc.password,
				UsernameFile: tc.usernameFile,
				PasswordFile: tc.passwordFile,
			}
			err := credentials.validateSources()
			if err == nil || strings.Contains(err.Error(), tc.usernameFile) && tc.usernameFile != "" {
				t.Fatal("invalid credential source accepted or exposed a path", err)
			}
		})
	}
	for _, values := range [][4]string{
		{"", "", "", ""},
		{"user", "password", "", ""},
		{"user", "", "", "password.txt"},
		{"", "password", "user.txt", ""},
		{"", "", "user.txt", "password.txt"},
		{strings.Repeat("a", 8192), strings.Repeat("a", 16384), "", ""},
		{"", "password", strings.Repeat("a", 8192), ""},
	} {
		credentials := Credentials{
			Username:     values[0],
			Password:     values[1],
			UsernameFile: values[2],
			PasswordFile: values[3],
		}
		err := credentials.validateSources()
		if err != nil {
			t.Fatal("valid credential source combination rejected", err)
		}
	}
}

func TestLoadValidatesAllCredentialSourcesBeforeIO(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	first := cfg.Routing.Stores[0].Local.credentials()
	first.Username, first.UsernameFile = "", "missing-secret-sentinel.txt"
	other := credentialTestConfig(t, "mongodb")
	second := other.Routing.Stores[0].Local.credentials()
	second.PasswordFile = "conflicting-secret-sentinel.txt"
	cfg.Routing.Stores = append(cfg.Routing.Stores, other.Routing.Stores[0])

	basicFilename, routingFilename := writeCredentialTestDocuments(t, cfg)
	if _, err := Load(basicFilename, routingFilename); err == nil || err.Error() != "credential value and file are mutually exclusive" {
		t.Fatal("all source conflicts must be rejected before opening the earlier missing credential file", err)
	}
}

func TestLoadValidatesGraphBeforeCredentialIO(t *testing.T) {
	for _, mode := range []string{"unknown-service", "local-alias", "large-membership"} {
		t.Run(mode, func(t *testing.T) {
			cfg := credentialTestConfig(t, "search")
			fields := cfg.Routing.Stores[0].Local.credentials()
			fields.Username, fields.UsernameFile = "", "missing-secret-sentinel.txt"
			want := "invalid or duplicate Store"
			switch mode {
			case "unknown-service":
				cfg.Routing.Stores[0].Name = "INVALID"
			case "local-alias":

				cfg.Routing.Stores = append(cfg.Routing.Stores, cfg.Routing.Stores[0])
			case "large-membership":
				for len(cfg.Routing.Stores) < 32 {
					service := cfg.Routing.Stores[0]
					service.Name = "store-" + strconv.Itoa(len(cfg.Routing.Stores))
					cfg.Routing.Stores = append(cfg.Routing.Stores, service)
				}
				want = "credential file unavailable"
			}
			basicFilename, routingFilename := writeCredentialTestDocuments(t, cfg)
			if _, err := Load(basicFilename, routingFilename); err == nil || err.Error() != want {
				t.Fatal("static graph failures must precede credential file IO", err)
			}
		})
	}
}

func TestUnresolvedCredentialFilesCannotOpen(t *testing.T) {
	for _, backend := range []string{"mongodb", "search"} {
		t.Run(backend, func(t *testing.T) {
			cfg := credentialTestConfig(t, backend)
			fields := cfg.Routing.Stores[0].Local.credentials()
			fields.Username, fields.UsernameFile = "", "unopened-secret-sentinel.txt"
			raw, err := yaml.Marshal(cfg.Routing)
			if err != nil {
				t.Fatal(err)
			}
			_, decodeErr := DecodeRouting(strings.NewReader(string(raw)))
			node, openErr := Open(context.Background(), cfg)
			for _, err := range []error{decodeErr, cfg.Routing.Validate(), cfg.Validate(), openErr} {
				if err == nil || err.Error() != "unresolved credential file" {
					t.Fatal("unresolved credential sources must fail without reading files or opening backends/listeners", err)
				}
			}
			if node != nil {
				t.Fatal("unresolved credential configuration constructed a node")
			}
		})
	}
}

func TestMongoCredentialMappingAndProfile(t *testing.T) {
	cfg := credentialTestConfig(t, "mongodb")
	m := cfg.Routing.Stores[0].Local.Backend.MongoDB
	cfg.Routing.Stores[0].Local.Backend.Authentication.Username = "user:name@/%?#+ 汉"
	cfg.Routing.Stores[0].Local.Backend.Authentication.Password = " password:/@%#?+[]汉 "
	if err := cfg.Validate(); err != nil {
		t.Fatal("special credential characters must be accepted without URI encoding", err)
	}
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	backend := resolved.Routing.Stores[0].Local.mongoConfig("mongodb")
	parsed, err := url.Parse(backend.URI)
	if err != nil || parsed.User != nil || backend.URI != m.URI || backend.Username != cfg.Routing.Stores[0].Local.Backend.Authentication.Username || backend.Password != cfg.Routing.Stores[0].Local.Backend.Authentication.Password {
		t.Fatal("MongoDB credentials must remain separate and unchanged in backend configuration", err)
	}
	m.URI = "mongodb://user:password-secret-sentinel@unresolved.invalid:27017/?authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
	if err := cfg.Validate(); err == nil || err.Error() != "MongoDB URI must not contain credentials" {
		t.Fatal("URI userinfo must be rejected without credential leakage", err)
	}
	m.URI = "mongodb://unresolved.invalid:27017/?authSource=admin&authMechanism=SCRAM-SHA-256&tls=false"
	if err := cfg.Validate(); err == nil || strings.Contains(err.Error(), "sentinel") {
		t.Fatal("separate credentials must still enforce the audited SCRAM/TLS profile", err)
	}
}

func TestLoadLaterInvalidCredentialBeforeStartup(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	other := credentialTestConfig(t, "mongodb")
	fields := other.Routing.Stores[0].Local.credentials()
	fields.Password, fields.PasswordFile = "", "invalid-secret-sentinel.txt"
	cfg.Routing.Stores = append(cfg.Routing.Stores, other.Routing.Stores[0])

	basicFilename, routingFilename := writeCredentialTestDocuments(t, cfg)
	filename := filepath.Join(filepath.Dir(routingFilename), fields.PasswordFile)
	if err := os.WriteFile(filename, []byte("bad\x00secret-sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(basicFilename, routingFilename); err == nil || err.Error() != "invalid credential file content" {
		t.Fatal("later invalid secret must fail before opening the first Search backend or missing CA", err)
	}
}

func TestCredentialSourceYAMLFieldsAreStrict(t *testing.T) {
	for _, backend := range []string{"mongodb", "search"} {
		t.Run(backend, func(t *testing.T) {
			cfg := credentialTestConfig(t, backend)
			raw, err := yaml.Marshal(cfg.Routing)
			if err != nil {
				t.Fatal(err)
			}
			input := string(raw)
			for _, replacement := range [][2]string{
				{`username_file: ""`, "username_file: true"},
				{`username_file: ""`, "username_file: 42"},
				{`username_file: ""`, "username_file: null"},
				{`password_file: ""`, "password_file: []"},
				{`username_file: ""`, "username_file: !!str secret-sentinel"},
				{`username_file: ""`, "usernamefile: secret-sentinel"},
			} {
				if !strings.Contains(input, replacement[0]) {
					t.Fatal("credential source field missing from serialized test document")
				}
				invalid := strings.Replace(input, replacement[0], replacement[1], 1)
				_, err := DecodeRouting(strings.NewReader(invalid))
				if err == nil || strings.Contains(err.Error(), "sentinel") {
					t.Fatal("invalid credential source YAML accepted or exposed input", err)
				}
			}
			for _, line := range strings.Split(input, "\n") {
				if strings.TrimSpace(line) != `username_file: ""` {
					continue
				}
				duplicate := strings.Replace(input, line, line+"\n"+strings.Replace(line, "username_file", "USERNAME_FILE", 1), 1)
				_, err := DecodeRouting(strings.NewReader(duplicate))
				if err == nil || err.Error() != "routing duplicate configuration field" {
					t.Fatal("case duplicate credential source fields must be rejected", err)
				}
			}
		})
	}
}

func TestCredentialPathsUseRoutingSymlinkDirectory(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	fields := cfg.Routing.Stores[0].Local.credentials()
	fields.Username, fields.UsernameFile = "", "username.txt"
	basicFilename, routingFilename := writeCredentialTestDocuments(t, cfg)
	directory := filepath.Dir(routingFilename)
	targetDirectory := filepath.Join(directory, "revision")
	if err := os.Mkdir(targetDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(routingFilename, filepath.Join(targetDirectory, "routes.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("revision/routes.yaml", routingFilename); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "username.txt"), []byte("public-user\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(targetDirectory, "username.txt"), []byte("invalid\x00"), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(basicFilename, routingFilename)
	if err != nil {
		t.Fatal("credential paths must use the routing filename's directory, rather than its symlink target", err)
	}
	if loaded.Routing.Stores[0].Local.credentials().Username != "public-user" {
		t.Fatal("wrong credential file selected through routing symlink")
	}
}

func TestLoadMongoReplicaCredentialFiles(t *testing.T) {
	for _, endpoint := range []string{
		"[::1]:27017/?directConnection=true",
		"mongo-a.invalid:27017/?replicaSet=rs",
		"[::1]:27017,[::2]:27018/?replicaSet=rs",
		"mongo-a.invalid:27017,[::1]:27018/?replicaSet=rs",
	} {
		cfg := credentialTestConfig(t, "mongodb")
		local := cfg.Routing.Stores[0].Local
		local.Backend.MongoDB.URI = "mongodb://" + endpoint + "&authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
		fields := local.credentials()
		username, password := fields.Username, fields.Password
		fields.Username, fields.Password = "", ""
		fields.UsernameFile, fields.PasswordFile = "new-user.txt", "new-password.txt"
		basic, routing := writeCredentialTestDocuments(t, cfg)
		for name, value := range map[string]string{fields.UsernameFile: username, fields.PasswordFile: password} {
			filename := filepath.Join(filepath.Dir(routing), name)
			if err := os.WriteFile(filename, []byte(value), 0600); err != nil {
				t.Fatal("cannot write new fixture credential material")
			}
		}
		loaded, err := Load(basic, routing)
		if err != nil {
			t.Fatal("Load rejected a supported URI before resolving new credential files", err)
		}
		resolved := loaded.Routing.Stores[0].Local.credentials()
		if resolved.Username != username || resolved.Password != password || resolved.UsernameFile != "" || resolved.PasswordFile != "" {
			t.Fatal("Load changed resolved credential values or retained source paths")
		}
		if err := loaded.Validate(); err != nil {
			t.Fatal("resolved replica profile failed full validation", err)
		}
	}
}

func TestLoadRejectsLaterMongoURIBeforeEarlierCredentialIO(t *testing.T) {
	for _, uri := range []string{
		"mongodb://user:private-sentinel@[::1]:27017/?replicaSet=rs",
		"mongodb://user%40private-sentinel:27017/?replicaSet=rs",
		"mongodb://[::1]:27017,mongo.invalid:27018/?replicaSet=rs&unknown=private-sentinel",
		"mongodb://[::1]:27017/?replicaSet=rs&ReplicaSet=rs",
		"mongodb://[::1]:27017/?replicaSet=rs&%72eplicaSet=rs",
		"mongodb://[::1]:27017/?replicaSet=rs&tls=",
		"mongodb://[::1]:27017/?replicaSet=rs&authSource=",
		"mongodb://[::1]:27017/?replicaSet=rs&authMechanism=",
		"mongodb://[::1:27017/?replicaSet=rs",
		"mongodb://[fe80::1%25lo0]:27017/?replicaSet=rs",
		"mongodb://[::1]:27017/?replicaSet=rs#private-sentinel",
		"mongodb://[::1]:27017/path?replicaSet=rs",
		"mongodb://[::1]:27017/?replicaSet=rs&private-sentinel=" + strings.Repeat("x", 4096),
	} {
		cfg := credentialTestConfig(t, "search")
		first := cfg.Routing.Stores[0].Local.credentials()
		first.Username, first.UsernameFile = "", "not-created-private-sentinel.txt"
		later := credentialTestConfig(t, "mongodb")
		later.Routing.Stores[0].Local.Backend.MongoDB.URI = uri
		cfg.Routing.Stores = append(cfg.Routing.Stores, later.Routing.Stores[0])
		want := mongodb.ValidateURI(uri)
		if want == nil {
			t.Fatal("negative URI test is not invalid")
		}
		basic, routing := writeCredentialTestDocuments(t, cfg)
		_, err := Load(basic, routing)
		if err == nil || err.Error() != want.Error() || strings.Contains(err.Error(), "sentinel") {
			t.Fatal("later invalid URI must fail before earlier credential file IO without details", err)
		}
		if err := cfg.Validate(); err == nil || err.Error() != want.Error() {
			t.Fatal("full validation must reject the same URI before unresolved file sources", err)
		}
	}
}
