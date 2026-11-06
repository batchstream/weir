package app

import (
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func credentialTestConfig(t *testing.T, backend string) Config {
	t.Helper()
	credentials := &Credentials{
		Username: "user-secret-sentinel",
		Password: " password-secret-sentinel:@/%?汉 ",
	}
	definition := StoreConfig{Name: backend, Authentication: credentials}
	if backend == "mongodb" {
		definition.MongoDB = &MongoDB{URI: "mongodb://unresolved.invalid:27017/?authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"}
	} else {
		definition.TLS = &TLS{CAFile: "/missing/ca-secret-sentinel.pem"}
		definition.Search = &Search{URL: "https://unresolved.invalid:9200"}
	}
	cfg := Config{}
	cfg.Listeners.Application = "127.0.0.1:0"
	cfg.Stores = []StoreConfig{definition}
	return cfg
}

// writeCredentialTestDocument writes cfg to its own directory, where relative
// credential files resolve.
func writeCredentialTestDocument(t *testing.T, cfg Config) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "config")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(directory, "weir.yaml")
	writeConfigFile(t, filename, cfg, 0600)
	return filename
}

func TestCredentialConfigurationUsesSharedAuthentication(t *testing.T) {
	for _, backend := range []string{"mongodb", "search"} {
		t.Run(backend, func(t *testing.T) {
			cfg := credentialTestConfig(t, backend)
			raw, err := yaml.Marshal(cfg.Stores[0].Authentication)
			if err != nil {
				t.Fatal(err)
			}
			expected := map[string]any{
				"username": "user-secret-sentinel",
				"password": " password-secret-sentinel:@/%?汉 ",
			}
			var fields map[string]any
			if err := yaml.Unmarshal(raw, &fields); err != nil || !reflect.DeepEqual(fields, expected) {
				t.Fatal("serialized Store changed its credential field layout", err)
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
					workingDirectory := t.TempDir()
					t.Chdir(workingDirectory)
					cfg := credentialTestConfig(t, backend)
					fields := cfg.Stores[0].Authentication
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
					configFilename := writeCredentialTestDocument(t, cfg)
					for _, input := range [][2]string{
						{fields.UsernameFile, username + "\n"},
						{fields.PasswordFile, password + "\r\n"},
					} {
						filename := input[0]
						if filename == "" {
							continue
						}
						if !filepath.IsAbs(filename) {
							filename = filepath.Join(filepath.Dir(configFilename), filename)
							decoy := filepath.Join(workingDirectory, input[0])
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
					loaded, err := Load(configFilename)
					if err != nil {
						t.Fatal("all inline/file combinations must load without backend, DNS or CA-file IO", err)
					}
					resolved := loaded.Stores[0].Authentication
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
		{"large-file", strings.Repeat("a", 1<<20), strings.Repeat("a", 1<<20)},
		{"empty", "", ""},
		{"only-lf", "\n", ""},
		{"only-crlf", "\r\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "input.txt")
			if err := os.WriteFile(filename, []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			value, err := credentialValue("", filename, "")
			if tc.want == "" {
				if err == nil || err.Error() != "invalid credential file content" {
					t.Fatal("empty text must be rejected without exposing its path", err)
				}
			} else if err != nil || value != tc.want {
				t.Fatal("valid text lost bytes", err)
			}
		})
	}
}

// What remains after one line ending goes to the backend's own validation.
func TestLoadRejectsControlCharactersFromCredentialFiles(t *testing.T) {
	for _, content := range []string{"value\r", "value\n\n", "value\r\n\r\n", "val\nue", "val\x00ue", "val\tue", "val\x7fue", "val\u0085ue", "val\xffue"} {
		for _, kind := range []string{"mongodb", "search"} {
			cfg := credentialTestConfig(t, kind)
			fields := cfg.Stores[0].Authentication
			fields.Password, fields.PasswordFile = "", "password.txt"
			configFilename := writeCredentialTestDocument(t, cfg)
			filename := filepath.Join(filepath.Dir(configFilename), fields.PasswordFile)
			if err := os.WriteFile(filename, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(configFilename); err == nil || !strings.Contains(err.Error(), "credential pair") {
				t.Fatal("control character in a credential file accepted", kind, err)
			}
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
	fields := cfg.Stores[0].Authentication
	fields.Username, fields.Password = "", ""
	fields.UsernameFile = filepath.Join(directory, "username.txt")
	fields.PasswordFile = filepath.Join(directory, "password.txt")
	if err := os.WriteFile(fields.UsernameFile, []byte(username+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fields.PasswordFile, []byte(password+"\r\n"), 0600); err != nil {
		t.Fatal(err)
	}
	configFilename := writeCredentialTestDocument(t, cfg)
	loaded, err := Load(configFilename)
	if err != nil {
		t.Fatal("large generated credentials retained an artificial path/text threshold", err)
	}
	resolved := loaded.Stores[0].Authentication
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
			_, err := credentialValue("", filename, "")
			if err == nil || err.Error() != "credential file unavailable" {
				t.Fatal("unavailable and non-regular files must fail without leaking paths", err)
			}
		})
	}
}

func TestCredentialValueAndFileAreExclusive(t *testing.T) {
	for _, credentials := range []Credentials{
		{Username: "user", UsernameFile: "secret-sentinel.txt", Password: "password"},
		{Username: "user", Password: "password", PasswordFile: "secret-sentinel.txt"},
	} {
		if err := credentials.resolve(t.TempDir()); err == nil || err.Error() != "credential value and file are mutually exclusive" {
			t.Fatal("credential value and file combined", err)
		}
	}
}

func TestMongoCredentialMappingAndProfile(t *testing.T) {
	cfg := credentialTestConfig(t, "mongodb")
	m := cfg.Stores[0].MongoDB
	cfg.Stores[0].Authentication.Username = "user:name@/%?#+ 汉"
	cfg.Stores[0].Authentication.Password = " password:/@%#?+[]汉 "
	if err := cfg.Validate(); err != nil {
		t.Fatal("special credential characters must be accepted without URI encoding", err)
	}
	backend := cfg.Stores[0].mongoConfig()
	parsed, err := url.Parse(backend.URI)
	if err != nil || parsed.User != nil || backend.URI != m.URI || backend.Username != cfg.Stores[0].Authentication.Username || backend.Password != cfg.Stores[0].Authentication.Password {
		t.Fatal("MongoDB credentials must remain separate and unchanged in backend configuration", err)
	}
	m.URI = "mongodb://user:password-secret-sentinel@unresolved.invalid:27017/?authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
	if err := cfg.Validate(); err == nil || strings.Contains(err.Error(), "sentinel") {
		t.Fatal("URI userinfo must be rejected without credential leakage", err)
	}
	m.URI = "mongodb://unresolved.invalid:27017/?authSource=admin&authMechanism=SCRAM-SHA-256"
	if err := cfg.Validate(); err == nil || strings.Contains(err.Error(), "sentinel") {
		t.Fatal("separate credentials must still enforce the audited SCRAM profile with explicit tls", err)
	}
}

func TestCredentialSourceYAMLFieldsAreStrict(t *testing.T) {
	for _, fields := range []string{
		"username_file: true\n      password: p\n",
		"username_file: 42\n      password: p\n",
		"username_file: [secret-sentinel]\n      password: p\n",
		"usernamefile: secret-sentinel\n      password: p\n",
		"USERNAME_FILE: secret-sentinel\n      password: p\n",
	} {
		input := searchDocument + "    authentication:\n      " + fields
		if _, err := Decode(input); err == nil || strings.Contains(err.Error(), "sentinel") {
			t.Fatal("invalid credential source YAML accepted or exposed input", fields, err)
		}
	}
}

func TestCredentialPathsUseConfigSymlinkDirectory(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	fields := cfg.Stores[0].Authentication
	fields.Username, fields.UsernameFile = "", "username.txt"
	configFilename := writeCredentialTestDocument(t, cfg)
	directory := filepath.Dir(configFilename)
	targetDirectory := filepath.Join(directory, "revision")
	if err := os.Mkdir(targetDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(configFilename, filepath.Join(targetDirectory, "weir.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("revision/weir.yaml", configFilename); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "username.txt"), []byte("public-user\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(targetDirectory, "username.txt"), []byte("invalid\x00"), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(configFilename)
	if err != nil {
		t.Fatal("credential paths must use the configuration filename's directory, rather than its symlink target", err)
	}
	if loaded.Stores[0].Authentication.Username != "public-user" {
		t.Fatal("wrong credential file selected through a configuration symlink")
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
		definition := &cfg.Stores[0]
		definition.MongoDB.URI = "mongodb://" + endpoint + "&authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
		fields := definition.Authentication
		username, password := fields.Username, fields.Password
		fields.Username, fields.Password = "", ""
		fields.UsernameFile, fields.PasswordFile = "new-user.txt", "new-password.txt"
		configFilename := writeCredentialTestDocument(t, cfg)
		for name, value := range map[string]string{fields.UsernameFile: username, fields.PasswordFile: password} {
			filename := filepath.Join(filepath.Dir(configFilename), name)
			if err := os.WriteFile(filename, []byte(value), 0600); err != nil {
				t.Fatal("cannot write new fixture credential material")
			}
		}
		loaded, err := Load(configFilename)
		if err != nil {
			t.Fatal("Load rejected a supported URI before resolving new credential files", err)
		}
		resolved := loaded.Stores[0].Authentication
		if resolved.Username != username || resolved.Password != password || resolved.UsernameFile != "" || resolved.PasswordFile != "" {
			t.Fatal("Load changed resolved credential values or retained source paths")
		}
		if err := loaded.Validate(); err != nil {
			t.Fatal("resolved replica profile failed full validation", err)
		}
	}
}
