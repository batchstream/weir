package app

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/store"
)

const searchDocument = `listeners:
  application: 127.0.0.1:0
stores:
  - name: records
    search:
      url: http://127.0.0.1:9200
`

func TestDecodeAppliesDefaults(t *testing.T) {
	cfg, err := Decode(strings.NewReader(searchDocument))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.shutdownTimeout() != 5*time.Second || cfg.Listeners.Diagnostics != "" {
		t.Fatal("omitted settings did not select their defaults", cfg)
	}
	limits := cfg.Stores[0].runtimeLimits()
	if limits != store.DefaultLimits() {
		t.Fatal("omitted batching did not select the Store defaults", limits)
	}
	if _, err := Decode(strings.NewReader("stores: []\n")); err == nil {
		t.Fatal("missing application listener accepted")
	}
}

func TestBatchingReachesRuntimeAndAdapter(t *testing.T) {
	input := searchDocument + `    batching:
      max_operations: 7
      max_bytes: 1KiB
      max_wait: 0s
      max_in_flight: 3
`
	cfg, err := Decode(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	definition := cfg.Stores[0]
	limits := definition.runtimeLimits()
	if limits.BatchOperations != 7 || limits.BatchBytes != 1024 || limits.BatchWait != 0 || limits.MaxInFlight != 3 {
		t.Fatal("explicit batching did not reach the Store runtime", limits)
	}
	if definition.searchConfig().Options.BatchBytes != 1024 {
		t.Fatal("batch bytes did not reach the adapter")
	}
	for _, fragment := range []string{
		"    batching:\n      max_operations: -1\n",
		"    batching:\n      max_in_flight: -1\n",
		"    batching:\n      max_wait: -1ms\n",
		"    batching:\n      max_operations: '7'\n",
		"    batching:\n      max_bytes: 1024\n",
		"    batching:\n      max_wait: 5\n",
		"    batching:\n      max_bytes: 9223372036854775808B\n",
	} {
		if _, err := Decode(strings.NewReader(searchDocument + fragment)); err == nil {
			t.Fatal("invalid batching accepted", fragment)
		}
	}
}

func TestDecodeIsStrict(t *testing.T) {
	inputs := []string{
		"",
		"[]",
		"secret-sentinel",
		"unknown-secret-sentinel: value\n" + searchDocument,
		strings.Replace(searchDocument, "url:", "retry_attempts: 5\n      url:", 1),
		strings.Replace(searchDocument, "    search:", "    backend: {}\n    search:", 1),
		searchDocument + "listeners:\n  application: 127.0.0.1:1\n",
		searchDocument + "---\n{}\n",
		"shutdown_timeout: 5\n" + searchDocument,
		"shutdown_timeout: -1s\n" + searchDocument,
	}
	for i, input := range inputs {
		if _, err := Decode(strings.NewReader(input)); err == nil || strings.Contains(err.Error(), "sentinel") {
			t.Fatal("invalid document accepted or error leaked input", i, err)
		}
	}
	large := searchDocument + strings.Repeat(" ", 256<<10)
	if _, err := Decode(strings.NewReader(large)); err != nil {
		t.Fatal("large valid configuration rejected", err)
	}
}

func TestStoresRequireOneBackendAndUniqueNames(t *testing.T) {
	inputs := []string{
		strings.Replace(searchDocument, "    search:\n      url: http://127.0.0.1:9200\n", "", 1),
		searchDocument + "    mongodb:\n      uri: mongodb://127.0.0.1:27017/?directConnection=true\n",
		searchDocument + "  - name: records\n    search:\n      url: http://127.0.0.1:9201\n",
		strings.Replace(searchDocument, "name: records", "name: Records", 1),
	}
	for i, input := range inputs {
		if _, err := Decode(strings.NewReader(input)); err == nil {
			t.Fatal("invalid Store accepted", i)
		}
	}
}

type configErrorReader struct{}

func (configErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("read-secret-sentinel")
}

func TestReadErrorsAreRedacted(t *testing.T) {
	if _, err := Decode(configErrorReader{}); err == nil || err.Error() != "configuration unavailable" {
		t.Fatal("reader error leaked input", err)
	}
	filename := filepath.Join(t.TempDir(), "missing-secret-sentinel.yaml")
	if _, err := Load(filename); err == nil || err.Error() != "configuration unavailable" {
		t.Fatal("missing file error leaked its path", err)
	}
}

func TestLoadPathIsRelativeToWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "config"), 0700); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(root, "config", "weir.yaml")
	if err := os.WriteFile(filename, []byte(searchDocument), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	for _, path := range []string{"config/weir.yaml", filename} {
		loaded, err := Load(path)
		if err != nil || len(loaded.Stores) != 1 {
			t.Fatal("configuration path did not resolve from the working directory", path, err)
		}
	}
}

func TestLoadResolvesCAFileFromConfigDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "config")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	relative := searchFixtureStore("relative", "https://127.0.0.1:9200")
	relative.TLS = &TLS{CAFile: "trust/ca.pem"}
	absolute := searchFixtureStore("absolute", "https://127.0.0.1:9200")
	absolute.TLS = &TLS{CAFile: "/etc/weir/ca.pem"}
	cfg := emptyConfig(t)
	cfg.Stores = []StoreConfig{relative, absolute}
	filename := filepath.Join(directory, "weir.yaml")
	writeConfigFile(t, filename, cfg, 0600)

	t.Chdir(t.TempDir())
	loaded, err := Load(filename)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Stores[0].TLS.CAFile != filepath.Join(directory, "trust", "ca.pem") || loaded.Stores[1].TLS.CAFile != "/etc/weir/ca.pem" {
		t.Fatal("CA paths must resolve from the configuration directory", loaded.Stores[0].TLS, loaded.Stores[1].TLS)
	}
}

func TestLoadDoesNotPerformStartupIO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	credentials := &Credentials{Username: "user", Password: "secret-sentinel"}
	trust := &TLS{CAFile: "/missing/ca-secret-sentinel.pem"}
	definition := searchFixtureStore("records", "https://unresolved.invalid:443")
	definition.Authentication, definition.TLS = credentials, trust

	cfg := Config{}
	cfg.Listeners.Application = listener.Addr().String()
	cfg.Stores = []StoreConfig{definition}
	filename := filepath.Join(t.TempDir(), "weir.yaml")
	writeConfigFile(t, filename, cfg, 0600)
	if _, err := Load(filename); err != nil {
		t.Fatal("Load accessed CA, DNS, backend or occupied listener", err)
	}
}

func TestLoadValidatesEveryStoreBeforeStartup(t *testing.T) {
	var contacts atomic.Int32
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { contacts.Add(1) })
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	invalid := StoreConfig{Name: "invalid"}

	cfg := Config{}
	cfg.Listeners.Application = strings.TrimPrefix(endpoint.URL, "http://")
	cfg.Stores = []StoreConfig{searchFixtureStore("records", endpoint.URL), invalid}
	filename := filepath.Join(t.TempDir(), "weir.yaml")
	writeConfigFile(t, filename, cfg, 0600)
	if _, err := Load(filename); err == nil || contacts.Load() != 0 {
		t.Fatal("invalid later Store reached startup", err, contacts.Load())
	}
}
