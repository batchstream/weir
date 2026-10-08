package app

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestDecodeBasicDefaultsAndListenerRequirement(t *testing.T) {
	input := "listeners:\n  application: 127.0.0.1:0\n"
	cfg, err := DecodeBasic(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	defaults := DefaultConfig().Basic
	if cfg.Transport != defaults.Transport || defaults.Listeners.Application != "" {
		t.Fatal("process defaults changed")
	}
	input = "{}\n"
	if _, err := DecodeBasic(strings.NewReader(input)); err == nil {
		t.Fatal("missing listeners accepted")
	}
	defaults.Listeners.Application = "127.0.0.1:0"
	if err := defaults.Validate(); err != nil {
		t.Fatal("runtime basic configuration rejected a listener", err)
	}
}

func TestStrictConfigurationDocuments(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	basic, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{"basic", "routing"} {
		t.Run(document, func(t *testing.T) {
			raw := string(basic)
			duplicate := "MEMORY: 512MiB\n"
			crossField := "stores: []\n"
			nestedDuplicate := strings.Replace(raw, "stall: 30s", "stall: 30s\n    STALL: 30s", 1)
			if document == "routing" {
				raw = string(routing)
				duplicate = "STORES: []\n"
				crossField = "listeners: {}\n"
				nestedDuplicate = strings.Replace(raw, "max_batch_operations: 0", "max_batch_operations: 0\n      MAX_BATCH_OPERATIONS: 0", 1)
			}
			var decodeErr error
			if document == "basic" {
				_, decodeErr = DecodeBasic(strings.NewReader(raw))
			} else {
				_, decodeErr = DecodeRouting(strings.NewReader(raw))
			}
			if decodeErr != nil {
				t.Fatal("valid document rejected", decodeErr)
			}
			inputs := []string{
				"", "null", "[]", "true", "2", "secret-sentinel", "{", "{]",
				"unknown-secret-sentinel: value\n" + raw,
				duplicate + raw,
				"ſtores: []\n" + string(routing),
				crossField + raw,
				nestedDuplicate,
				raw + "---\n{}\n",
				raw + "---\nnull\n",
				raw + "---\n",
				raw + "garbage\n",
				"unexpected: " + strings.Repeat("[", 12) + "0" + strings.Repeat("]", 12) + "\n" + raw,
				"1: value\n" + raw,
				"? [a, b]\n: value\n" + raw,
				"<<: {}\n" + raw,
				"'<<': {}\n" + raw,
				"unexpected: &secret-sentinel value\n" + raw,
				"unexpected: &a [*a]\n" + raw,
				"unexpected: !secret-sentinel value\n" + raw,
				"unexpected: !!str secret-sentinel\n" + raw,
				"!!map\n" + raw,
			}
			for i, input := range inputs {
				if document == "basic" {
					_, decodeErr = DecodeBasic(strings.NewReader(input))
				} else {
					_, decodeErr = DecodeRouting(strings.NewReader(input))
				}
				if decodeErr == nil || strings.Contains(decodeErr.Error(), "sentinel") {
					t.Fatal("invalid document accepted or error leaked input", i, decodeErr)
				}
			}
			bounded := raw + strings.Repeat(" ", maxConfigBytes-len(raw))
			if document == "basic" {
				_, decodeErr = DecodeBasic(strings.NewReader(bounded))
			} else {
				_, decodeErr = DecodeRouting(strings.NewReader(bounded))
			}
			if decodeErr != nil {
				t.Fatal("exact size bound rejected", decodeErr)
			}
			if document == "basic" {
				_, decodeErr = DecodeBasic(strings.NewReader(bounded + " "))
			} else {
				_, decodeErr = DecodeRouting(strings.NewReader(bounded + " "))
			}
			if decodeErr == nil || !strings.Contains(decodeErr.Error(), "exceeds bound") {
				t.Fatal("oversized configuration accepted", decodeErr)
			}
		})
	}
}

func TestConfigurationNestingBound(t *testing.T) {
	for _, depth := range []int{11, 12} {
		input := "unexpected: " + strings.Repeat("[", depth) + "value" + strings.Repeat("]", depth) + "\n"
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(input), &document); err != nil {
			t.Fatal(err)
		}
		err := validateConfigYAML(document.Content[0], 0, true, "")
		if depth == 11 && err != nil {
			t.Fatal("exact nesting bound rejected", err)
		}
		if depth == 12 && (err == nil || !strings.Contains(err.Error(), "nesting limit")) {
			t.Fatal("excessive nesting accepted", err)
		}
		_, basicErr := DecodeBasic(strings.NewReader(input))
		_, routingErr := DecodeRouting(strings.NewReader(input))
		for _, err := range []error{basicErr, routingErr} {
			if err == nil || depth == 11 && strings.Contains(err.Error(), "nesting limit") || depth == 12 && !strings.Contains(err.Error(), "nesting limit") {
				t.Fatal("document nesting boundary mismatch", depth, err)
			}
		}
	}
}

func TestConfigurationUnicodeFieldDuplicates(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	routing, err := yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	input := "ſtores: []\n" + string(routing)
	if _, err := DecodeRouting(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "duplicate configuration field") {
		t.Fatal("Unicode case alias overwrote routing field", err)
	}
	basic, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	input = strings.Replace(string(basic), "stall: 30s", "stall: 30s\n        ſtall: 30s", 1)
	if _, err := DecodeBasic(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "duplicate configuration field") {
		t.Fatal("Unicode case alias overwrote basic field", err)
	}
}

func TestBasicConfigurationRejectsNullFields(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	raw, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"diagnostics: null\n",
		"listeners: ~\n",
		"memory:\n",
		"transport: null\n",
		"transport:\n  timeouts:\n    stall: null\n",
		"transport:\n  timeouts:\n    request: null\n",
	} {
		input := fragment + string(raw)
		if _, err := DecodeBasic(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "cannot be null") {
			t.Fatal("null basic field accepted", err)
		}
	}
}

type configErrorReader struct{}

func (configErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("reader-path-secret-sentinel")
}

func TestConfigurationReadErrorsAreRedacted(t *testing.T) {
	reader := configErrorReader{}
	_, basicErr := DecodeBasic(reader)
	_, routingErr := DecodeRouting(reader)
	if basicErr == nil || basicErr.Error() != "basic configuration unavailable" || routingErr == nil || routingErr.Error() != "routing configuration unavailable" {
		t.Fatal("reader error lost its document scope or leaked input", basicErr, routingErr)
	}
}

func TestLoadRoutingPaths(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "basic")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := credentialTestConfig(t, "search")
	routing, err := yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	routingFilename := filepath.Join(root, "routing.yaml")
	if err := os.WriteFile(routingFilename, routing, 0600); err != nil {
		t.Fatal(err)
	}
	basic, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(directory, "node.yaml")
	if err := os.WriteFile(filename, basic, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "routing.yaml"), []byte("invalid-secret-sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	for _, basicPath := range []string{"basic/node.yaml", filename} {
		for _, routingPath := range []string{"routing.yaml", routingFilename} {
			loaded, err := Load(basicPath, routingPath)
			if err != nil || loaded.Basic.Listeners.Application != cfg.Basic.Listeners.Application ||
				len(loaded.Routing.Stores) != 1 {
				t.Fatal("basic and routing paths must both resolve from the working directory", err)
			}
		}
	}
}

func TestLoadUnavailableDocumentsAreRedacted(t *testing.T) {
	root := t.TempDir()
	filename := filepath.Join(root, "missing-basic-secret-sentinel.yaml")
	if _, err := Load(filename, ""); err == nil || err.Error() != "basic configuration unavailable" {
		t.Fatal("missing basic error leaked path", err)
	}
	cfg := credentialTestConfig(t, "search")
	basic, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	filename = filepath.Join(root, "node.yaml")
	if err := os.WriteFile(filename, basic, 0600); err != nil {
		t.Fatal(err)
	}
	routingFilename := filepath.Join(root, "missing-routing-secret-sentinel.yaml")
	if _, err := Load(filename, routingFilename); err == nil || err.Error() != "routing configuration unavailable" {
		t.Fatal("missing routing error leaked path", err)
	}
	cfg.Basic.Listeners.Application = "invalid-listener-secret-sentinel"
	basic, err = yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, basic, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filename, routingFilename); err == nil || err.Error() != "invalid listener configuration" {
		t.Fatal("invalid basic configuration reached routing file or leaked value", err)
	}
}

func TestLoadDoesNotPerformStartupIO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	credentials := Credentials{Username: "user", Password: "secret-sentinel"}
	connection := &SearchConnection{Credentials: credentials, CAFile: "/missing/ca-secret-sentinel.pem"}
	backend := &Search{
		URL:        "https://unresolved.invalid:443",
		Connection: connection,
	}
	local := &Local{Search: backend}
	service := StoreConfig{Name: "records", Local: local}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = listener.Addr().String()
	cfg.Routing.Stores = []StoreConfig{service}
	filename := filepath.Join(t.TempDir(), "node.yaml")
	routingFilename := writeConfigFiles(t, filename, cfg, 0600)
	if _, err := Load(filename, routingFilename); err != nil {
		t.Fatal("Load accessed CA, DNS, backend or occupied listener", err)
	}
}

func TestLoadValidatesWholeGraphBeforeStartup(t *testing.T) {
	var contacts atomic.Int32
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { contacts.Add(1) })
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	backend := &Search{URL: endpoint.URL}
	local := &Local{Search: backend}
	service := StoreConfig{Name: "records", Local: local}
	invalidService := StoreConfig{Name: "invalid"}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = strings.TrimPrefix(endpoint.URL, "http://")
	cfg.Routing.Stores = []StoreConfig{service, invalidService}
	filename := filepath.Join(t.TempDir(), "node.yaml")
	routingFilename := writeConfigFiles(t, filename, cfg, 0600)
	if _, err := Load(filename, routingFilename); err == nil || contacts.Load() != 0 {
		t.Fatal("invalid later service reached startup", err, contacts.Load())
	}
}

func TestDecodeRoutingAllowsOptionalNullAdapters(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	raw, err := yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRouting(strings.NewReader(string(raw)))
	if err != nil || len(decoded.Stores) != 1 || decoded.Stores[0].Search == nil || decoded.Stores[0].MongoDB != nil {
		t.Fatal("optional null backend rejected", err)
	}
	if _, err := DecodeRouting(io.LimitReader(strings.NewReader(string(raw)), int64(len(raw)/2))); err == nil {
		t.Fatal("truncated Store document accepted")
	}
}

func TestConfigurationScalarTypes(t *testing.T) {
	basic := "listeners:\n  application: 127.0.0.1:0\n"
	for _, fragment := range []string{
		"listeners:\n  application: 127001\n", basic + "diagnostics:\n  allow_intranet: yes\n", basic + "diagnostics:\n  allow_intranet: 'true'\n", basic + "transport:\n  timeouts:\n    stall: true\n", basic + "discovery:\n  seeds: [123]\n",
	} {
		_, err := DecodeBasic(strings.NewReader(fragment))
		if err == nil || !strings.Contains(err.Error(), "invalid configuration scalar type") {
			t.Fatal("basic field accepted implicit type coercion", err)
		}
	}
	input := "stores:\n  - name: records\n    search:\n      url: http://127.0.0.1:9200\n    max_batch_operations: 2\n"
	for _, replacement := range [][2]string{{"name: records", "name: true"}, {"url: http://127.0.0.1:9200", "url: 123"}, {"max_batch_operations: 2", "max_batch_operations: '2'"}} {
		invalid := strings.Replace(input, replacement[0], replacement[1], 1)
		_, err := DecodeRouting(strings.NewReader(invalid))
		if err == nil || !strings.Contains(err.Error(), "invalid configuration scalar type") {
			t.Fatal("Store field accepted implicit coercion", replacement, err)
		}
	}
}

func TestRoutingNullFields(t *testing.T) {
	input := `stores:
  - name: database
    search: null
    mongodb:
      uri: mongodb://127.0.0.1:27017
`
	cfg, err := DecodeRouting(strings.NewReader(input))
	if err != nil || cfg.Stores[0].Local.Search != nil {
		t.Fatal("unused adapter blocks may be explicitly null", err)
	}
	for _, field := range []string{"uri"} {
		lines := strings.Split(input, "\n")
		for i, line := range lines {
			prefix := "      " + field + ":"
			if strings.HasPrefix(line, prefix) {
				lines[i] = prefix + " null"
			}
		}
		_, err := DecodeRouting(strings.NewReader(strings.Join(lines, "\n")))
		if err == nil || !strings.Contains(err.Error(), "cannot be null") {
			t.Fatal("required adapter scalar may not be null", field, err)
		}
	}
	for _, document := range []string{
		"stores: [null]\n",
		"stores:\n  - name: database\n    mongodb:\n      uri: mongodb://127.0.0.1:27017\n    max_batch_operations: null\n",
	} {
		if _, err := DecodeRouting(strings.NewReader(document)); err == nil {
			t.Fatal("null Store or required scalar accepted")
		}
	}
}

func TestNestedUnknownRoutingFieldsAreRedacted(t *testing.T) {
	input := `stores:
  - name: search
    search:
      url: https://127.0.0.1:9200
      connection:
        username: user-secret-sentinel
        password: password-secret-sentinel
        ca_file: /missing/ca-secret-sentinel.pem
`
	if _, err := DecodeRouting(strings.NewReader(input)); err != nil {
		t.Fatal("valid secret-bearing fields should pass pure validation", err)
	}
	for _, field := range []string{"server_name", "insecure_skip_verify", "auth_provider", "token", "resolver", "tls", "credentials"} {
		unknown := strings.Replace(input, "      connection:\n", "      connection:\n        "+field+": field-secret-sentinel\n", 1)
		_, err := DecodeRouting(strings.NewReader(unknown))
		if err == nil || err.Error() != "routing invalid configuration YAML or unknown field" {
			t.Fatal("unknown nested option must be rejected without exposing credentials or parser details", field, err)
		}
	}
	for _, mutation := range [][2]string{
		{"password-secret-sentinel", "null"},
		{"username: user-secret-sentinel", "username: bad:user-secret-sentinel"},
	} {
		invalid := strings.Replace(input, mutation[0], mutation[1], 1)
		_, err := DecodeRouting(strings.NewReader(invalid))
		if err == nil || strings.Contains(err.Error(), "sentinel") || strings.Contains(err.Error(), "/missing/") {
			t.Fatal("invalid nested input accepted or leaked a sensitive value", mutation, err)
		}
	}
}
