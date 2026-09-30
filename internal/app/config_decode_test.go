package app

import (
	"encoding/json"
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

	"github.com/batchstream/weir/internal/backend/search"
)

func TestDecodeBasicDefaultsAndRequiredRoutingFile(t *testing.T) {
	input := `{"listeners":{"application":"127.0.0.1:0"},"routing":{"file":"routing.json"}}`
	cfg, err := DecodeBasic(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	defaults := DefaultConfig().Basic
	if cfg.Memory != defaults.Memory || cfg.Forwarding != defaults.Forwarding || cfg.Transport != defaults.Transport || defaults.Listeners.Application != "" || defaults.Routing.File != "" {
		t.Fatal("process defaults changed")
	}
	for _, field := range []string{
		"",
		`,"routing":{}`,
		`,"routing":{"file":""}`,
		`,"routing":{"file":" \t\r\n"}`,
		`,"routing":{"file":null}`,
	} {
		input := `{"listeners":{"application":"127.0.0.1:0"}` + field + `}`
		if _, err := DecodeBasic(strings.NewReader(input)); err == nil {
			t.Fatal("missing or empty routing.file accepted")
		}
	}
	input = `{"routing":{"file":"routing.json"}}`
	if _, err := DecodeBasic(strings.NewReader(input)); err == nil {
		t.Fatal("missing listeners accepted")
	}
	defaults.Listeners.Application = "127.0.0.1:0"
	if err := defaults.Validate(); err != nil {
		t.Fatal("runtime basic configuration requires a file path", err)
	}
}

func TestStrictConfigurationDocuments(t *testing.T) {
	cfg := remoteConfig(t)
	cfg.Basic.Routing.File = "routing.json"
	basic, err := json.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := json.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{"basic", "routing"} {
		t.Run(document, func(t *testing.T) {
			raw := string(basic)
			duplicate := `"MEMORY":"512MiB",`
			crossField := `"services":[],`
			nestedDuplicate := strings.Replace(raw, `"max_connections":16`, `"max_connections":16,"MAX_CONNECTIONS":16`, 1)
			if document == "routing" {
				raw = string(routing)
				duplicate = `"SERVICES":[],`
				crossField = `"application":"127.0.0.1:0",`
				nestedDuplicate = strings.Replace(raw, `"max_concurrency":2`, `"max_concurrency":2,"MAX_CONCURRENCY":2`, 1)
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
				"", "null", "[]", "true", "2", `"secret-sentinel"`, "{", "{]",
				`{"unknown-secret-sentinel":1,` + raw[1:],
				"{" + duplicate + raw[1:],
				`{"ſervices":[],` + string(routing[1:]),
				"{" + crossField + raw[1:],
				nestedDuplicate, raw + " {}", raw + " null", raw + " garbage",
				`{"unexpected":` + strings.Repeat("[", 12) + "0" + strings.Repeat("]", 12) + "," + raw[1:],
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
		input := `{"unexpected":` + strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth) + "}"
		decoder := json.NewDecoder(strings.NewReader(input))
		err := uniqueJSON(decoder, 0, true)
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
	cfg := remoteConfig(t)
	cfg.Basic.Routing.File = "routing.json"
	routing, err := json.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	input := `{"ſervices":[],` + string(routing[1:])
	if _, err := DecodeRouting(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "duplicate configuration field") {
		t.Fatal("Unicode case alias overwrote routing field", err)
	}
	basic, err := json.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	input = strings.Replace(string(basic), `"max_sessions":16`, `"max_sessions":16,"max_ſessions":16`, 1)
	if _, err := DecodeBasic(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "duplicate configuration field") {
		t.Fatal("Unicode case alias overwrote basic field", err)
	}
}

func TestBasicConfigurationRejectsNullFields(t *testing.T) {
	cfg := remoteConfig(t)
	cfg.Basic.Routing.File = "routing.json"
	raw, err := json.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"diagnostics":null,`,
		`"listeners":null,`,
		`"memory":null,`,
		`"transport":null,`,
		`"transport":{"max_connections":null},`,
		`"transport":{"timeouts":{"unary":null}},`,
		`"forwarding":null,`,
		`"routing":null,`,
	} {
		input := "{" + fragment + string(raw[1:])
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
	cfg := remoteConfig(t)
	routing, err := json.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	routingFilename := filepath.Join(root, "routing.json")
	if err := os.WriteFile(routingFilename, routing, 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../routing.json", routingFilename} {
		cfg.Basic.Routing.File = path
		basic, err := json.Marshal(cfg.Basic)
		if err != nil {
			t.Fatal(err)
		}
		filename := filepath.Join(directory, "node.json")
		if err := os.WriteFile(filename, basic, 0600); err != nil {
			t.Fatal(err)
		}
		loaded, err := Load(filename)
		if err != nil ||
			loaded.Basic.Listeners.Application != cfg.Basic.Listeners.Application ||
			loaded.Basic.Routing.File != path ||
			len(loaded.Routing.Services) != 1 ||
			len(loaded.Routing.Routes) != 1 {
			t.Fatal("routing path not resolved from basic file", err)
		}
	}
}

func TestLoadUnavailableDocumentsAreRedacted(t *testing.T) {
	root := t.TempDir()
	filename := filepath.Join(root, "missing-basic-secret-sentinel.json")
	if _, err := Load(filename); err == nil || err.Error() != "basic configuration unavailable" {
		t.Fatal("missing basic error leaked path", err)
	}
	cfg := remoteConfig(t)
	cfg.Basic.Routing.File = "missing-routing-secret-sentinel.json"
	basic, err := json.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	filename = filepath.Join(root, "node.json")
	if err := os.WriteFile(filename, basic, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filename); err == nil || err.Error() != "routing configuration unavailable" {
		t.Fatal("missing routing error leaked path", err)
	}
	cfg.Basic.Listeners.Application = "invalid-listener-secret-sentinel"
	basic, err = json.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, basic, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filename); err == nil || err.Error() != "invalid listener configuration" {
		t.Fatal("invalid basic configuration reached routing file or leaked value", err)
	}
}

func TestLoadDoesNotPerformStartupIO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	connection := &search.Connection{Username: "user", Password: "secret-sentinel", CAFile: "/missing/ca-secret-sentinel.pem"}
	backend := &Search{
		URL:        "https://unresolved.invalid:443",
		Index:      "records",
		Profile:    search.ElasticsearchProfile,
		Connection: connection,
	}
	local := &Local{Search: backend}
	service := Service{Name: "search", Local: local}
	route := Route{Store: "records", Service: "search"}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = listener.Addr().String()
	cfg.Routing.Services, cfg.Routing.Routes = []Service{service}, []Route{route}
	filename := filepath.Join(t.TempDir(), "node.json")
	writeConfigFiles(t, filename, cfg, 0600)
	if _, err := Load(filename); err != nil {
		t.Fatal("Load accessed CA, DNS, backend or occupied listener", err)
	}
}

func TestLoadValidatesWholeGraphBeforeStartup(t *testing.T) {
	var contacts atomic.Int32
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { contacts.Add(1) })
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	backend := &Search{URL: endpoint.URL, Index: "records", Profile: search.ElasticsearchProfile}
	local := &Local{Search: backend}
	service := Service{Name: "search", Local: local}
	invalidService := Service{Name: "invalid"}
	route := Route{Store: "records", Service: "search"}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = strings.TrimPrefix(endpoint.URL, "http://")
	cfg.Routing.Services, cfg.Routing.Routes = []Service{service, invalidService}, []Route{route}
	filename := filepath.Join(t.TempDir(), "node.json")
	writeConfigFiles(t, filename, cfg, 0600)
	if _, err := Load(filename); err == nil || contacts.Load() != 0 {
		t.Fatal("invalid later service reached startup", err, contacts.Load())
	}
}

func TestDecodeRoutingAllowsOptionalNullAdapters(t *testing.T) {
	cfg := remoteConfig(t)
	raw, err := json.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRouting(strings.NewReader(string(raw)))
	if err != nil || decoded.Services[0].Local != nil || decoded.Services[0].Remote == nil {
		t.Fatal("optional null Local adapter rejected", err)
	}
	if _, err := DecodeRouting(io.LimitReader(strings.NewReader(string(raw)), int64(len(raw)-1))); err == nil {
		t.Fatal("truncated routing document accepted")
	}
}
