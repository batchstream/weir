package app

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/server"
	"go.yaml.in/yaml/v3"
)

func TestDurationMarshalJSON(t *testing.T) {
	for _, duration := range []time.Duration{
		0,
		time.Nanosecond,
		1500 * time.Millisecond,
		30 * time.Second,
		5 * time.Minute,
		time.Duration(1<<63 - 1),
	} {
		value := Duration(duration)
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != `"`+duration.String()+`"` {
			t.Fatal("duration is not readable", string(raw))
		}
	}
}

func TestByteSizeMarshalJSON(t *testing.T) {
	cases := []struct {
		bytes     ByteSize
		canonical string
	}{
		{0, `"0B"`},
		{17, `"17B"`},
		{1 << 10, `"1KiB"`},
		{1536 << 10, `"1536KiB"`},
		{512 << 20, `"512MiB"`},
		{1 << 30, `"1GiB"`},
		{64 << 30, `"64GiB"`},
		{ByteSize(^uint64(0)), `"18446744073709551615B"`},
	}
	for _, tc := range cases {
		raw, err := json.Marshal(tc.bytes)
		if err != nil || string(raw) != tc.canonical {
			t.Fatal("byte size output did not choose the largest integral unit", string(raw), err)
		}
	}
}

func TestDurationYAML(t *testing.T) {
	for _, duration := range []time.Duration{
		0,
		time.Nanosecond,
		1500 * time.Millisecond,
		30 * time.Second,
		5 * time.Minute,
		time.Duration(1<<63 - 1),
	} {
		value := Duration(duration)
		raw, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Duration
		if err := yaml.Unmarshal(raw, &decoded); err != nil || decoded != value {
			t.Fatal("duration changed in YAML round trip", err)
		}
		if strings.TrimSpace(string(raw)) != duration.String() {
			t.Fatal("duration is not readable", string(raw))
		}
	}
	for _, input := range []string{
		"30000",
		"null",
		"true",
		"[]",
		"{}",
		"''",
		"30",
		"30sec",
		"1ms-secret-sentinel",
		"9223372036854775808ns",
		"999999999999999999999h",
	} {
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(input), &document); err != nil {
			t.Fatal(err)
		}
		value := Duration(time.Second)
		err := value.UnmarshalYAML(document.Content[0])
		if err == nil || strings.Contains(err.Error(), "sentinel") || value != Duration(time.Second) {
			t.Fatal("invalid YAML duration accepted, changed state or leaked input", err)
		}
	}
}

func TestByteSizeYAML(t *testing.T) {
	cases := []struct {
		input     string
		bytes     ByteSize
		canonical string
	}{
		{"0B", 0, "0B"},
		{"17B", 17, "17B"},
		{"1024B", 1 << 10, "1KiB"},
		{"1536KiB", 1536 << 10, "1536KiB"},
		{"512MiB", 512 << 20, "512MiB"},
		{"1024MiB", 1 << 30, "1GiB"},
		{"64GiB", 64 << 30, "64GiB"},
		{"18446744073709551615B", ByteSize(^uint64(0)), "18446744073709551615B"},
	}
	for _, tc := range cases {
		var value ByteSize
		if err := yaml.Unmarshal([]byte(tc.input), &value); err != nil || value != tc.bytes {
			t.Fatal("byte size parsing changed units", tc.input, err)
		}
		raw, err := yaml.Marshal(value)
		if err != nil || strings.TrimSpace(string(raw)) != tc.canonical {
			t.Fatal("byte size did not choose the largest integral unit", tc.input, string(raw), err)
		}
	}
	for _, input := range []string{
		"512",
		"null",
		"true",
		"[]",
		"{}",
		"''",
		"-1B",
		"+1B",
		"1.5GiB",
		"1GB",
		"'1MiB '",
		"' 1MiB'",
		"1mib",
		"memory-secret-sentinel",
		"18446744073709551616B",
		"18014398509481984KiB",
		"17592186044416MiB",
		"17179869184GiB",
	} {
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(input), &document); err != nil {
			t.Fatal(err)
		}
		value := ByteSize(512 << 20)
		err := value.UnmarshalYAML(document.Content[0])
		if err == nil || strings.Contains(err.Error(), "sentinel") || value != 512<<20 {
			t.Fatal("invalid YAML byte size accepted, changed state or leaked input", err)
		}
	}
}

func TestGroupedConfigurationDefaultsAndConversion(t *testing.T) {
	input := `listeners:
  application: 127.0.0.1:0
memory: 1GiB
transport:
  max_sessions: 1
  timeouts:
    unary: 1.5s
forwarding:
  hop_limit: 0
`
	basic, err := DecodeBasic(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	expected := server.DefaultLimits()
	expected.Sessions = 1
	expected.UnaryLifetime = 1500 * time.Millisecond
	if basic.Transport.serverLimits() != expected || basic.Memory != 1<<30 || basic.Forwarding.HopLimit != 0 {
		t.Fatal("partial nested settings discarded defaults or changed units")
	}
	cfg := remoteConfig(t)
	cfg.Basic = basic
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	if node.budget != 1<<30 {
		t.Fatal("assembly converted the byte budget a second time", node.budget)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(raw, &sections); err != nil || len(sections) != 2 || sections["basic"] == nil || sections["routing"] == nil {
		t.Fatal("assembled configuration should have explicit diagnostic JSON sections", err)
	}
}

func TestGroupedConfigurationBounds(t *testing.T) {
	prefix := "listeners:\n  application: 127.0.0.1:0\n"
	for _, fragment := range []string{
		"memory: 64MiB\n", "memory: 64GiB\n",
		`transport:
  max_connections: 1
  max_sessions: 64
  timeouts:
    unary: 1ns
    bulk: 15m
    scan: 5m
    native: 5m
    stall: 30s
`,
		"forwarding:\n  hop_limit: 8\n",
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment)); err != nil {
			t.Fatal("valid inclusive bound rejected", fragment, err)
		}
	}
	for _, fragment := range []string{
		"memory: 67108863B\n", "memory: 68719476737B\n",
		"transport:\n  max_connections: 0\n", "transport:\n  max_connections: 65\n",
		"transport:\n  max_sessions: 0\n", "transport:\n  max_sessions: 65\n",
		"transport:\n  timeouts:\n    unary: 0s\n", "transport:\n  timeouts:\n    unary: -1s\n",
		"transport:\n  timeouts:\n    unary: 30.000000001s\n",
		"transport:\n  timeouts:\n    bulk: 15m0.000000001s\n", "transport:\n  timeouts:\n    scan: 5m0.000000001s\n",
		"transport:\n  timeouts:\n    native: 5m0.000000001s\n", "transport:\n  timeouts:\n    stall: 30.000000001s\n",
		"forwarding:\n  hop_limit: -1\n", "forwarding:\n  hop_limit: 9\n",
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment)); err == nil {
			t.Fatal("out-of-range grouped configuration accepted", fragment)
		}
	}
}

func TestGroupedConfigurationRejectsOldFieldsAndNumbers(t *testing.T) {
	prefix := "listeners:\n  application: 127.0.0.1:0\n"
	for _, fragment := range []string{
		"application: 127.0.0.1:0\n", "peer: 127.0.0.1:0\n", "diagnostics: 127.0.0.1:0\n", "diagnostics_allow_intranet: true\n",
		"memory_mib: 512\n", "initial_forwards: 4\n", "limits: {}\n", "routing_file: routing.yaml\n",
		"memory: 512\n", "memory: null\n", "transport:\n  timeouts:\n    unary: 30000\n", "transport:\n  timeouts:\n    unary: null\n",
		"transport:\n  connections: 16\n", "transport:\n  sessions: 16\n", "transport:\n  unary_ms: 30000\n",
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment)); err == nil {
			t.Fatal("old field or non-string unit accepted", fragment)
		}
	}
	for _, local := range []string{
		`      mongo:
        uri: mongodb://127.0.0.1:27017
`,
		`      mongodb:
        uri: mongodb://127.0.0.1:27017
      concurrency: 4
`,
		`      mongodb:
        uri: mongodb://127.0.0.1:27017
      batch_operations: 16
`,
	} {
		input := "services:\n  - name: local\n    local:\n" + local + "routes:\n  - store: records\n    service: local\n"
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("old local field accepted", local)
		}
	}
	input := `services:
  - name: remote
    remote:
      endpoints: [127.0.0.1:1]
      relays: 2
routes:
  - store: records
    service: remote
`
	if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
		t.Fatal("old remote relays field accepted")
	}
}
