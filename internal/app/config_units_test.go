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
    request: 1.5s
`
	basic, err := DecodeBasic(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	expected := server.DefaultLimits()
	expected.Sessions = 1
	expected.RequestLifetime = 1500 * time.Millisecond
	if basic.Transport.serverLimits() != expected || basic.Memory != 1<<30 {
		t.Fatal("partial nested settings discarded defaults or changed units")
	}
	cfg := emptyConfig(t)
	cfg.Basic = basic
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	if budget := node.guard.Snapshot().Budget; budget != 1<<30 {
		t.Fatal("assembly converted the byte budget a second time", budget)
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
    request: 1ns
    stall: 30s
`,
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment)); err != nil {
			t.Fatal("valid inclusive bound rejected", fragment, err)
		}
	}
	for _, fragment := range []string{
		"memory: 67108863B\n", "memory: 68719476737B\n",
		"transport:\n  max_connections: 0\n", "transport:\n  max_connections: 9223372036854775807\n",
		"transport:\n  max_sessions: 0\n", "transport:\n  max_sessions: 9223372036854775807\n",
		"transport:\n  timeouts:\n    request: 0s\n", "transport:\n  timeouts:\n    request: -1s\n",
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment)); err == nil {
			t.Fatal("out-of-range grouped configuration accepted", fragment)
		}
	}
}

func TestGroupedConfigurationRejectsUnknownFieldsAndInvalidUnits(t *testing.T) {
	prefix := "listeners:\n  application: 127.0.0.1:0\n"
	for _, fragment := range []string{
		"unexpected: true\n",
		"transport:\n  unexpected: 16\n",
		"transport:\n  timeouts:\n    unexpected: 1s\n",
		"memory: 512\n", "memory: null\n",
		"transport:\n  timeouts:\n    request: 30000\n",
		"transport:\n  timeouts:\n    request: null\n",
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment)); err == nil {
			t.Fatal("unknown field or non-string unit accepted", fragment)
		}
	}
	for _, local := range []string{
		`    unexpected: {}
`,
		`    mongodb:
      uri: mongodb://127.0.0.1:27017
      unexpected: true
`,
		`    mongodb:
      uri: mongodb://127.0.0.1:27017
    unexpected: 4
`,
	} {
		input := "stores:\n  - name: records\n" + local
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("unknown Store or backend field accepted", local)
		}
	}
}
