package app

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/server"
)

func TestDurationJSON(t *testing.T) {
	for _, duration := range []time.Duration{0, time.Nanosecond, 1500 * time.Millisecond, 30 * time.Second, 5 * time.Minute, time.Duration(1<<63 - 1)} {
		value := Duration(duration)
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Duration
		if err := json.Unmarshal(raw, &decoded); err != nil || decoded != value {
			t.Fatal("duration changed in JSON round trip", err)
		}
		if string(raw) != `"`+duration.String()+`"` {
			t.Fatal("duration is not readable", string(raw))
		}
	}
	for _, input := range []string{"30000", "null", "true", `""`, `"30"`, `"30sec"`, `"1ms-secret-sentinel"`, `"9223372036854775808ns"`, `"999999999999999999999h"`} {
		value := Duration(time.Second)
		err := json.Unmarshal([]byte(input), &value)
		if err == nil || strings.Contains(err.Error(), "sentinel") || value != Duration(time.Second) {
			t.Fatal("invalid duration accepted, changed state or leaked input", err)
		}
	}
}

func TestByteSizeJSON(t *testing.T) {
	cases := []struct {
		input     string
		bytes     ByteSize
		canonical string
	}{
		{`"0B"`, 0, `"0B"`},
		{`"17B"`, 17, `"17B"`},
		{`"1024B"`, 1 << 10, `"1KiB"`},
		{`"1536KiB"`, 1536 << 10, `"1536KiB"`},
		{`"512MiB"`, 512 << 20, `"512MiB"`},
		{`"1024MiB"`, 1 << 30, `"1GiB"`},
		{`"64GiB"`, 64 << 30, `"64GiB"`},
		{`"18446744073709551615B"`, ByteSize(^uint64(0)), `"18446744073709551615B"`},
	}
	for _, tc := range cases {
		var value ByteSize
		if err := json.Unmarshal([]byte(tc.input), &value); err != nil || value != tc.bytes {
			t.Fatal("byte size parsing changed units", tc.input, err)
		}
		raw, err := json.Marshal(value)
		if err != nil || string(raw) != tc.canonical {
			t.Fatal("byte size did not choose the largest integral unit", tc.input, string(raw), err)
		}
	}
	for _, input := range []string{"512", "null", "true", `""`, `"512"`, `"-1B"`, `"+1B"`, `"1.5GiB"`, `"1GB"`, `"1MiB "`, `" 1MiB"`, `"1mib"`, `"memory-secret-sentinel"`, `"18446744073709551616B"`, `"18014398509481984KiB"`, `"17592186044416MiB"`, `"17179869184GiB"`} {
		value := ByteSize(512 << 20)
		err := json.Unmarshal([]byte(input), &value)
		if err == nil || strings.Contains(err.Error(), "sentinel") || value != 512<<20 {
			t.Fatal("invalid byte size accepted, changed state or leaked input", err)
		}
	}
}

func TestGroupedConfigurationDefaultsAndConversion(t *testing.T) {
	input := `{"listeners":{"application":"127.0.0.1:0"},"routing":{"file":"routing.json"},"memory":"1GiB","transport":{"max_sessions":1,"timeouts":{"unary":"1.5s"}},"forwarding":{"hop_limit":0}}`
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
		t.Fatal("assembled configuration should have explicit sections", err)
	}
}

func TestGroupedConfigurationBounds(t *testing.T) {
	prefix := `{"listeners":{"application":"127.0.0.1:0"},"routing":{"file":"routing.json"},`
	for _, fragment := range []string{
		`"memory":"64MiB"`, `"memory":"64GiB"`,
		`"transport":{"max_connections":1,"max_sessions":64,"timeouts":{"unary":"1ns","bulk":"15m","scan":"5m","native":"5m","stall":"30s"}}`,
		`"forwarding":{"hop_limit":8}`,
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment + "}")); err != nil {
			t.Fatal("valid inclusive bound rejected", fragment, err)
		}
	}
	for _, fragment := range []string{
		`"memory":"67108863B"`, `"memory":"68719476737B"`,
		`"transport":{"max_connections":0}`, `"transport":{"max_connections":65}`, `"transport":{"max_sessions":0}`, `"transport":{"max_sessions":65}`,
		`"transport":{"timeouts":{"unary":"0s"}}`, `"transport":{"timeouts":{"unary":"-1s"}}`, `"transport":{"timeouts":{"unary":"30.000000001s"}}`,
		`"transport":{"timeouts":{"bulk":"15m0.000000001s"}}`, `"transport":{"timeouts":{"scan":"5m0.000000001s"}}`, `"transport":{"timeouts":{"native":"5m0.000000001s"}}`, `"transport":{"timeouts":{"stall":"30.000000001s"}}`,
		`"forwarding":{"hop_limit":-1}`, `"forwarding":{"hop_limit":9}`,
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment + "}")); err == nil {
			t.Fatal("out-of-range grouped configuration accepted", fragment)
		}
	}
}

func TestGroupedConfigurationRejectsOldFieldsAndNumbers(t *testing.T) {
	prefix := `{"listeners":{"application":"127.0.0.1:0"},"routing":{"file":"routing.json"},`
	for _, fragment := range []string{
		`"application":"127.0.0.1:0"`, `"peer":"127.0.0.1:0"`, `"diagnostics":"127.0.0.1:0"`, `"diagnostics_allow_intranet":true`,
		`"memory_mib":512`, `"initial_forwards":4`, `"limits":{}`, `"routing_file":"routing.json"`,
		`"memory":512`, `"memory":null`, `"transport":{"timeouts":{"unary":30000}}`, `"transport":{"timeouts":{"unary":null}}`,
		`"transport":{"connections":16}`, `"transport":{"sessions":16}`, `"transport":{"unary_ms":30000}`,
	} {
		if _, err := DecodeBasic(strings.NewReader(prefix + fragment + "}")); err == nil {
			t.Fatal("old field or non-string unit accepted", fragment)
		}
	}
	for _, local := range []string{
		`{"mongo":{"uri":"mongodb://127.0.0.1:27017","database":"db","collection":"records"}}`,
		`{"mongodb":{"uri":"mongodb://127.0.0.1:27017","database":"db","collection":"records"},"concurrency":4}`,
		`{"mongodb":{"uri":"mongodb://127.0.0.1:27017","database":"db","collection":"records"},"batch_operations":16}`,
	} {
		input := `{"services":[{"name":"local","local":` + local + `}],"routes":[{"store":"records","service":"local"}]}`
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("old local field accepted", local)
		}
	}
	input := `{"services":[{"name":"remote","remote":{"endpoints":["127.0.0.1:1"],"relays":2}}],"routes":[{"store":"records","service":"remote"}]}`
	if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
		t.Fatal("old remote relays field accepted")
	}
}
