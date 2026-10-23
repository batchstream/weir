package app

import (
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestByteSizeYAMLUsesLargestIntegralUnit(t *testing.T) {
	cases := []struct {
		bytes     ByteSize
		canonical string
	}{
		{0, "0B"},
		{17, "17B"},
		{1 << 10, "1KiB"},
		{1536 << 10, "1536KiB"},
		{512 << 20, "512MiB"},
		{1 << 30, "1GiB"},
		{64 << 30, "64GiB"},
		{ByteSize(^uint64(0)), "18446744073709551615B"},
	}
	for _, tc := range cases {
		text, err := tc.bytes.MarshalYAML()
		if err != nil || text != tc.canonical {
			t.Fatal("byte size output did not choose the largest integral unit", text, err)
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
