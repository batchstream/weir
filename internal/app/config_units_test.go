package app

import (
	"math"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestDurationYAML(t *testing.T) {
	for _, duration := range []time.Duration{
		0,
		time.Nanosecond,
		1500 * time.Millisecond,
		30 * time.Second,
		5 * time.Minute,
		time.Duration(1<<63 - 1),
	} {
		var decoded Duration
		if err := yaml.Unmarshal([]byte(duration.String()), &decoded); err != nil || decoded != Duration(duration) {
			t.Fatal("duration did not parse", duration, err)
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
		input string
		bytes ByteSize
	}{
		{"0B", 0},
		{"17B", 17},
		{"1024B", 1 << 10},
		{"1536KiB", 1536 << 10},
		{"512MiB", 512 << 20},
		{"64GiB", 64 << 30},
		{"9223372036854775807B", math.MaxInt},
	}
	for _, tc := range cases {
		var value ByteSize
		if err := yaml.Unmarshal([]byte(tc.input), &value); err != nil || value != tc.bytes {
			t.Fatal("byte size parsing changed units", tc.input, err)
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
		"9223372036854775808B",
		"9007199254740992KiB",
		"8796093022208MiB",
		"8589934592GiB",
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
