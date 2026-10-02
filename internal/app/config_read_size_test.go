package app

import (
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/protocol"
	"go.yaml.in/yaml/v3"
)

func TestReadSizeConfigurationDefaultsBoundsAndAdapterMapping(t *testing.T) {
	backends := []string{
		"      mongodb:\n        uri: mongodb://127.0.0.1:27017\n",
		"      search:\n        url: http://127.0.0.1:9200\n",
	}
	suffix := "routes:\n  - store: records\n    service: database\n"
	cases := []struct {
		field string
		want  int
	}{
		{"", protocol.MaxDocument},
		{"      max_read_size: 1KiB\n", 1024},
		{"      max_read_size: 16KiB\n", 16 << 10},
		{"      max_read_size: 2MiB\n", protocol.MaxDocument},
	}
	for _, backend := range backends {
		prefix := "services:\n  - name: database\n    local:\n" + backend
		for _, tc := range cases {
			cfg, err := DecodeRouting(strings.NewReader(prefix + tc.field + suffix))
			if err != nil {
				t.Fatal("valid read-size declaration rejected", err)
			}
			local := cfg.Services[0].Local
			var got int
			if local.MongoDB != nil {
				got = local.mongoConfig("database").MaxReadSize
			} else {
				got = local.searchConfig("database").MaxReadSize
			}
			if got != tc.want {
				t.Fatal("adapter lost normalized read-size declaration", got, tc.want)
			}
			raw, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			roundTrip, err := DecodeRouting(strings.NewReader(string(raw)))
			if err != nil || len(roundTrip.Services) != 1 {
				t.Fatal("read-size declaration could not round trip", err)
			}
			roundLocal := roundTrip.Services[0].Local
			if tc.field == "" && roundLocal.MaxReadSize != nil || tc.field != "" && (roundLocal.MaxReadSize == nil || int(*roundLocal.MaxReadSize) != tc.want) {
				t.Fatal("read-size declaration changed during YAML round trip")
			}
		}
		for _, value := range []string{"0B", "1023B", "2097153B", "18446744073709551615B", "1GB", "1024", "null", "true", "{}", "[]"} {
			input := prefix + "      max_read_size: " + value + "\n" + suffix
			if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
				t.Fatal("invalid read-size declaration accepted", value)
			}
		}
	}
}
