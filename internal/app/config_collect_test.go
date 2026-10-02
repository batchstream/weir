package app

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestBatchCollectConfigurationDefaultExplicitZeroAndBounds(t *testing.T) {
	prefix := "services:\n  - name: database\n    local:\n      mongodb:\n        uri: mongodb://127.0.0.1:27017\n"
	suffix := "routes:\n  - store: records\n    service: database\n"
	cases := []struct {
		field string
		want  time.Duration
	}{
		{"", 5 * time.Millisecond},
		{"      batch_collect: 0ms\n", 0},
		{"      batch_collect: 1ms\n", time.Millisecond},
		{"      batch_collect: 1.5ms\n", 1500 * time.Microsecond},
		{"      batch_collect: 5ms\n", 5 * time.Millisecond},
		{"      batch_collect: 10ms\n", 10 * time.Millisecond},
	}
	for _, tc := range cases {
		cfg, err := DecodeRouting(strings.NewReader(prefix + tc.field + suffix))
		if err != nil {
			t.Fatal("valid collection interval rejected", tc.field, err)
		}
		local := cfg.Services[0].Local
		if got := local.runtimeLimits().Collect; got != tc.want {
			t.Fatal("collection default or explicit value lost", got, tc.want)
		}
		raw, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		roundTrip, err := DecodeRouting(strings.NewReader(string(raw)))
		if err != nil || roundTrip.Services[0].Local.runtimeLimits().Collect != tc.want {
			t.Fatal("collection interval changed in routing round trip", err)
		}
		encoded, err := json.Marshal(local)
		if err != nil || tc.field != "" && !strings.Contains(string(encoded), `"batch_collect":"`+tc.want.String()+`"`) {
			t.Fatal("diagnostic collection interval lost its units", string(encoded), err)
		}
	}
	for _, value := range []string{"-1ns", "10.000001ms", "1s", "0", "true", "null", "{}", "[]", "\"\""} {
		input := prefix + "      batch_collect: " + value + "\n" + suffix
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("invalid collection interval accepted", value)
		}
	}
}
