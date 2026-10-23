package app

import (
	"fmt"

	"github.com/iceisfun/golua/vm"
)

// EffectiveConfig reports resolved operational settings and startup derivations.
// It is an inspection report, not a replacement configuration document.
type EffectiveConfig struct {
	Basic     BasicConfig       `json:"basic" yaml:"basic"`
	Routing   RoutingConfig     `json:"routing" yaml:"routing"`
	Automatic map[string]string `json:"automatic" yaml:"automatic"`
}

// Effective fills omitted Store defaults and redacts authentication and CA paths.
// It performs no IO and leaves the loaded configuration unchanged. Addresses and
// Store names remain visible; validation forbids credentials in backend URLs.
func (cfg Config) Effective() (EffectiveConfig, error) {
	report := EffectiveConfig{}
	resolved, err := cfg.Resolve()
	if err != nil {
		return report, err
	}
	report.Basic = resolved.Basic
	report.Routing = resolved.Routing
	report.Automatic = map[string]string{
		"memory_capacity": "detected at startup from visible container, host and finite process address-space limits; container/OS limits remain the hard boundary",
	}
	report.Automatic["lua.native_defaults"] = fmt.Sprintf("input 0 selects golua defaults: max_call_depth=%d, max_stack_slots=%d", vm.DefaultMaxCallDepth, vm.DefaultMaxStackSlots)
	for _, definition := range report.Routing.Stores {
		local := definition.Local
		if local.Backend.Authentication != nil {
			authentication := Credentials{Username: "<redacted>", Password: "<redacted>"}
			local.Backend.Authentication = &authentication
		}
		if local.Backend.TLS != nil {
			trust := BackendTLS{CAFile: "<redacted>"}
			local.Backend.TLS = &trust
		}
	}
	return report, nil
}
