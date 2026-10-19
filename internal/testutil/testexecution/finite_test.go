package testexecution

import (
	"testing"
	"time"
)

func TestExecutionProfileContractFiniteCapability(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		p, err := LinuxFunctionalProfile(arch)
		if err != nil || validateProfile(p) != nil {
			t.Fatal("native finite profile rejected", err)
		}
		if p.Duration() != 60*time.Second || p.CapacityMiB() != 1024 || FunctionalDocuments != 16 || FunctionalDocumentBytes != 64<<10 {
			t.Fatal("finite bounds changed")
		}
		v := &ValidatedInputs{valid: true, profile: p}
		if err := v.RequireExecutable(); err != nil {
			t.Fatal(err)
		}
		in := ProvenanceInputs{}
		env := map[string]string{"WEIR_SEARCH_INTEGRATION": "elasticsearch"}
		if err := CheckLegacyEnv(p, in, env); err != nil {
			t.Fatal("single backend legacy env check", err)
		}
		mutations := map[string]func(*ExecutionProfile){
			"formal":     func(p *ExecutionProfile) { p.Intent = "formal" },
			"smoke":      func(p *ExecutionProfile) { p.Intent = "smoke" },
			"TLS":        func(p *ExecutionProfile) { p.Backends[0].Transport = "tls" },
			"remote":     func(p *ExecutionProfile) { p.Backends[0].Endpoints.Driver = "remote" },
			"OS":         func(p *ExecutionProfile) { p.Driver.OS = "darwin" },
			"mixed arch": func(p *ExecutionProfile) { p.CLI.Arch = "386" },
			"version":    func(p *ExecutionProfile) { p.Backends[0].Version = "8.19.21" },
			"product":    func(p *ExecutionProfile) { p.Backends[0].Product = "opensearch" },
			"load suite": func(p *ExecutionProfile) { p.Suite = "load" },
			"unknown ID": func(p *ExecutionProfile) { p.ID = "finite-v2" },
		}
		for name, mutate := range mutations {
			t.Run(arch+"/"+name, func(t *testing.T) {
				other, _ := LinuxFunctionalProfile(arch)
				mutate(&other)
				if validateProfile(other) == nil {
					t.Fatal("accepted unsupported finite combination")
				}
			})
		}
	}
	if _, err := LinuxFunctionalProfile("386"); err == nil {
		t.Fatal("accepted unsupported architecture")
	}
	// Every previously declared Linux formal/smoke/fault cell stays fail-closed.
	for _, suite := range []string{"load", "fault"} {
		for _, intent := range []string{"formal", "smoke"} {
			p, _ := LegacyProfile("legacy-desktop-load-v1")
			p.ID = "linux-native-" + suite + "-v1"
			p.Suite = suite
			p.Intent = intent
			p.Executor = LinuxExecutor
			target := Target{OS: "linux", Arch: "amd64"}
			p.Driver = target
			p.CLI = target
			for i := range p.Backends {
				p.Backends[i].Endpoints = EndpointRoles{Driver: "owned-local", CLI: "owned-local"}
			}
			if err := validateProfile(p); err != nil {
				t.Fatal(err)
			}
			v := &ValidatedInputs{valid: true, profile: p}
			if v.RequireExecutable() == nil {
				t.Fatal("formal/smoke capability accidentally opened", suite, intent)
			}
		}
	}
}
