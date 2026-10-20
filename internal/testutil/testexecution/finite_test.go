package testexecution

import (
	"testing"
	"time"
)

func TestExecutionProfileContractFiniteCapability(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		p, err := LinuxFunctionalProfile(LinuxFunctionalID, arch)
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
				other, _ := LinuxFunctionalProfile(LinuxFunctionalID, arch)
				mutate(&other)
				if validateProfile(other) == nil {
					t.Fatal("accepted unsupported finite combination")
				}
			})
		}
	}
	if _, err := LinuxFunctionalProfile(LinuxFunctionalID, "386"); err == nil {
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

func TestExecutionProfileContractOpenSearchFiniteExact(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		for _, id := range []string{LinuxFunctionalID, LinuxOpenSearchFunctionalID} {
			profile, err := LinuxFunctionalProfile(id, arch)
			if err != nil || validateProfile(profile) != nil {
				t.Fatal(err)
			}
			backend := profile.Backends[0]
			if profile.Duration() != FunctionalDeadline {
				t.Fatal("wrong finite deadline")
			}
			good := map[string]string{"WEIR_SEARCH_INTEGRATION": backend.Product}
			inputs := ProvenanceInputs{}
			if err := CheckLegacyEnv(profile, inputs, good); err != nil {
				t.Fatal(err)
			}
			if err := CheckLegacyEnv(profile, inputs, good); err != nil {
				t.Fatal("revalidation", err)
			}
			bad := map[string]string{"WEIR_SEARCH_INTEGRATION": "opensearch"}
			if id == LinuxOpenSearchFunctionalID {
				bad["WEIR_SEARCH_INTEGRATION"] = "elasticsearch"
			}
			if CheckLegacyEnv(profile, inputs, bad) == nil {
				t.Fatal("cross-product conflict accepted")
			}
			mutations := []func(*ExecutionProfile){
				func(p *ExecutionProfile) { p.ID = "linux-native-opensearch-functional-v2" },
				func(p *ExecutionProfile) { p.Backends[0].Product = "unknown" },
				func(p *ExecutionProfile) { p.Backends[0].Version = "3.3.1" },
				func(p *ExecutionProfile) {
					p.ID = LinuxFunctionalID
					if id == LinuxFunctionalID {
						p.ID = LinuxOpenSearchFunctionalID
					}
				},
				func(p *ExecutionProfile) { p.Intent = "formal" }, func(p *ExecutionProfile) { p.Backends[0].Transport = "tls" },
				func(p *ExecutionProfile) { p.CLI.Arch = "386" }, func(p *ExecutionProfile) { p.Driver.OS = "darwin" },
				func(p *ExecutionProfile) { p.Backends[0].Endpoints.CLI = "remote" }, func(p *ExecutionProfile) { p.Suite = "load" },
				func(p *ExecutionProfile) { p.Executor = LegacyExecutor }, func(p *ExecutionProfile) { p.Policy = "other" },
			}
			for _, mutate := range mutations {
				other, _ := LinuxFunctionalProfile(id, arch)
				mutate(&other)
				if validateProfile(other) == nil {
					t.Fatal("cross-field capability accepted", other)
				}
			}
		}
	}
	if _, err := LinuxFunctionalProfile("unknown", "amd64"); err == nil {
		t.Fatal("unknown ID accepted")
	}
}
