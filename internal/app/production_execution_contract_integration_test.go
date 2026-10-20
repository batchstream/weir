//go:build integration

package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testexecution"
)

// Exact offline selector: no opt-in, fixtures, Docker, listener, or CLI startup.
func TestExecutionProfileContractLegacyIdentity(t *testing.T) {
	id := strings.Repeat("a", 64)
	valid := id + " weir-search-local " + legacySearchImage + " 172.17.0.2 true"
	actual, address, err := validateLegacySearch(valid)
	if err != nil || actual != id || address.String() != "172.17.0.2" {
		t.Fatal("dynamic full instance identity not captured", err)
	}
	for name, raw := range map[string]string{
		"short ID": strings.Replace(valid, id, id[:12], 1), "wrong owner": strings.Replace(valid, "weir-search-local", "unowned", 1),
		"wrong image": strings.Replace(valid, legacySearchImage, "elasticsearch:latest", 1), "stopped": strings.Replace(valid, " true", " false", 1),
		"public endpoint": strings.Replace(valid, "172.17.0.2", "8.8.8.8", 1), "missing instance": "",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := validateLegacySearch(raw); err == nil {
				t.Fatal("accepted wrong fixture identity")
			}
		})
	}
	if err := validateContainerOwnership(id+" owned", id, "owned"); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{id + " unowned", strings.Repeat("b", 64) + " owned", id[:12] + " owned", ""} {
		if err := validateContainerOwnership(raw, id, "owned"); err == nil {
			t.Fatal("cleanup would stop/remove changed ID/owner")
		}
	}
	opts := memoryRecoveryOptions{}
	if err := validateProductionCLIOptions(opts); err == nil {
		t.Fatal("production CLI accepted unvalidated options")
	}
}
func TestExecutionProfileContractFixedPolicy(t *testing.T) {
	if testexecution.Warmup != 30*time.Second || testexecution.Measurement != 600*time.Second || testexecution.SmokeMeasurement != 120*time.Second || testexecution.ArrivalRPC != 200 || testexecution.SlowConsumers != 32 || testexecution.SlowConsumerBytes != 8<<20 || testexecution.Burst != 256 {
		t.Fatal("load policy changed")
	}
	if testexecution.ReadP99 != 100*time.Millisecond || testexecution.OrdinaryP99 != 250*time.Millisecond || testexecution.LuaBatchP99 != 500*time.Millisecond || testexecution.Maximum != 3*time.Second || testexecution.Recovery != 5*time.Second || testexecution.FaultHealthy != 3*time.Second || testexecution.FaultCancel != 3*time.Second || testexecution.Shutdown != 8*time.Second {
		t.Fatal("latency/recovery policy changed")
	}
	if testexecution.CPUs != 2 || testexecution.LoadMiB != 1024 || testexecution.FaultMiB != 512 || testexecution.PIDs != 96 {
		t.Fatal("CLI quota changed")
	}
	root := testutil.Root(t)
	raw, err := os.ReadFile(filepath.Join(root, "internal", "app", "memory_recovery_integration_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"--read-only"`, `"--cap-drop=ALL"`, `"--security-opt=no-new-privileges"`, `"--user=65532:65532"`, `"--memory=%dm", opts.capacityMiB`, `"--memory-swap=%dm", opts.capacityMiB`, `"--cpus=2"`, `"--pids-limit=96"`} {
		if !strings.Contains(string(raw), required) {
			t.Fatal("CLI resource/security option lost", required)
		}
	}
}
func TestExecutionProfileContractConsumerOrder(t *testing.T) {
	root := testutil.Root(t)
	entries := map[string]string{"production_qualification_integration_test.go": "TestProductionProcessQualification", "production_fault_integration_test.go": "TestProductionProcessFaultQualification"}
	for file, symbol := range entries {
		path := filepath.Join(root, "internal", "app", file)
		tree, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var consumer *ast.FuncDecl
		for _, decl := range tree.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == symbol {
				consumer = fn
			}
		}
		if consumer == nil {
			t.Fatal("actual production entrypoint missing", symbol)
		}
		// The only statement allowed before preflight is the explicit non-opt-in skip.
		if len(consumer.Body.List) < 2 {
			t.Fatal("consumer missing preflight")
		}
		optin, ok := consumer.Body.List[0].(*ast.IfStmt)
		if !ok {
			t.Fatal("consumer lost explicit opt-in")
		}
		skips := 0
		ast.Inspect(optin, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Skip" {
					skips++
				}
			}
			return true
		})
		if skips != 1 {
			t.Fatal("non-opted-in skip boundary changed")
		}
		assignment, ok := consumer.Body.List[1].(*ast.AssignStmt)
		if !ok || len(assignment.Rhs) != 1 {
			t.Fatal("preflight is not first opted-in operation")
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok {
			t.Fatal("missing execution preflight call")
		}
		function, ok := call.Fun.(*ast.Ident)
		if !ok || function.Name != "loadProductionExecution" {
			t.Fatal("fixture could precede validated execution preflight")
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), "execution: execution") || strings.Contains(string(raw), "qualification=%t") {
			t.Fatal("production migration omitted explicit validated CLI option or can claim qualification")
		}
	}
}

// Product/transport selection must follow strict validation, and repeat
// validation must accept the derived opt-ins for finite and formal cells.
func TestExecutionProfileContractFiniteProductPropagation(t *testing.T) {
	for _, id := range []string{testexecution.LinuxFunctionalID, testexecution.LinuxOpenSearchFunctionalID} {
		profile, err := testexecution.LinuxFunctionalProfile(id, "amd64")
		if err != nil {
			t.Fatal(err)
		}
		product := profile.Backends[0].Product
		provenance := testexecution.ProvenanceInputs{ProfileID: id}
		for _, supplied := range []map[string]string{nil, {"WEIR_SEARCH_INTEGRATION": product}} {
			if err := testexecution.CheckLegacyEnv(profile, provenance, supplied); err != nil {
				t.Fatal(err)
			}
		}
		derived := map[string]string{"WEIR_INTEGRATION": "1", "WEIR_SEARCH_INTEGRATION": product, "WEIR_MONGO_PROFILE": ""}
		for range 2 {
			if err := testexecution.CheckLegacyEnv(profile, provenance, derived); err != nil {
				t.Fatal("revalidation", err)
			}
		}
		for _, wrong := range []string{"unknown", "opensearch-2.19", "elasticsearch", "opensearch"} {
			if wrong == product {
				continue
			}
			conflict := map[string]string{"WEIR_SEARCH_INTEGRATION": wrong}
			if testexecution.CheckLegacyEnv(profile, provenance, conflict) == nil {
				t.Fatal("conflicting product accepted", id, wrong)
			}
		}
	}
	for _, suite := range []string{"load", "fault"} {
		for _, product := range []string{"elasticsearch", "opensearch"} {
			for _, transport := range []string{"plain", "tls"} {
				roles := testexecution.EndpointRoles{Driver: "owned-local", CLI: "owned-local"}
				mongo := testexecution.Backend{Product: "mongodb", Version: "8.0.32", Transport: transport, Endpoints: roles}
				version := "8.19.22"
				if product == "opensearch" {
					version = "3.3.0"
				}
				search := testexecution.Backend{Product: product, Version: version, Transport: transport, Endpoints: roles}
				target := testexecution.Target{OS: "linux", Arch: "amd64"}
				profile := testexecution.ExecutionProfile{SchemaVersion: 1, ID: "linux-native-" + suite + "-v1", Suite: suite, Intent: "formal", Policy: testexecution.PolicyID, Executor: testexecution.LinuxExecutor, Driver: target, CLI: target, Backends: []testexecution.Backend{mongo, search}}
				provenance := testexecution.ProvenanceInputs{ProfileID: profile.ID}
				mongoProfile, conflictProfile := "", "tls"
				if transport == "tls" {
					mongoProfile, conflictProfile = "tls", ""
				}
				derived := map[string]string{"WEIR_INTEGRATION": "1", "WEIR_SEARCH_INTEGRATION": product, "WEIR_MONGO_PROFILE": mongoProfile}
				for range 2 {
					if err := testexecution.CheckLegacyEnv(profile, provenance, derived); err != nil {
						t.Fatal("native transport revalidation", suite, product, transport, err)
					}
				}
				conflict := map[string]string{"WEIR_MONGO_PROFILE": conflictProfile}
				if testexecution.CheckLegacyEnv(profile, provenance, conflict) == nil {
					t.Fatal("conflicting Mongo transport opt-in accepted", suite, product, transport)
				}
			}
		}
	}
	root := testutil.Root(t)
	raw, err := os.ReadFile(filepath.Join(root, "internal", "app", "production_execution_integration_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	load := strings.Index(text, "testexecution.Load(opts)")
	executable := strings.Index(text, "inputs.RequireExecutable()")
	harness := strings.Index(text, "inputs.CheckHarnessRoot(")
	optIn := strings.Index(text, `t.Setenv("WEIR_INTEGRATION", "1")`)
	product := strings.Index(text, `t.Setenv("WEIR_SEARCH_INTEGRATION", product)`)
	if load < 0 || executable <= load || harness <= executable || optIn <= harness || product <= optIn {
		t.Fatal("fixture opt-in can precede strict source/profile/provenance/root validation")
	}
	if !strings.Contains(text, "profile := inputs.Profile()") || !strings.Contains(text, "product := profile.Backends[len(profile.Backends)-1].Product") {
		t.Fatal("validated product not propagated")
	}
	transport := "if profile.Backends[0].Transport == \"tls\" {\n\t\tmongoProfile = \"tls\"\n\t}"
	mongoOptIn := strings.Index(text, `t.Setenv("WEIR_MONGO_PROFILE", mongoProfile)`)
	if !strings.Contains(text, `mongoProfile := ""`) || !strings.Contains(text, transport) || mongoOptIn <= product {
		t.Fatal("validated Mongo transport not propagated after strict preflight")
	}
}
