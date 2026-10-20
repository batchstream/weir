// Package testexecution validates test-only execution inputs. It does not attest
// workflow origin, registry contents, runtime controls, or qualification results.
package testexecution

import (
	"fmt"
	"reflect"
	"time"
)

const SchemaVersion = 1
const PolicyID = "production-v1"
const LegacyExecutor = "legacy-desktop-v1"
const LinuxExecutor = "linux-native-v1"
const LinuxFunctionalID = "linux-native-es-functional-v1"
const LinuxOpenSearchFunctionalID = "linux-native-opensearch-functional-v1"
const FunctionalDeadline = 60 * time.Second
const FunctionalDocuments = 16
const FunctionalDocumentBytes = 64 << 10

// LinuxFunctionalProfile is a finite correctness capability, never a load,
// fault, smoke, TLS or formal qualification capability.
func LinuxFunctionalProfile(id, arch string) (ExecutionProfile, error) {
	target := Target{OS: "linux", Arch: arch}
	roles := EndpointRoles{Driver: "owned-local", CLI: "owned-local"}
	backend := Backend{Product: "elasticsearch", Version: "8.19.22", Transport: "plain", Endpoints: roles}
	switch id {
	case LinuxFunctionalID:
	case LinuxOpenSearchFunctionalID:
		backend.Product, backend.Version = "opensearch", "3.3.0"
	default:
		empty := ExecutionProfile{}
		return empty, fmt.Errorf("unknown finite profile %q", id)
	}
	p := ExecutionProfile{SchemaVersion: SchemaVersion, ID: id, Suite: "functional", Intent: "finite", Policy: PolicyID, Executor: LinuxExecutor, Driver: target, CLI: target, Backends: []Backend{backend}}
	if arch != "amd64" && arch != "arm64" {
		return p, fmt.Errorf("unsupported native Linux architecture")
	}
	return p, nil
}

// Workload and resource policy is code, never caller-supplied JSON tuning.
const (
	Warmup            = 30 * time.Second
	Measurement       = 600 * time.Second
	SmokeMeasurement  = 120 * time.Second
	ArrivalRPC        = 200
	SlowConsumers     = 32
	SlowConsumerBytes = 8 << 20
	Burst             = 256
	ReadP99           = 100 * time.Millisecond
	OrdinaryP99       = 250 * time.Millisecond
	LuaBatchP99       = 500 * time.Millisecond
	Maximum           = 3 * time.Second
	Recovery          = 5 * time.Second
	FaultHealthy      = 3 * time.Second
	FaultCancel       = 3 * time.Second
	Shutdown          = 8 * time.Second
	CPUs              = 2
	LoadMiB           = 1024
	FaultMiB          = 512
	PIDs              = 96
)

type Target struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

func (t Target) String() string { return t.OS + "/" + t.Arch }

type EndpointRoles struct {
	Driver string `json:"driver"`
	CLI    string `json:"cli"`
}
type Backend struct {
	Product   string        `json:"product"`
	Version   string        `json:"version"`
	Transport string        `json:"transport"`
	Endpoints EndpointRoles `json:"endpoints"`
}
type ExecutionProfile struct {
	SchemaVersion int       `json:"schemaVersion"`
	ID            string    `json:"id"`
	Suite         string    `json:"suite"`
	Intent        string    `json:"intent"`
	Policy        string    `json:"policy"`
	Executor      string    `json:"executor"`
	Driver        Target    `json:"driver"`
	CLI           Target    `json:"cli"`
	Backends      []Backend `json:"backends"`
}

// LegacyProfile names the historical Desktop-only workload. Even its 600s load
// is reproduction, not Linux/backend-resource or formal production evidence.
func LegacyProfile(id string) (ExecutionProfile, error) {
	p := ExecutionProfile{SchemaVersion: SchemaVersion, ID: id, Policy: PolicyID, Executor: LegacyExecutor}
	p.Driver = Target{OS: "darwin", Arch: "arm64"}
	p.CLI = Target{OS: "linux", Arch: "arm64"}
	mongoRoles := EndpointRoles{Driver: "loopback", CLI: "desktop-host"}
	searchRoles := EndpointRoles{Driver: "loopback", CLI: "desktop-bridge"}
	switch id {
	case "legacy-desktop-load-v1":
		p.Suite, p.Intent = "load", "legacy"
	case "legacy-desktop-smoke-v1":
		p.Suite, p.Intent = "load", "smoke"
	case "legacy-desktop-fault-v1":
		p.Suite, p.Intent = "fault", "legacy"
		searchRoles.CLI = "desktop-proxy"
		mongoRoles.CLI = "desktop-proxy"
	default:
		return p, fmt.Errorf("unknown legacy profile %q", id)
	}
	mongo := Backend{Product: "mongodb", Version: "8.0.32", Transport: "plain", Endpoints: mongoRoles}
	search := Backend{Product: "elasticsearch", Version: "8.19.22", Transport: "plain", Endpoints: searchRoles}
	p.Backends = []Backend{mongo, search}
	return p, nil
}
func validateProfile(p ExecutionProfile) error {
	if p.SchemaVersion != SchemaVersion || p.Policy != PolicyID {
		return fmt.Errorf("unsupported schema/policy")
	}
	if p.Executor == LegacyExecutor {
		expected, err := LegacyProfile(p.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(p, expected) {
			return fmt.Errorf("legacy profile conflicts with named immutable spec")
		}
		return nil
	}
	if p.ID == LinuxFunctionalID || p.ID == LinuxOpenSearchFunctionalID {
		expected, err := LinuxFunctionalProfile(p.ID, p.Driver.Arch)
		if err != nil || !reflect.DeepEqual(p, expected) {
			return fmt.Errorf("finite functional profile conflicts with immutable spec")
		}
		return nil
	}
	if p.Executor != LinuxExecutor || p.ID != "linux-native-"+p.Suite+"-v1" || (p.Suite != "load" && p.Suite != "fault") || (p.Intent != "formal" && p.Intent != "smoke") {
		return fmt.Errorf("unknown profile/executor/suite/intent")
	}
	if p.CLI.OS != "linux" || p.Driver != p.CLI || (p.CLI.Arch != "amd64" && p.CLI.Arch != "arm64") {
		return fmt.Errorf("native Linux driver/CLI targets required")
	}
	if len(p.Backends) != 2 || p.Backends[0].Product != "mongodb" || p.Backends[0].Version != "8.0.32" {
		return fmt.Errorf("missing pinned MongoDB backend")
	}
	for i, b := range p.Backends {
		if b.Transport != "plain" && b.Transport != "tls" {
			return fmt.Errorf("unknown transport")
		}
		if b.Endpoints.Driver != "owned-local" || b.Endpoints.CLI != "owned-local" {
			return fmt.Errorf("Linux endpoints must use owned-local roles, not remote addresses")
		}
		if i == 1 && !((b.Product == "elasticsearch" && b.Version == "8.19.22") || (b.Product == "opensearch" && b.Version == "3.3.0")) {
			return fmt.Errorf("unknown search product/version")
		}
	}
	return nil
}
func (p ExecutionProfile) Duration() time.Duration {
	if p.ID == LinuxFunctionalID || p.ID == LinuxOpenSearchFunctionalID {
		return FunctionalDeadline
	}
	if p.Intent == "smoke" {
		return SmokeMeasurement
	}
	return Measurement
}
func (p ExecutionProfile) CapacityMiB() int {
	if p.Suite == "fault" {
		return FaultMiB
	}
	return LoadMiB
}

// NativePlainLoad names the implemented formal Linux load capability.
func (p ExecutionProfile) NativePlainLoad() bool {
	return validateProfile(p) == nil && p.Executor == LinuxExecutor && p.ID == "linux-native-load-v1" && p.Suite == "load" && p.Intent == "formal" && p.Driver.OS == "linux" && p.Driver.Arch == "amd64" && p.CLI == p.Driver && len(p.Backends) == 2 && p.Backends[0].Product == "mongodb" && p.Backends[0].Transport == "plain" && (p.Backends[1].Product == "elasticsearch" || p.Backends[1].Product == "opensearch") && p.Backends[1].Transport == "plain"
}

// NativePlainFault deliberately admits only the exact native plain fault cell.
func (p ExecutionProfile) NativePlainFault() bool {
	if validateProfile(p) != nil || p.ID != "linux-native-fault-v1" || p.Suite != "fault" {
		return false
	}
	p.ID, p.Suite = "linux-native-load-v1", "load"
	return p.NativePlainLoad()
}

// NativeTLSLoad admits only matching verified backend TLS cells. The application
// listener remains unchanged; this does not assert client-to-Weir TLS.
func (p ExecutionProfile) NativeTLSLoad() bool {
	if validateProfile(p) != nil || len(p.Backends) != 2 || p.Backends[0].Transport != "tls" || p.Backends[1].Transport != "tls" {
		return false
	}
	p.Backends = append([]Backend(nil), p.Backends...)
	p.Backends[0].Transport, p.Backends[1].Transport = "plain", "plain"
	return p.NativePlainLoad()
}

func (p ExecutionProfile) NativeTLSFault() bool {
	if validateProfile(p) != nil || p.ID != "linux-native-fault-v1" || p.Suite != "fault" {
		return false
	}
	p.ID, p.Suite = "linux-native-load-v1", "load"
	return p.NativeTLSLoad()
}

// RequireExecutable is deliberately separate from parsing and byte validation.
// Unsupported declarations must not downgrade into an implemented capability.
// Native load is restricted independently of the broader declarable profile schema.
func (v *ValidatedInputs) RequireExecutable() error {
	if v == nil || !v.valid {
		return fmt.Errorf("validated inputs required")
	}
	if v.profile.Executor != LegacyExecutor && v.profile.ID != LinuxFunctionalID && v.profile.ID != LinuxOpenSearchFunctionalID && !v.profile.NativePlainLoad() && !v.profile.NativePlainFault() && !v.profile.NativeTLSLoad() && !v.profile.NativeTLSFault() {
		return fmt.Errorf("executor %q is declared but not implemented; no fixtures may be created", v.profile.Executor)
	}
	return nil
}
func (v *ValidatedInputs) Profile() ExecutionProfile {
	p := v.profile
	p.Backends = append([]Backend(nil), p.Backends...)
	return p
}
func (v *ValidatedInputs) CLIPath() string       { return v.inputs.CLI.Path }
func (v *ValidatedInputs) CLISHA256() string     { return v.inputs.CLI.SHA256 }
func (v *ValidatedInputs) ServerSource() Source  { return v.inputs.Server }
func (v *ValidatedInputs) HarnessSource() Source { return v.inputs.Harness }
func (v *ValidatedInputs) UnmarshalJSON([]byte) error {
	return fmt.Errorf("validated inputs cannot be decoded or self-attested")
}
