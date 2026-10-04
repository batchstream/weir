package app

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
)

// Config contains the separately loaded process and local Store settings.
type Config struct {
	Basic   BasicConfig   `json:"basic" yaml:"basic"`
	Routing RoutingConfig `json:"routing" yaml:"routing"`
}

// BasicConfig groups settings by their process responsibility.
type BasicConfig struct {
	Listeners   ListenerConfig    `json:"listeners" yaml:"listeners"`
	Diagnostics DiagnosticsConfig `json:"diagnostics" yaml:"diagnostics"`
	Memory      ByteSize          `json:"memory" yaml:"memory"`
	Transport   TransportConfig   `json:"transport" yaml:"transport"`
	Discovery   DiscoveryConfig   `json:"discovery" yaml:"discovery"`
}

type ListenerConfig struct {
	Application string `json:"application" yaml:"application"`
	Peer        string `json:"peer" yaml:"peer"`
}

type DiagnosticsConfig struct {
	Address       string `json:"address" yaml:"address"`
	AllowIntranet bool   `json:"allow_intranet" yaml:"allow_intranet"`
}

// DiscoveryConfig advertises the local Store group and seeds directory synchronization.
type DiscoveryConfig struct {
	Group          string   `json:"group" yaml:"group"`
	PeerAddress    string   `json:"peer_address" yaml:"peer_address"`
	PeerAddressEnv string   `json:"peer_address_env" yaml:"peer_address_env"`
	Seeds          []string `json:"seeds" yaml:"seeds,omitempty"`
	Advertise      []string `json:"advertise" yaml:"advertise,omitempty"`
}

// RoutingConfig declares only Stores hosted by this process.
type RoutingConfig struct {
	Stores []StoreConfig `json:"stores" yaml:"stores"`
}

type StoreConfig struct {
	Name   string `json:"name" yaml:"name"`
	*Local `yaml:",inline"`
}

type Local struct {
	MongoDB            *Mongo    `json:"mongodb" yaml:"mongodb"`
	Search             *Search   `json:"search" yaml:"search"`
	MaxConcurrency     int       `json:"max_concurrency" yaml:"max_concurrency"`
	MaxBatchOperations int       `json:"max_batch_operations" yaml:"max_batch_operations"`
	MaxReadSize        *ByteSize `json:"max_read_size,omitempty" yaml:"max_read_size,omitempty"`
	WorkingMemory      *ByteSize `json:"working_memory,omitempty" yaml:"working_memory,omitempty"`
	BackendTimeout     *Duration `json:"backend_timeout,omitempty" yaml:"backend_timeout,omitempty"`
}

type Mongo struct {
	URI          string `json:"uri" yaml:"uri"`
	Username     string `json:"username" yaml:"username"`
	Password     string `json:"password" yaml:"password"`
	UsernameFile string `json:"username_file" yaml:"username_file"`
	PasswordFile string `json:"password_file" yaml:"password_file"`
}

type Search struct {
	Connection *SearchConnection `json:"connection" yaml:"connection"`
	URL        string            `json:"url" yaml:"url"`
}

type SearchConnection struct {
	Username     string `json:"username" yaml:"username"`
	Password     string `json:"password" yaml:"password"`
	UsernameFile string `json:"username_file" yaml:"username_file"`
	PasswordFile string `json:"password_file" yaml:"password_file"`
	CAFile       string `json:"ca_file" yaml:"ca_file"`
}

type TransportConfig struct {
	MaxConnections int               `json:"max_connections" yaml:"max_connections"`
	MaxSessions    int               `json:"max_sessions" yaml:"max_sessions"`
	Timeouts       TransportTimeouts `json:"timeouts" yaml:"timeouts"`
}

type TransportTimeouts struct {
	Request Duration `json:"request" yaml:"request"`
	Stall   Duration `json:"stall" yaml:"stall"`
}

func DefaultConfig() Config {
	defaults := server.DefaultLimits()
	timeouts := TransportTimeouts{
		Request: Duration(defaults.RequestLifetime),
		Stall:   Duration(defaults.Stall),
	}
	transport := TransportConfig{
		MaxConnections: defaults.Connections,
		MaxSessions:    defaults.Sessions,
		Timeouts:       timeouts,
	}
	basic := BasicConfig{
		Memory:    2 << 30,
		Transport: transport,
	}

	cfg := Config{Basic: basic}
	return cfg
}

func (cfg TransportConfig) serverLimits() server.Limits {
	timeouts := cfg.Timeouts
	limits := server.Limits{
		Connections:     cfg.MaxConnections,
		Sessions:        cfg.MaxSessions,
		RequestLifetime: time.Duration(timeouts.Request),
		Stall:           time.Duration(timeouts.Stall),
	}
	return limits
}

func address(value string, loopback bool) bool {
	host, port, err := net.SplitHostPort(value)
	ip := net.ParseIP(host)
	number, portErr := strconv.Atoi(port)

	return err == nil && ip != nil && (!loopback || ip.IsLoopback()) &&
		port != "" && strings.Trim(port, "0123456789") == "" &&
		portErr == nil && number >= 0 && number <= 65535
}

func validName(name string) bool {
	return protocol.ValidStoreName(name)
}

func (cfg Config) Validate() error {
	if err := cfg.Basic.Validate(); err != nil {
		return err
	}
	if err := cfg.Routing.Validate(); err != nil {
		return err
	}
	if err := cfg.validateDiscovery(); err != nil {
		return err
	}
	if uint64(cfg.Basic.Memory) < cfg.ReservedMemory() {
		return errors.New("process memory budget cannot cover declared Route and Store bounds")
	}
	return nil
}

// ReservedMemory is a conservative application/transport working-set envelope.
// Runtime heap and RSS additionally include GC slack, stacks and driver/native
// allocations; the overload guard enforces the configured process threshold.
func (cfg Config) ReservedMemory() uint64 {
	if cfg.Basic.Transport.serverLimits().Validate() != nil {
		return (64 << 30) + 1
	}
	transportCosts := []uint64{uint64(cfg.Basic.Transport.MaxSessions) * (96 << 20), uint64(cfg.Basic.Transport.MaxConnections) * (256 << 10)}
	budget := addMemoryBudget(64<<20, transportCosts)
	for _, service := range cfg.Routing.Stores {
		if service.Local != nil {
			limits := service.Local.runtimeLimits()
			if limits.Validate() != nil {
				return (64 << 30) + 1
			}
			costs := []uint64{uint64(limits.PendingBytes), uint64(limits.ResultBytes), uint64(limits.WorkingBytes), uint64(limits.Concurrency) * (2 << 20)}
			budget = addMemoryBudget(budget, costs)
		}
	}
	return budget
}

func addMemoryBudget(budget uint64, costs []uint64) uint64 {
	const maximum = 64 << 30
	for _, cost := range costs {
		if budget > maximum || cost > maximum-budget {
			return maximum + 1
		}
		budget += cost
	}
	return budget
}

func (cfg BasicConfig) Validate() error {
	if err := cfg.Discovery.validateSource(); err != nil {
		return err
	}

	if cfg.Diagnostics.AllowIntranet && cfg.Diagnostics.Address == "" {
		return errors.New("diagnostics.allow_intranet requires a diagnostic listener")
	}
	if cfg.Diagnostics.Address != "" && !address(cfg.Diagnostics.Address, !cfg.Diagnostics.AllowIntranet) {
		return errors.New("diagnostics requires explicit IP and port; non-loopback requires diagnostics.allow_intranet")
	}

	if cfg.Listeners.Application == "" && cfg.Listeners.Peer == "" ||
		cfg.Listeners.Application != "" && !address(cfg.Listeners.Application, false) ||
		cfg.Listeners.Peer != "" && !address(cfg.Listeners.Peer, false) {
		return errors.New("invalid listener configuration")
	}
	listeners := []string{cfg.Listeners.Application, cfg.Listeners.Peer, cfg.Diagnostics.Address}
	for i, listener := range listeners {
		if listener == "" {
			continue
		}
		host, port, _ := net.SplitHostPort(listener)
		number, _ := strconv.Atoi(port)
		if number == 0 {
			continue // Each bind requests its own ephemeral port.
		}
		ip := net.ParseIP(host)
		for _, other := range listeners[:i] {
			otherHost, otherPort, _ := net.SplitHostPort(other)
			otherNumber, _ := strconv.Atoi(otherPort)
			otherIP := net.ParseIP(otherHost)
			if number == otherNumber && (ip.Equal(otherIP) || ip.IsUnspecified() || otherIP.IsUnspecified()) {
				return errors.New("duplicate or overlapping listener addresses")
			}
		}
	}

	if cfg.Memory < 64<<20 || cfg.Memory > 64<<30 {
		return errors.New("invalid process bounds")
	}
	if err := cfg.Transport.serverLimits().Validate(); err != nil {
		return err
	}

	return nil
}

func (cfg DiscoveryConfig) validateSource() error {
	if cfg.PeerAddress != "" && cfg.PeerAddressEnv != "" {
		return errors.New("peer address value and environment source are mutually exclusive")
	}
	if cfg.PeerAddressEnv == "" {
		return nil
	}
	if len(cfg.PeerAddressEnv) > 128 {
		return errors.New("invalid peer address environment source")
	}
	for i, c := range cfg.PeerAddressEnv {
		if !(c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || i > 0 && c >= '0' && c <= '9') {
			return errors.New("invalid peer address environment source")
		}
	}
	return nil
}

func (cfg Config) validateDiscovery() error {
	discovery := cfg.Basic.Discovery
	if discovery.PeerAddressEnv != "" {
		return errors.New("unresolved peer address environment source")
	}
	if discovery.Group != "" && !directory.ValidReplicaGroup(discovery.Group) {
		return errors.New("invalid discovery group")
	}
	if len(discovery.Advertise) > 16 || len(discovery.Seeds) > 16 {
		return errors.New("discovery address bound exceeded")
	}
	for _, addresses := range [][]string{discovery.Advertise, discovery.Seeds} {
		if len(addresses) != 0 {
			if _, err := protocol.CanonicalEndpoints(addresses); err != nil {
				return errors.New("invalid discovery addresses")
			}
		}
	}
	if discovery.PeerAddress != "" {
		if _, err := protocol.CanonicalEndpoint(discovery.PeerAddress); err != nil {
			return errors.New("invalid advertised peer address")
		}
		if cfg.Basic.Listeners.Peer == "" {
			return errors.New("advertised peer address requires peer listener")
		}
	}
	if len(discovery.Seeds) != 0 && cfg.Basic.Listeners.Peer == "" {
		return errors.New("discovery seeds require peer listener")
	}
	if len(cfg.Routing.Stores) != 0 && cfg.Basic.Listeners.Application == "" {
		return errors.New("local Stores require application listener")
	}
	if len(discovery.Advertise) == 0 && len(cfg.Routing.Stores) != 0 {
		host, _, _ := net.SplitHostPort(cfg.Basic.Listeners.Application)
		if net.ParseIP(host).IsUnspecified() {
			return errors.New("wildcard application listener requires discovery.advertise")
		}
	}
	if cfg.Basic.Listeners.Peer != "" && discovery.PeerAddress == "" {
		host, _, _ := net.SplitHostPort(cfg.Basic.Listeners.Peer)
		if net.ParseIP(host).IsUnspecified() {
			return errors.New("wildcard peer listener requires discovery.peer_address")
		}
	}
	return nil
}

func (cfg RoutingConfig) Validate() error {
	if err := cfg.validateGraph(); err != nil {
		return err
	}
	if err := cfg.validateCredentialSources(); err != nil {
		return err
	}
	for _, service := range cfg.Stores {

		if l := service.Local; l != nil {
			if l.BackendTimeout != nil && *l.BackendTimeout <= 0 {
				return errors.New("backend_timeout must be positive")
			}
			if l.MaxReadSize != nil && (*l.MaxReadSize < 1<<10 || *l.MaxReadSize > protocol.MaxDocument) {
				return errors.New("max_read_size must be between 1KiB and 2MiB")
			}
			limits := l.runtimeLimits()
			if err := limits.Validate(); err != nil {
				return err
			}

			if m := l.MongoDB; m != nil {
				if m.UsernameFile != "" || m.PasswordFile != "" {
					return errors.New("unresolved credential file")
				}
				config := l.mongoConfig(service.Name)
				if err := mongodb.ValidateConfig(config); err != nil {
					return err
				}
			}

			if l.Search != nil {
				if c := l.Search.Connection; c != nil && (c.UsernameFile != "" || c.PasswordFile != "") {
					return errors.New("unresolved credential file")
				}
				config := l.searchConfig(service.Name)
				if err := search.ValidateConfig(config); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (cfg RoutingConfig) validateGraph() error {
	if len(cfg.Stores) > 16 {
		return errors.New("invalid local Store bounds")
	}
	names := make(map[string]bool)
	for _, definition := range cfg.Stores {
		if !validName(definition.Name) || names[definition.Name] || definition.Local == nil {
			return errors.New("invalid or duplicate Store")
		}
		if (definition.MongoDB == nil) == (definition.Search == nil) {
			return errors.New("LocalStore requires exactly one adapter")
		}
		names[definition.Name] = true
	}
	return nil
}

func (l *Local) runtimeLimits() store.Limits {
	limits := store.DefaultLimits()
	if l.BackendTimeout != nil {
		limits.BackendTimeout = time.Duration(*l.BackendTimeout)
	}
	if l.MaxConcurrency != 0 {
		limits.Concurrency = l.MaxConcurrency
	}
	if l.MaxBatchOperations != 0 {
		limits.BatchOperations = l.MaxBatchOperations
	}
	if l.WorkingMemory != nil {
		limits.WorkingBytes = int(*l.WorkingMemory)
		if uint64(*l.WorkingMemory) > uint64(^uint(0)>>1) {
			limits.WorkingBytes = -1
		}
	}

	return limits
}

func (l *Local) searchConfig(name string) search.Config {
	var connection *search.Connection
	if c := l.Search.Connection; c != nil {
		connection = &search.Connection{
			Username: c.Username,
			Password: c.Password,
			CAFile:   c.CAFile,
		}
	}
	cfg := search.Config{
		Store:       name,
		URL:         l.Search.URL,
		Pool:        l.runtimeLimits().Concurrency,
		Connection:  connection,
		MaxReadSize: execution.DefaultMaxReadSize,
	}
	if l.MaxReadSize != nil {
		cfg.MaxReadSize = int(*l.MaxReadSize)
	}
	return cfg
}

func (l *Local) mongoConfig(name string) mongodb.Config {
	m := l.MongoDB
	cfg := mongodb.Config{
		URI:         m.URI,
		Username:    m.Username,
		Password:    m.Password,
		Store:       name,
		Pool:        uint64(l.runtimeLimits().Concurrency),
		MaxReadSize: execution.DefaultMaxReadSize,
	}
	if l.MaxReadSize != nil {
		cfg.MaxReadSize = int(*l.MaxReadSize)
	}
	return cfg
}
