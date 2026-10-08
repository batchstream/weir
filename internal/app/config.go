package app

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/overload"
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
	Lua         LuaConfig         `json:"lua" yaml:"lua"`
	Lifecycle   LifecycleConfig   `json:"lifecycle" yaml:"lifecycle"`
	Listeners   ListenerConfig    `json:"listeners" yaml:"listeners"`
	Diagnostics DiagnosticsConfig `json:"diagnostics" yaml:"diagnostics"`
	Transport   TransportConfig   `json:"transport" yaml:"transport"`
	Discovery   DiscoveryConfig   `json:"discovery" yaml:"discovery"`
	Overload    OverloadConfig    `json:"overload" yaml:"overload"`
}

type ListenerConfig struct {
	Application string `json:"application" yaml:"application"`
	Peer        string `json:"peer" yaml:"peer"`
}

type DiagnosticsConfig struct {
	Timeout       Duration `json:"timeout" yaml:"timeout"`
	Address       string   `json:"address" yaml:"address"`
	AllowIntranet bool     `json:"allow_intranet" yaml:"allow_intranet"`
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
	Backend  BackendConfig  `json:"backend" yaml:"backend"`
	Queue    QueueConfig    `json:"queue" yaml:"queue"`
	Batching BatchingConfig `json:"batching" yaml:"batching"`
	Scan     ScanConfig     `json:"scan" yaml:"scan"`
}

type BackendConfig struct {
	MongoDB              *Mongo       `json:"mongodb,omitempty" yaml:"mongodb,omitempty"`
	Search               *Search      `json:"search,omitempty" yaml:"search,omitempty"`
	Authentication       *Credentials `json:"authentication,omitempty" yaml:"authentication,omitempty"`
	TLS                  *BackendTLS  `json:"tls,omitempty" yaml:"tls,omitempty"`
	ConnectTimeout       *Duration    `json:"connect_timeout,omitempty" yaml:"connect_timeout,omitempty"`
	MetadataCacheEntries *int         `json:"metadata_cache_entries,omitempty" yaml:"metadata_cache_entries,omitempty"`
	MaxExchangeBytes     *ByteSize    `json:"max_exchange_bytes,omitempty" yaml:"max_exchange_bytes,omitempty"`
}

type BackendTLS struct {
	CAFile string `json:"ca_file" yaml:"ca_file"`
}

type BatchingConfig struct {
	MaxOperations int       `json:"max_operations" yaml:"max_operations"`
	MaxBytes      *ByteSize `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
}

type ScanConfig struct {
	MaxBatchDocuments int       `json:"max_batch_documents" yaml:"max_batch_documents"`
	MaxBatchBytes     *ByteSize `json:"max_batch_bytes,omitempty" yaml:"max_batch_bytes,omitempty"`
}

type LuaConfig struct {
	VM     LuaVMConfig    `json:"vm" yaml:"vm"`
	Values LuaValueConfig `json:"values" yaml:"values"`
}

type LuaVMConfig struct {
	MaxInstructions int64 `json:"max_instructions" yaml:"max_instructions"`
	MaxCallDepth    int   `json:"max_call_depth" yaml:"max_call_depth"`
	MaxStackSlots   int   `json:"max_stack_slots" yaml:"max_stack_slots"`
}

type LuaValueConfig struct {
	MaxBytes *ByteSize `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
	MaxDepth int       `json:"max_depth" yaml:"max_depth"`
	MaxNodes int       `json:"max_nodes" yaml:"max_nodes"`
}

type OverloadConfig struct {
	Memory MemoryPressureConfig `json:"memory" yaml:"memory"`
}

type MemoryPressureConfig struct {
	HighWatermark  int      `json:"high_watermark" yaml:"high_watermark"`
	LowWatermark   int      `json:"low_watermark" yaml:"low_watermark"`
	SampleInterval Duration `json:"sample_interval" yaml:"sample_interval"`
}

func (cfg MemoryPressureConfig) limits() overload.Limits {
	limits := overload.Limits{HighWatermark: uint64(cfg.HighWatermark), LowWatermark: uint64(cfg.LowWatermark), SampleInterval: time.Duration(cfg.SampleInterval)}
	return limits
}

// QueueConfig bounds waiting work. Dispatch releases queue capacity;
// running operations and completed results do not consume this capacity.
type QueueConfig struct {
	MaxOperations int       `json:"max_operations" yaml:"max_operations"`
	MaxBytes      *ByteSize `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
}

type Mongo struct {
	Pool MongoPoolConfig `json:"pool" yaml:"pool"`
	URI  string          `json:"uri" yaml:"uri"`
}

type MongoPoolConfig struct {
	MaxConnecting uint64 `json:"max_connecting" yaml:"max_connecting"`
}

type Credentials struct {
	Username     string `json:"username" yaml:"username"`
	Password     string `json:"password" yaml:"password"`
	UsernameFile string `json:"username_file" yaml:"username_file"`
	PasswordFile string `json:"password_file" yaml:"password_file"`
}

type Search struct {
	URL string `json:"url" yaml:"url"`
}

type TransportConfig struct {
	MaxPendingRecords int               `json:"max_pending_records" yaml:"max_pending_records"`
	Timeouts          TransportTimeouts `json:"timeouts" yaml:"timeouts"`
	Keepalive         KeepaliveConfig   `json:"keepalive" yaml:"keepalive"`
}

type KeepaliveConfig struct {
	Interval Duration `json:"interval" yaml:"interval"`
	Timeout  Duration `json:"timeout" yaml:"timeout"`
}

type LifecycleConfig struct {
	StartupTimeout  Duration `json:"startup_timeout" yaml:"startup_timeout"`
	ShutdownTimeout Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

type TransportTimeouts struct {
	Handshake Duration `json:"handshake" yaml:"handshake"`
	Idle      Duration `json:"idle" yaml:"idle"`
	Stall     Duration `json:"stall" yaml:"stall"`
}

func DefaultConfig() Config {
	defaults := server.DefaultLimits()
	timeouts := TransportTimeouts{Stall: Duration(defaults.Stall), Handshake: Duration(defaults.Handshake), Idle: Duration(defaults.Idle)}
	keepalive := KeepaliveConfig{Interval: Duration(defaults.KeepaliveInterval), Timeout: Duration(defaults.KeepaliveTimeout)}
	transport := TransportConfig{Timeouts: timeouts, Keepalive: keepalive, MaxPendingRecords: defaults.MaxPendingRecords}
	memory := MemoryPressureConfig{HighWatermark: 80, LowWatermark: 70, SampleInterval: Duration(100 * time.Millisecond)}
	luaDefaults := luaengine.DefaultLimits()
	valueBytes := ByteSize(luaDefaults.Values.MaxBytes)
	values := LuaValueConfig{MaxBytes: &valueBytes, MaxDepth: luaDefaults.Values.MaxDepth, MaxNodes: luaDefaults.Values.MaxNodes}
	lua := LuaConfig{Values: values}
	basic := BasicConfig{Lua: lua, Lifecycle: LifecycleConfig{StartupTimeout: Duration(5 * time.Second), ShutdownTimeout: Duration(5 * time.Second)}, Transport: transport, Diagnostics: DiagnosticsConfig{Timeout: Duration(diagnosticTimeout)}, Overload: OverloadConfig{Memory: memory}}
	cfg := Config{Basic: basic}
	return cfg
}

func (cfg TransportConfig) serverLimits() server.Limits {
	if cfg.MaxPendingRecords == 0 {
		cfg.MaxPendingRecords = server.RecordStreamItems
	}
	limits := server.Limits{MaxPendingRecords: cfg.MaxPendingRecords, KeepaliveInterval: time.Duration(cfg.Keepalive.Interval), KeepaliveTimeout: time.Duration(cfg.Keepalive.Timeout), Stall: time.Duration(cfg.Timeouts.Stall), Handshake: time.Duration(cfg.Timeouts.Handshake), Idle: time.Duration(cfg.Timeouts.Idle)}
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
	return nil
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

	if cfg.Lifecycle.StartupTimeout <= 0 || cfg.Lifecycle.ShutdownTimeout <= 0 || cfg.Transport.Timeouts.Handshake <= 0 {
		return errors.New("invalid lifecycle or handshake timeout")
	}
	if cfg.Transport.Keepalive.Interval < Duration(time.Second) || cfg.Transport.Keepalive.Timeout <= 0 {
		return errors.New("invalid transport keepalive configuration")
	}
	if cfg.Diagnostics.Timeout <= 0 {
		return errors.New("invalid diagnostics timeout")
	}
	if err := cfg.Overload.Memory.limits().Validate(); err != nil {
		return err
	}
	if err := cfg.Lua.limits().Validate(); err != nil {
		return err
	}
	return cfg.Transport.serverLimits().Validate()
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
	if err := cfg.validateStores(); err != nil {
		return err
	}
	if err := cfg.validateCredentialSources(); err != nil {
		return err
	}
	for _, service := range cfg.Stores {
		l := service.Local
		opts := l.backendOptions()
		if err := opts.Validate(); err != nil {
			return err
		}
		limits := l.runtimeLimits()
		if err := limits.Validate(); err != nil {
			return err
		}

		credentials := l.credentials()
		if credentials != nil && (credentials.UsernameFile != "" || credentials.PasswordFile != "") {
			return errors.New("unresolved credential file")
		}
		if l.Backend.MongoDB != nil {
			config := l.mongoConfig(service.Name)
			if err := mongodb.ValidateConfig(config); err != nil {
				return err
			}
		} else {
			config := l.searchConfig(service.Name)
			if err := search.ValidateConfig(config); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cfg RoutingConfig) validateStores() error {
	if len(cfg.Stores) > 16 {
		return errors.New("invalid local Store bounds")
	}
	names := make(map[string]bool)
	for _, definition := range cfg.Stores {
		if !protocol.ValidStoreName(definition.Name) || names[definition.Name] || definition.Local == nil {
			return errors.New("invalid or duplicate Store")
		}
		if (definition.Backend.MongoDB == nil) == (definition.Backend.Search == nil) {
			return errors.New("LocalStore requires exactly one adapter")
		}
		names[definition.Name] = true
	}
	return nil
}

func (l *Local) runtimeLimits() store.Limits {
	limits := store.DefaultLimits()
	if l.Batching.MaxOperations != 0 {
		limits.BatchOperations = l.Batching.MaxOperations
	}
	if l.Batching.MaxBytes != nil {
		limits.BatchBytes = boundedSize(*l.Batching.MaxBytes)
	}
	if l.Queue.MaxOperations != 0 {
		limits.QueueOperations = l.Queue.MaxOperations
	}
	if l.Queue.MaxBytes != nil {
		limits.QueueBytes = boundedSize(*l.Queue.MaxBytes)
	}
	limits.Scan = l.backendOptions().Scan
	return limits
}

func (l *Local) backendOptions() backend.Options {
	opts := backend.DefaultOptions()
	if l.Backend.ConnectTimeout != nil {
		opts.ConnectTimeout = time.Duration(*l.Backend.ConnectTimeout)
	}
	if l.Backend.MetadataCacheEntries != nil {
		opts.MetadataCacheEntries = *l.Backend.MetadataCacheEntries
	}
	if l.Backend.MaxExchangeBytes != nil {
		opts.ExchangeBytes = boundedSize(*l.Backend.MaxExchangeBytes)
	}
	if l.Scan.MaxBatchDocuments != 0 {
		opts.Scan.Documents = l.Scan.MaxBatchDocuments
	}
	if l.Scan.MaxBatchBytes != nil {
		opts.Scan.Bytes = boundedSize(*l.Scan.MaxBatchBytes)
	}
	return opts
}

func (cfg LuaConfig) limits() luaengine.Limits {
	limits := luaengine.DefaultLimits()
	limits.MaxInstructions = cfg.VM.MaxInstructions
	limits.MaxCallDepth = cfg.VM.MaxCallDepth
	limits.MaxStackSlots = cfg.VM.MaxStackSlots
	if cfg.Values.MaxBytes != nil {
		limits.Values.MaxBytes = boundedSize(*cfg.Values.MaxBytes)
	}
	if cfg.Values.MaxDepth != 0 {
		limits.Values.MaxDepth = cfg.Values.MaxDepth
	}
	if cfg.Values.MaxNodes != 0 {
		limits.Values.MaxNodes = cfg.Values.MaxNodes
	}
	return limits
}

func boundedSize(size ByteSize) int {
	if uint64(size) > uint64(^uint(0)>>1) {
		return -1
	}
	return int(size)
}

func (l *Local) searchConfig(name string) search.Config {
	var connection *search.Connection
	if l.Backend.Authentication != nil || l.Backend.TLS != nil {
		connection = &search.Connection{}
		if c := l.Backend.Authentication; c != nil {
			connection.Username, connection.Password = c.Username, c.Password
		}
		if tls := l.Backend.TLS; tls != nil {
			connection.CAFile = tls.CAFile
		}
	}
	settings := l.backendOptions()
	cfg := search.Config{Options: &settings, Store: name, URL: l.Backend.Search.URL, Connection: connection}
	return cfg
}

func (l *Local) mongoConfig(name string) mongodb.Config {
	m := l.Backend.MongoDB
	settings := l.backendOptions()
	cfg := mongodb.Config{Options: &settings, URI: m.URI, MaxConnecting: m.Pool.MaxConnecting, Store: name}
	if c := l.Backend.Authentication; c != nil {
		cfg.Username, cfg.Password = c.Username, c.Password
	}
	if tls := l.Backend.TLS; tls != nil {
		cfg.CAFile = tls.CAFile
	}
	return cfg
}
