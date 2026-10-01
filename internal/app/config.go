package app

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
)

// Config contains the separately loaded process and routing settings.
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
	Forwarding  ForwardingConfig  `json:"forwarding" yaml:"forwarding"`
}

type ListenerConfig struct {
	Application string `json:"application" yaml:"application"`
	Peer        string `json:"peer" yaml:"peer"`
}

type DiagnosticsConfig struct {
	Address       string `json:"address" yaml:"address"`
	AllowIntranet bool   `json:"allow_intranet" yaml:"allow_intranet"`
}

type ForwardingConfig struct {
	HopLimit int `json:"hop_limit" yaml:"hop_limit"`
}

// RoutingConfig defines the complete, static service and Store graph.
type RoutingConfig struct {
	Services []Service `json:"services" yaml:"services"`
	Routes   []Route   `json:"routes" yaml:"routes"`
}

type Route struct {
	Store   string `json:"store" yaml:"store"`
	Service string `json:"service" yaml:"service"`
}

type Service struct {
	Name   string  `json:"name" yaml:"name"`
	Local  *Local  `json:"local" yaml:"local"`
	Remote *Remote `json:"remote" yaml:"remote"`
}

type Local struct {
	MongoDB            *Mongo  `json:"mongodb" yaml:"mongodb"`
	Search             *Search `json:"search" yaml:"search"`
	MaxConcurrency     int     `json:"max_concurrency" yaml:"max_concurrency"`
	MaxBatchOperations int     `json:"max_batch_operations" yaml:"max_batch_operations"`
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

type Remote struct {
	Endpoints      []string `json:"endpoints" yaml:"endpoints"`
	MaxConcurrency int      `json:"max_concurrency" yaml:"max_concurrency"`
}

type TransportConfig struct {
	MaxConnections int               `json:"max_connections" yaml:"max_connections"`
	MaxSessions    int               `json:"max_sessions" yaml:"max_sessions"`
	Timeouts       TransportTimeouts `json:"timeouts" yaml:"timeouts"`
}

type TransportTimeouts struct {
	Unary  Duration `json:"unary" yaml:"unary"`
	Bulk   Duration `json:"bulk" yaml:"bulk"`
	Scan   Duration `json:"scan" yaml:"scan"`
	Native Duration `json:"native" yaml:"native"`
	Stall  Duration `json:"stall" yaml:"stall"`
}

func DefaultConfig() Config {
	defaults := server.DefaultLimits()
	timeouts := TransportTimeouts{
		Unary:  Duration(defaults.UnaryLifetime),
		Bulk:   Duration(defaults.BulkLifetime),
		Scan:   Duration(defaults.ScanLifetime),
		Native: Duration(defaults.NativeLifetime),
		Stall:  Duration(defaults.Stall),
	}
	transport := TransportConfig{
		MaxConnections: defaults.Connections,
		MaxSessions:    defaults.Sessions,
		Timeouts:       timeouts,
	}
	forwarding := ForwardingConfig{HopLimit: 4}
	basic := BasicConfig{
		Memory:     512 << 20,
		Transport:  transport,
		Forwarding: forwarding,
	}

	cfg := Config{Basic: basic}
	return cfg
}

func (cfg TransportConfig) serverLimits() server.Limits {
	timeouts := cfg.Timeouts
	limits := server.Limits{
		Connections:    cfg.MaxConnections,
		Sessions:       cfg.MaxSessions,
		UnaryLifetime:  time.Duration(timeouts.Unary),
		BulkLifetime:   time.Duration(timeouts.Bulk),
		ScanLifetime:   time.Duration(timeouts.Scan),
		NativeLifetime: time.Duration(timeouts.Native),
		Stall:          time.Duration(timeouts.Stall),
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
	parsed, segments, err := protocol.ParseResource("weir://" + name)
	return err == nil && parsed == name && len(segments) == 0
}

func (cfg Config) Validate() error {
	if err := cfg.Basic.Validate(); err != nil {
		return err
	}
	return cfg.Routing.Validate()
}

func (cfg BasicConfig) Validate() error {
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

	if cfg.Forwarding.HopLimit < 0 || cfg.Forwarding.HopLimit > 8 ||
		cfg.Memory < 64<<20 || cfg.Memory > 64<<30 {
		return errors.New("invalid process bounds")
	}
	if err := cfg.Transport.serverLimits().Validate(); err != nil {
		return err
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
	for _, service := range cfg.Services {
		if r := service.Remote; r != nil {
			_, endpointErr := server.CanonicalEndpoints(r.Endpoints)
			if endpointErr != nil || r.MaxConcurrency < 1 || r.MaxConcurrency > 16 {
				return errors.New("invalid RemoteWeir")
			}
		}

		if l := service.Local; l != nil {
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
	if len(cfg.Services) > 16 ||
		len(cfg.Routes) > 16 ||
		(len(cfg.Services) == 0) != (len(cfg.Routes) == 0) {
		return errors.New("invalid static graph bounds")
	}
	services := make(map[string]Service)
	for _, service := range cfg.Services {
		if !validName(service.Name) || services[service.Name].Name != "" || (service.Local == nil) == (service.Remote == nil) {
			return errors.New("invalid or duplicate Service")
		}
		if l := service.Local; l != nil && (l.MongoDB == nil) == (l.Search == nil) {
			return errors.New("LocalStore requires exactly one adapter")
		}
		services[service.Name] = service
	}

	stores := make(map[string]bool)
	uses := make(map[string]int)
	for _, route := range cfg.Routes {
		if !validName(route.Store) || stores[route.Store] || services[route.Service].Name == "" {
			return errors.New("duplicate Store or invalid Service reference")
		}
		stores[route.Store] = true
		uses[route.Service]++
	}

	for name, service := range services {
		if uses[name] == 0 || service.Local != nil && uses[name] != 1 {
			return errors.New("unused or aliased LocalStore")
		}
	}

	return nil
}

func (l *Local) runtimeLimits() store.Limits {
	limits := store.DefaultLimits()
	if l.MaxConcurrency != 0 {
		limits.Concurrency = l.MaxConcurrency
	}
	if l.MaxBatchOperations != 0 {
		limits.BatchOperations = l.MaxBatchOperations
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
		Store:      name,
		URL:        l.Search.URL,
		Pool:       l.runtimeLimits().Concurrency,
		Connection: connection,
	}
	return cfg
}

func (l *Local) mongoConfig(name string) mongodb.Config {
	m := l.MongoDB
	cfg := mongodb.Config{
		URI:      m.URI,
		Username: m.Username,
		Password: m.Password,
		Store:    name,
		Pool:     uint64(l.runtimeLimits().Concurrency),
	}
	return cfg
}
