package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
)

type Config struct {
	Application     string          `json:"application"`
	Peer            string          `json:"peer"`
	Identity        *Identity       `json:"identity"`
	Allow           []Grant         `json:"allow"`
	Services        []Service       `json:"services"`
	Routes          []Route         `json:"routes"`
	InitialForwards int             `json:"initial_forwards"`
	MemoryMiB       uint64          `json:"memory_mib"`
	Limits          TransportLimits `json:"limits"`
}
type Identity struct {
	Certificate string `json:"certificate"`
	PrivateKey  string `json:"private_key"`
	CA          string `json:"ca"`
}
type Grant struct {
	Identity   string   `json:"identity"`
	Store      string   `json:"store"`
	Operations []string `json:"operations"`
}
type Route struct {
	Store   string `json:"store"`
	Service string `json:"service"`
}
type Service struct {
	Name   string  `json:"name"`
	Local  *Local  `json:"local"`
	Remote *Remote `json:"remote"`
}
type Local struct {
	Mongo           *Mongo  `json:"mongo"`
	Search          *Search `json:"search"`
	BatchOperations int     `json:"batch_operations"`
}
type Mongo struct {
	URI        string `json:"uri"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
}
type Search struct {
	URL     string `json:"url"`
	Index   string `json:"index"`
	Profile string `json:"profile"`
}
type Remote struct {
	Endpoint   string `json:"endpoint"`
	ServerName string `json:"server_name"`
	Relays     int    `json:"relays"`
}
type TransportLimits struct {
	Connections int `json:"connections"`
	Sessions    int `json:"sessions"`
	UnaryMS     int `json:"unary_ms"`
	BulkMS      int `json:"bulk_ms"`
	ScanMS      int `json:"scan_ms"`
	NativeMS    int `json:"native_ms"`
	StallMS     int `json:"stall_ms"`
}

func DefaultConfig() Config {
	limits := TransportLimits{Connections: 16, Sessions: 16, UnaryMS: 30000, BulkMS: 900000, ScanMS: 300000, NativeMS: 300000, StallMS: 30000}
	cfg := Config{MemoryMiB: 512, InitialForwards: 4, Limits: limits}
	return cfg
}
func (l TransportLimits) serverLimits() server.Limits {
	limits := server.Limits{Connections: l.Connections, Sessions: l.Sessions, UnaryLifetime: time.Duration(l.UnaryMS) * time.Millisecond, BulkLifetime: time.Duration(l.BulkMS) * time.Millisecond, ScanLifetime: time.Duration(l.ScanMS) * time.Millisecond, NativeLifetime: time.Duration(l.NativeMS) * time.Millisecond, Stall: time.Duration(l.StallMS) * time.Millisecond}
	return limits
}
func Decode(input io.Reader) (Config, error) {
	cfg := DefaultConfig()
	raw, err := io.ReadAll(io.LimitReader(input, (128<<10)+1))
	if err != nil || len(raw) > 128<<10 {
		return cfg, errors.New("configuration exceeds bound")
	}
	// encoding/json alone accepts duplicate keys; reject these before decoding.
	tokens := json.NewDecoder(bytes.NewReader(raw))
	if err := uniqueJSON(tokens, 0); err != nil {
		return cfg, err
	}
	if _, err := tokens.Token(); err != io.EOF {
		return cfg, errors.New("trailing configuration data")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, errors.New("invalid configuration fields or types")
	}
	return cfg, cfg.Validate()
}
func uniqueJSON(d *json.Decoder, depth int) error {
	if depth > 12 {
		return errors.New("configuration nesting limit")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); ok {
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				text, ok := key.(string)
				// Match encoding/json's case-insensitive struct field lookup so
				// alternate casing cannot silently overwrite an earlier field.
				keyName := strings.ToLower(text)
				if !ok || seen[keyName] {
					return errors.New("duplicate configuration field")
				}
				seen[keyName] = true
				if err := uniqueJSON(d, depth+1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := uniqueJSON(d, depth+1); err != nil {
					return err
				}
			}
		default:
			return errors.New("invalid configuration structure")
		}
		_, err = d.Token()
	}
	return err
}

var dnsName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)
var mongoName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,62}$`)
var searchName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

func validIdentity(name string) bool { return len(name) <= 253 && dnsName.MatchString(name) }
func address(value string, loopback bool) bool {
	host, port, err := net.SplitHostPort(value)
	ip := net.ParseIP(host)
	number, portErr := strconv.Atoi(port)
	return err == nil && ip != nil && (!loopback || ip.IsLoopback()) && portErr == nil && number >= 0 && number <= 65535
}
func validName(name string) bool {
	parsed, segments, err := protocol.ParseResource("weir://" + name)
	return err == nil && parsed == name && len(segments) == 0
}
func permissions(operations []string) (server.Permission, error) {
	var result server.Permission
	if len(operations) == 0 || len(operations) > 4 {
		return 0, errors.New("invalid operation grant")
	}
	for _, operation := range operations {
		var value server.Permission
		switch operation {
		case "read":
			value = server.ReadPermission
		case "mutate":
			value = server.MutatePermission
		case "scan":
			value = server.ScanPermission
		case "native":
			value = server.NativePermission
		default:
			return 0, errors.New("unknown operation grant")
		}
		if result&value != 0 {
			return 0, errors.New("duplicate operation grant")
		}
		result |= value
	}
	return result, nil
}
func (cfg Config) Validate() error {
	if cfg.Application == "" && cfg.Peer == "" || cfg.Application != "" && !address(cfg.Application, true) || cfg.Peer != "" && (!address(cfg.Peer, false) || cfg.Peer == cfg.Application) {
		return errors.New("invalid listener configuration")
	}
	if cfg.InitialForwards < 0 || cfg.InitialForwards > 8 || cfg.MemoryMiB < 64 || cfg.MemoryMiB > 65536 {
		return errors.New("invalid process bounds")
	}
	for _, bound := range []int{cfg.Limits.UnaryMS, cfg.Limits.BulkMS, cfg.Limits.ScanMS, cfg.Limits.NativeMS, cfg.Limits.StallMS} {
		if bound < 1 || bound > 900000 {
			return errors.New("invalid transport duration")
		}
	}
	if err := cfg.Limits.serverLimits().Validate(); err != nil {
		return err
	}
	if len(cfg.Services) == 0 || len(cfg.Services) > 16 || len(cfg.Routes) == 0 || len(cfg.Routes) > 16 || len(cfg.Allow) > 128 {
		return errors.New("invalid static graph bounds")
	}
	services := make(map[string]Service)
	needsTLS := cfg.Peer != ""
	for _, service := range cfg.Services {
		if !validName(service.Name) || services[service.Name].Name != "" || (service.Local == nil) == (service.Remote == nil) {
			return errors.New("invalid or duplicate Service")
		}
		services[service.Name] = service
		if r := service.Remote; r != nil {
			needsTLS = true
			_, port, _ := net.SplitHostPort(r.Endpoint)
			if !address(r.Endpoint, false) || port == "0" || !validIdentity(r.ServerName) || r.Relays < 1 || r.Relays > 16 {
				return errors.New("invalid RemoteWeir")
			}
		}
		if l := service.Local; l != nil {
			if (l.Mongo == nil) == (l.Search == nil) {
				return errors.New("LocalStore requires exactly one adapter")
			}
			limits := store.DefaultLimits()
			if l.BatchOperations != 0 {
				limits.BatchOperations = l.BatchOperations
			}
			if err := limits.Validate(); err != nil {
				return err
			}
			if m := l.Mongo; m != nil {
				if !mongoName.MatchString(m.Database) || !mongoName.MatchString(m.Collection) || m.URI == "" {
					return errors.New("invalid MongoDB configuration")
				}
			}
			if search := l.Search; search != nil {
				if !searchName.MatchString(search.Index) || search.Profile != "elasticsearch-8.17.0" && search.Profile != "opensearch-2.19.0" {
					return errors.New("invalid Search configuration")
				}
				if len(search.URL) < 7 || search.URL[:7] != "http://" || !address(search.URL[7:], true) {
					return errors.New("invalid Search endpoint")
				}
			}
		}
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
	if needsTLS && (cfg.Identity == nil || cfg.Identity.Certificate == "" || cfg.Identity.PrivateKey == "" || cfg.Identity.CA == "") || !needsTLS && cfg.Identity != nil {
		return errors.New("invalid peer identity configuration")
	}
	if cfg.Peer != "" && len(cfg.Allow) == 0 || cfg.Peer == "" && len(cfg.Allow) != 0 {
		return errors.New("peer listener requires explicit grants")
	}
	grants := make(map[string]bool)
	identities := make(map[string]bool)
	for _, grant := range cfg.Allow {
		key := grant.Identity + "/" + grant.Store
		if !validIdentity(grant.Identity) || !stores[grant.Store] || grants[key] {
			return errors.New("invalid or duplicate peer grant")
		}
		if _, err := permissions(grant.Operations); err != nil {
			return err
		}
		grants[key] = true
		identities[grant.Identity] = true
	}
	if len(identities) > 32 {
		return errors.New("peer identity bound")
	}
	return nil
}
