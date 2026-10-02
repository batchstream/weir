package protocol

import (
	"errors"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	MaxDiscoveryEndpoints = 128
	MaxDiscoveryCacheTTL  = 30 * time.Second
)

func ValidStoreName(name string) bool {
	parsed, segments, err := ParseResource("weir://" + name)
	return err == nil && parsed == name && len(segments) == 0
}

// CanonicalEndpoint accepts portable IP or DNS host:port addresses without I/O.
func CanonicalEndpoint(value string) (string, error) {
	if len(value) > 260 || strings.ContainsAny(value, "/@?#%\\ \t\r\n") {
		return "", errors.New("invalid discovery endpoint")
	}
	host, port, err := net.SplitHostPort(value)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || strings.Trim(port, "0123456789") != "" || number < 1 || number > 65535 {
		return "", errors.New("discovery endpoint requires host and explicit port 1-65535")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		ip = ip.Unmap()
		if ip.IsUnspecified() || ip.IsMulticast() || ip.Zone() != "" {
			return "", errors.New("discovery endpoint must be reachable unicast")
		}
		host = ip.String()
	} else {
		host = strings.TrimSuffix(strings.ToLower(host), ".")
		if len(host) < 1 || len(host) > 253 || strings.Trim(host, "0123456789.") == "" || strings.ContainsAny(value, "[]") {
			return "", errors.New("invalid discovery DNS name")
		}
		for _, label := range strings.Split(host, ".") {
			if len(label) < 1 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return "", errors.New("invalid discovery DNS label")
			}
			for _, c := range label {
				if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
					return "", errors.New("discovery DNS name requires ASCII labels")
				}
			}
		}
	}
	return net.JoinHostPort(host, strconv.Itoa(number)), nil
}

func CanonicalEndpoints(input []string) ([]string, error) {
	if len(input) < 1 || len(input) > MaxDiscoveryEndpoints {
		return nil, errors.New("discovery requires 1-128 endpoints")
	}
	out := make([]string, 0, len(input))
	for _, value := range input {
		address, err := CanonicalEndpoint(value)
		if err != nil {
			return nil, err
		}
		out = append(out, address)
	}
	slices.Sort(out)
	if len(slices.Compact(slices.Clone(out))) != len(out) {
		return nil, errors.New("duplicate discovery endpoint")
	}
	return out, nil
}
