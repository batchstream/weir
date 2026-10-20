package testmongo

import (
	"encoding/binary"
	"errors"
)

const replicaProxyMaxConnections = 128
const replicaProxyMaxCommands = 4096

// ReplicaProxyConnection retains bounded counts, never authentication or command
// payloads. A polling monitor has repeated hello replies and no business commands.
type ReplicaProxyConnection struct {
	ID                       int
	FrontendTLS, UpstreamTLS bool
	Commands                 map[string]int
	DiscoveryReplies         int
}

// ReplicaProxyRejection stores only a fixed reason and wire header numbers.
// Neither authentication nor BSON command/reply payloads are retained here.
type ReplicaProxyRejection struct {
	Response                                                     bool
	Opcode, Flags, Length, DeclaredLength, RequestID, ResponseTo uint32
	Reason                                                       string
}

type ReplicaProxySnapshot struct {
	Frontend, Upstream string
	Connections        []ReplicaProxyConnection
	Active, Rejections int
	LastRejection      ReplicaProxyRejection
}

func (p *Proxy) beginReplicaConnection() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.observations) >= replicaProxyMaxConnections {
		p.rejections++
		return -1
	}
	index := len(p.observations)
	observation := ReplicaProxyConnection{ID: index + 1, Commands: make(map[string]int)}
	p.observations = append(p.observations, observation)
	return index
}

func (p *Proxy) replicaTLS(index int, upstream bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if upstream {
		p.observations[index].UpstreamTLS = true
	} else {
		p.observations[index].FrontendTLS = true
	}
}

func (p *Proxy) reserveReplicaRequest(command string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.replicaRequests >= replicaProxyMaxCommands || len(command) == 0 || len(command) > 64 {
		p.rejections++
		return false
	}
	p.replicaRequests++
	return true
}

func (p *Proxy) recordReplicaCommand(index int, command string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Only a complete upstream Write contributes command coverage. Reservation
	// above bounds all connections' metadata, including failed/held requests.
	p.observations[index].Commands[command]++
}

func (p *Proxy) rejectReplicaMessage(message []byte, response bool, reason error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rejections++
	rejection := ReplicaProxyRejection{Response: response, Length: uint32(len(message)), Reason: reason.Error()}
	if len(message) >= 20 {
		rejection.DeclaredLength = binary.LittleEndian.Uint32(message[:4])
		rejection.RequestID = binary.LittleEndian.Uint32(message[4:8])
		rejection.ResponseTo = binary.LittleEndian.Uint32(message[8:12])
		rejection.Opcode = binary.LittleEndian.Uint32(message[12:16])
		rejection.Flags = binary.LittleEndian.Uint32(message[16:20])
	}
	p.lastRejection = rejection
}

func (p *Proxy) replicaSnapshot(frontend string) ReplicaProxySnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	connections := make([]ReplicaProxyConnection, len(p.observations))
	for i, observation := range p.observations {
		copy := observation
		copy.Commands = make(map[string]int, len(observation.Commands))
		for command, count := range observation.Commands {
			copy.Commands[command] = count
		}
		connections[i] = copy
	}
	snapshot := ReplicaProxySnapshot{Frontend: frontend, Upstream: p.backendAddress, Connections: connections, Active: p.upstream, Rejections: p.rejections, LastRejection: p.lastRejection}
	return snapshot
}

type replicaCoverageOptions struct {
	mapping    *replicaAddressMap
	discovered []string
	snapshots  [3]ReplicaProxySnapshot
	primary    int
	secure     bool
}

// Counts alone cannot exclude direct discovery bypass. First require the actual
// driver's entire discovered address set to equal the three owned frontends.
func checkReplicaCoverage(opts replicaCoverageOptions) error {
	failure := errors.New("owned replica frontend topology or monitor/data coverage missing")
	if opts.mapping == nil || len(opts.discovered) != 3 || opts.primary < 0 || opts.primary >= 3 {
		return failure
	}
	seen := make(map[string]bool, 3)
	for _, address := range opts.discovered {
		found := false
		for _, frontend := range opts.mapping.frontend {
			found = found || address == frontend
		}
		if !found || seen[address] {
			return failure
		}
		seen[address] = true
	}
	for i, snapshot := range opts.snapshots {
		if snapshot.Frontend != opts.mapping.frontend[i] || snapshot.Upstream != opts.mapping.actual[i] || snapshot.Rejections != 0 {
			return failure
		}
		monitor := false
		commands := make(map[string]int)
		for _, connection := range snapshot.Connections {
			// A stopped member may receive new frontend accepts whose upstream
			// dial/handshake fails. No command may use such an unverified leg.
			if len(connection.Commands) == 0 {
				continue
			}
			if connection.FrontendTLS != opts.secure || connection.UpstreamTLS != opts.secure {
				return failure
			}
			onlyHello := true
			for command, count := range connection.Commands {
				commands[command] += count
				onlyHello = onlyHello && replicaHello(command)
			}
			monitor = monitor || onlyHello && connection.DiscoveryReplies >= 2
		}
		if !monitor {
			return failure
		}
		if i == opts.primary {
			for _, command := range []string{"find", "bulkWrite", "findAndModify", "commitTransaction"} {
				if commands[command] == 0 {
					return failure
				}
			}
		}
	}
	return nil
}
