package testmongo

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

// This intentionally preserves the fault relay's single reader. It proves why
// upstream high water cannot be asserted as the downstream raw ownership bound.
func TestProxyRetainsUpstreamAfterDownstreamClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	accepted := make(chan net.Conn, 2)
	workers.Go(func() {
		for range 2 {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
			workers.Go(func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				if request, err := readMessage(conn); err == nil {
					_, _ = conn.Write(request)
				}
			})
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		workers.Wait()
	})
	opts := proxyOptions{uri: "mongodb://" + listener.Addr().String()}
	p := startProxy(t, opts)
	gate, received, replied := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	p.Monitor = &event.CommandMonitor{Started: func(ctx context.Context, _ *event.CommandStartedEvent) {
		close(received)
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}, Succeeded: func(context.Context, *event.CommandSucceededEvent) { close(replied) }}
	first, err := net.DialTimeout("tcp", p.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	doc := bson.D{{Key: "ok", Value: 1}}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	message := make([]byte, 21)
	binary.LittleEndian.PutUint32(message, uint32(21+len(raw)))
	binary.LittleEndian.PutUint32(message[12:], 2013)
	message = append(message, raw...)
	_, err = first.Write(message)
	if err != nil {
		t.Fatal(err)
	}
	<-received
	_ = first.Close()
	t.Logf("%s downstream raw Close completed; observer request blocked before backend write", time.Now().UTC().Format(time.RFC3339Nano))
	second, err := net.DialTimeout("tcp", p.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for range 2 {
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("upstream not accepted")
		}
	}
	until := time.Now().Add(time.Second)
	for {
		current, _ := p.Sockets()
		if current == 2 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("observer did not record accepted upstream")
		}
		time.Sleep(time.Millisecond)
	}
	current, peak := p.Sockets()
	if current != 2 || peak != 2 {
		t.Fatal("retirement overlap missing", current, peak)
	}
	t.Logf("%s one downstream alive, observer upstream=%d peak=%d; no upstream gate/cap", time.Now().UTC().Format(time.RFC3339Nano), current, peak)
	release.Do(func() { close(gate) })
	select {
	case <-replied:
	case <-time.After(time.Second):
		t.Fatal("remote tail not completed")
	}
	t.Logf("%s backend reply observed after downstream Close", time.Now().UTC().Format(time.RFC3339Nano))
	_ = second.Close()
	until = time.Now().Add(time.Second)
	for {
		current, _ = p.Sockets()
		if current == 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("observer tail retained", current)
		}
		time.Sleep(time.Millisecond)
	}
}
