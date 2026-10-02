package server

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/store"
)

func TestConnectionBoundAndSingleClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	limits := DefaultLimits()
	limits.Connections = 2
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{admission: admission}
	bounded := &limitedListener{Listener: listener, slots: admission.connections, server: s}
	accepted := make(chan net.Conn, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := bounded.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()
	var peers, servers []net.Conn
	defer func() {
		for _, conn := range peers {
			_ = conn.Close()
		}
		for _, conn := range servers {
			_ = conn.Close()
		}
		_ = listener.Close()
		<-done
	}()
	for i := 0; i < 2; i++ {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, conn)
		select {
		case server := <-accepted:
			servers = append(servers, server)
		case <-time.After(time.Second):
			t.Fatal("accept stalled")
		}
	}
	extra, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err = extra.Read(buf); err != io.EOF {
		t.Fatal("excess connection not rejected", err)
	}
	_ = servers[0].Close()
	_ = servers[0].Close()
	if len(bounded.slots) != 1 {
		t.Fatal("connection credit closed more than once")
	}
}

// The fixture is the assembly owner. A listener never closes borrowed runtimes.
func newLocalServer(t *testing.T, stores map[string]*store.Runtime, limits Limits) (*Server, error) {
	t.Helper()
	admission, err := NewAdmission(limits)
	if err != nil {
		return nil, err
	}
	config := Config{Stores: stores, Limits: limits, Admission: admission}
	srv, err := New(config)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, runtime := range stores {
			if err := runtime.Close(ctx); err != nil {
				t.Error(err)
			}
		}
	})
	return srv, err
}
