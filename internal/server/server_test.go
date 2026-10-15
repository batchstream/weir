package server

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/store"
)

func TestConnectionOpeningUsesConfiguredHandshakeTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	limits := DefaultLimits()
	limits.Handshake = 500 * time.Millisecond
	limits.Stall = 20 * time.Millisecond
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Admission: admission, Limits: limits}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.grpc.Stop()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	wrapper := &limitedListener{Listener: listener, server: server}
	connection, err := wrapper.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := client.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	_, err = client.Read(buffer)
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatal("stream stall setting ended the connection handshake", err)
	}
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Read(buffer); !errors.Is(err, io.EOF) {
		t.Fatal("configured handshake timeout did not close the connection", err)
	}
}

func TestConnectionsHaveNoAdmissionCapAndCloseOnce(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	limits := DefaultLimits()
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Admission: admission, Limits: limits}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.grpc.Stop()
	wrapper := &limitedListener{Listener: listener, server: server}
	var clients, accepted []net.Conn
	defer func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
		for _, conn := range accepted {
			_ = conn.Close()
		}
	}()
	for range 32 {
		client, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, client)
		conn, err := wrapper.Accept()
		if err != nil {
			t.Fatal(err)
		}
		accepted = append(accepted, conn)
	}
	if admission.activeConnections.Load() != 32 {
		t.Fatal("connections were capped")
	}
	for _, conn := range accepted {
		_ = conn.Close()
		_ = conn.Close()
	}
	if admission.activeConnections.Load() != 0 {
		t.Fatal("close was not idempotent")
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
