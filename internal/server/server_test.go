package server

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestConnectionOpeningUsesConfiguredHandshakeTimeout(t *testing.T) {
	limits := DefaultLimits()
	limits.Handshake = 500 * time.Millisecond
	limits.Stall = 20 * time.Millisecond
	opts := peerServerOptions{limits: limits}
	_, address := startPeerServer(t, opts)
	client, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	started := time.Now()
	if err := client.SetReadDeadline(started.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// The server's own SETTINGS may arrive first; a silent client is then
	// disconnected by the handshake timeout, not by the stream stall timeout.
	_, err = io.Copy(io.Discard, client)
	elapsed := time.Since(started)
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		t.Fatal("configured handshake timeout did not close the connection", err)
	}
	if elapsed < 400*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Fatal("connection closed outside the handshake timeout", elapsed)
	}
}
