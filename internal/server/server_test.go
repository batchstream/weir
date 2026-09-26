package server

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestConnectionBoundAndSingleClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	s := &Server{}
	bounded := &limitedListener{Listener: listener, slots: make(chan struct{}, 2), server: s}
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
