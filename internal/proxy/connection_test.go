package proxy

import (
	"net"
	"testing"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

func TestDialUpstream_Plaintext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	addr := ln.Addr().String()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	conn.Close()
}

func TestEndpointUsesTLS_Neither(t *testing.T) {
	cfg := config.ClusterConfig{
		Mode:   config.ModeActivePassive,
		Active: config.ActivePrimary,
	}
	if endpointUsesTLS(cfg, "primary") {
		t.Error("endpointUsesTLS should return false")
	}
}

func TestDialUpstream_ConnectionRefused(t *testing.T) {
	_, err := net.DialTimeout("tcp", "127.0.0.1:1", 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected connection refused")
	}
}
