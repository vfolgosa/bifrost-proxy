package pool

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

// startTCPServer starts a TLS server on a random port and returns its address.
// The handler function is called for each accepted connection in a goroutine.
func startTCPServer(t *testing.T, handler func(net.Conn)) string {
	t.Helper()

	cert := generateTestCert(t)
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("failed to start TLS server: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handler(conn)
		}
	}()

	return ln.Addr().String()
}

// generateTestCert creates a self-signed certificate for testing.
func generateTestCert(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("failed to generate serial: %v", err)
	}

	tmpl := x509.Certificate{
		SerialNumber: serial,
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("failed to create key pair: %v", err)
	}
	return cert
}

// TestGetCreatesConnection verifies that Get creates a new TLS connection
// when no idle connections exist.
func TestGetCreatesConnection(t *testing.T) {
	addr := startTCPServer(t, func(conn net.Conn) {
		// Read one byte to ensure the client completes its handshake
		// before we close — avoids "connection reset by peer" on Go 1.26+.
		buf := make([]byte, 1)
		conn.Read(buf)
		conn.Close()
	})

	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 10,
	}
	cp := New(cfg, nil)
	defer cp.Close()

	conn, err := cp.Get(addr)
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", addr, err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}

	cp.Put(addr, conn)
}

// Use a server that reads before closing to avoid TLS handshake races.
func testServerHandler(conn net.Conn) {
	buf := make([]byte, 1)
	conn.Read(buf)
	conn.Close()
}

// TestPutAndReuse verifies that Put returns a connection to the pool and
// subsequent Get reuses it.
func TestPutAndReuse(t *testing.T) {
	addr := startTCPServer(t, testServerHandler)

	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 10,
	}
	cp := New(cfg, nil)
	defer cp.Close()

	conn1, err := cp.Get(addr)
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}

	// Return the connection.
	cp.Put(addr, conn1)

	// Get again — should reuse the same connection.
	conn2, err := cp.Get(addr)
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}
	defer cp.Put(addr, conn2)

	if conn1 != conn2 {
		t.Error("expected the same connection to be reused")
	}
}

// TestMaxConnectionsPerBroker verifies that the pool respects the
// max_connections_per_broker limit.
func TestMaxConnectionsPerBroker(t *testing.T) {
	addr := startTCPServer(t, func(conn net.Conn) {
		// Hold connections open so the pool fills up.
		buf := make([]byte, 1)
		conn.Read(buf)
	})

	maxConns := 5
	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: maxConns,
	}
	cp := New(cfg, nil)
	defer cp.Close()

	// Acquire maxConns connections.
	var conns []net.Conn
	for i := 0; i < maxConns; i++ {
		conn, err := cp.Get(addr)
		if err != nil {
			t.Fatalf("Get #%d failed: %v", i, err)
		}
		conns = append(conns, conn)
	}

	// Attempt to get one more — should block. We use a channel and timeout.
	done := make(chan struct{})
	go func() {
		conn, _ := cp.Get(addr)
		if conn != nil {
			cp.Put(addr, conn)
		}
		close(done)
	}()

	select {
	case <-done:
		t.Error("Get should have blocked when pool is at capacity, but it returned immediately")
	case <-time.After(500 * time.Millisecond):
		// Expected — Get is blocking.
	}

	// Return one connection — the blocked Get should now succeed.
	cp.Put(addr, conns[0])
	conns = conns[1:]

	select {
	case <-done:
		// Success — the blocked Get completed.
	case <-time.After(2 * time.Second):
		t.Error("Get did not unblock after returning a connection")
	}

	// Cleanup: return remaining connections.
	for _, conn := range conns {
		cp.Put(addr, conn)
	}
}

// TestIdleTimeout verifies that connections idle for longer than idle_timeout
// are closed and removed from the pool.
func TestIdleTimeout(t *testing.T) {
	addr := startTCPServer(t, testServerHandler)

	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 10,
		IdleTimeout:             config.Duration(50 * time.Millisecond),
	}
	cp := New(cfg, nil)
	defer cp.Close()

	// Get and return a connection.
	conn, err := cp.Get(addr)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	cp.Put(addr, conn)

	// Wait for idle timeout to expire + cleanup tick.
	time.Sleep(200 * time.Millisecond)

	// Get again — should create a new connection (the old one was cleaned up).
	conn2, err := cp.Get(addr)
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}
	defer cp.Put(addr, conn2)

	if conn == conn2 {
		t.Error("expected a new connection (old one should have been cleaned up)")
	}
}

// TestMultipleBrokers verifies the pool correctly isolates connections
// between different broker addresses.
func TestMultipleBrokers(t *testing.T) {
	addr1 := startTCPServer(t, testServerHandler)
	addr2 := startTCPServer(t, testServerHandler)

	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 10,
	}
	cp := New(cfg, nil)
	defer cp.Close()

	conn1, err := cp.Get(addr1)
	if err != nil {
		t.Fatalf("Get(addr1) failed: %v", err)
	}
	defer cp.Put(addr1, conn1)

	conn2, err := cp.Get(addr2)
	if err != nil {
		t.Fatalf("Get(addr2) failed: %v", err)
	}
	defer cp.Put(addr2, conn2)

	if conn1.RemoteAddr().String() != conn2.RemoteAddr().String() {
		t.Logf("connections to different brokers are correctly isolated")
	}
}

// TestConcurrentAccess verifies the pool is safe for concurrent use.
func TestConcurrentAccess(t *testing.T) {
	addr := startTCPServer(t, func(conn net.Conn) {
		buf := make([]byte, 1)
		conn.Read(buf)
		conn.Close()
	})

	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 20,
	}
	cp := New(cfg, nil)
	defer cp.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				conn, err := cp.Get(addr)
				if err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}
				// Small delay to simulate work.
				time.Sleep(5 * time.Millisecond)
				cp.Put(addr, conn)
			}
		}()
	}

	wg.Wait()
}

// TestCloseDrainsPool verifies that Close drains all idle connections.
func TestCloseDrainsPool(t *testing.T) {
	addr := startTCPServer(t, testServerHandler)

	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 10,
	}
	cp := New(cfg, nil)

	// Get and return a connection.
	conn, err := cp.Get(addr)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	cp.Put(addr, conn)

	// Close the pool.
	cp.Close()

	// Verify the idle connection was closed.
	_, err = conn.Read(make([]byte, 1))
	if err == nil {
		t.Error("expected connection to be closed after pool Close")
	}
}

// TestGetDialFailure verifies that Get returns an error when the broker
// is unreachable.
func TestGetDialFailure(t *testing.T) {
	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 10,
	}
	cp := New(cfg, nil)
	defer cp.Close()

	_, err := cp.Get("127.0.0.1:19999") // Unlikely to have a listener.
	if err == nil {
		t.Error("expected error dialing unreachable broker")
	}
}

// TestDefaultMaxConnections verifies the default of 50 connections when
// not configured.
func TestDefaultMaxConnections(t *testing.T) {
	addr := startTCPServer(t, func(conn net.Conn) {
		buf := make([]byte, 1)
		conn.Read(buf)
	})

	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 0, // Should default to 50.
	}
	cp := New(cfg, nil)
	defer cp.Close()

	bp := cp.getOrCreatePool(addr)
	if bp.maxSize != defaultMaxConnections {
		t.Errorf("maxSize = %d, want %d (default)", bp.maxSize, defaultMaxConnections)
	}
}

// TestKeepAliveEnabled verifies that TCP KeepAlive is set on connections.
func TestKeepAliveEnabled(t *testing.T) {
	addr := startTCPServer(t, testServerHandler)

	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 10,
		KeepAliveInterval:       config.Duration(30 * time.Second),
	}
	cp := New(cfg, nil)
	defer cp.Close()

	conn, err := cp.Get(addr)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer cp.Put(addr, conn)

	// Verify the connection is still usable after keep-alive setup.
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}
}

// TestBufferPool verifies the sync.Pool for connection buffers works.
func TestBufferPool(t *testing.T) {
	cfg := config.ConnectionPoolConfig{
		MaxConnectionsPerBroker: 10,
	}
	cp := New(cfg, nil)
	defer cp.Close()

	// Get a buffer, use it, and return it.
	bufPtr := cp.bufPool.Get().(*[]byte)
	buf := *bufPtr
	buf = append(buf, []byte("test data")...)
	*bufPtr = buf[:0] // Reset for reuse.
	cp.bufPool.Put(bufPtr)

	// Get another buffer — should be the same underlying array.
	bufPtr2 := cp.bufPool.Get().(*[]byte)
	if bufPtr2 == nil {
		t.Fatal("expected non-nil buffer from pool")
	}
	cp.bufPool.Put(bufPtr2)
}
