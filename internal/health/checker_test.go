package health

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

// ── Test helpers ────────────────────────────────────────────────────────

// startPlainTCPServer starts a plain TCP server on a random port.
func startPlainTCPServer(t *testing.T, handler func(net.Conn)) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start TCP server: %v", err)
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

// startTLSServer starts a TLS server on a random port that responds to
// MetadataRequests. Returns the address.
func startTLSServer(t *testing.T, handler func(net.Conn)) string {
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

// metadataHandler reads a MetadataRequest and responds with a minimal
// valid MetadataResponse.
func metadataHandler(conn net.Conn) {
	defer conn.Close()

	// Read size prefix.
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return
	}

	reqSize := binary.BigEndian.Uint32(sizeBuf[:])
	reqBuf := make([]byte, reqSize)
	if _, err := io.ReadFull(conn, reqBuf); err != nil {
		return
	}

	// Respond with a valid MetadataResponse:
	// Size(4) + CorrelationID(4) + Brokers array (empty: count=0 in 4 bytes).
	correlationID := int32(0)
	if len(reqBuf) >= 4 {
		// correlationID starts at offset 4 in the request header (after APIKey+APIVersion)
		// Wait — the body begins after the Size prefix, so:
		// reqBuf[0:2] = APIKey, reqBuf[2:4] = APIVersion, reqBuf[4:8] = CorrelationID
		if len(reqBuf) >= 8 {
			correlationID = int32(binary.BigEndian.Uint32(reqBuf[4:8]))
		}
	}

	respBodyLen := 4 + 4 // CorrelationID + broker_count=0
	resp := make([]byte, 4+respBodyLen)
	binary.BigEndian.PutUint32(resp[0:4], uint32(respBodyLen))
	binary.BigEndian.PutUint32(resp[4:8], uint32(correlationID))
	binary.BigEndian.PutUint32(resp[8:12], 0) // broker count = 0

	conn.Write(resp)
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

// ── Tests ───────────────────────────────────────────────────────────────

func TestNewChecker_SkipsDisabled(t *testing.T) {
	clusters := map[string]config.ClusterConfig{
		"bu-disabled": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: "pkc-1.aws.confluent.cloud:9092",
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: "pkc-2.aws.confluent.cloud:9092",
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:  false,
				Interval: config.Duration(10 * time.Second),
			},
		},
		"bu-enabled": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: "pkc-3.aws.confluent.cloud:9092",
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: "pkc-4.aws.confluent.cloud:9092",
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:  true,
				Interval: config.Duration(10 * time.Second),
			},
		},
	}

	c := New(clusters)
	defer c.Stop()

	health := c.Health()
	if len(health) != 1 {
		t.Fatalf("Health() returned %d clusters, want 1 (disabled should be skipped)", len(health))
	}

	if _, ok := health["bu-enabled"]; !ok {
		t.Fatal("bu-enabled should be present in Health()")
	}
	if _, ok := health["bu-disabled"]; ok {
		t.Fatal("bu-disabled should NOT be present in Health()")
	}
}

func TestChecker_StartStop(t *testing.T) {
	addr := startPlainTCPServer(t, metadataHandler)

	clusters := map[string]config.ClusterConfig{
		"test-bu": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:  true,
				Interval: config.Duration(50 * time.Millisecond),
			},
		},
	}

	c := New(clusters)
	c.Start()

	// Wait for at least one check cycle.
	time.Sleep(100 * time.Millisecond)

	c.Stop()

	// After Stop, Health() should still return data (last known state).
	health := c.Health()
	ch, ok := health["test-bu"]
	if !ok {
		t.Fatal("test-bu should be present in Health()")
	}

	// Both should have LastCheckAt set.
	if ch.Primary.LastCheckAt.IsZero() {
		t.Error("Primary.LastCheckAt should not be zero")
	}
	if ch.Secondary.LastCheckAt.IsZero() {
		t.Error("Secondary.LastCheckAt should not be zero")
	}
}

func TestChecker_HealthCheckSuccess(t *testing.T) {
	addr := startPlainTCPServer(t, metadataHandler)

	clusters := map[string]config.ClusterConfig{
		"test-bu": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:           true,
				Interval:          config.Duration(50 * time.Millisecond),
				FailureThreshold:  3,
				RecoveryThreshold: 2,
			},
		},
	}

	c := New(clusters)
	c.Start()

	// Wait for a couple of check cycles.
	time.Sleep(200 * time.Millisecond)

	c.Stop()

	health := c.Health()
	ch := health["test-bu"]

	// Both endpoints should be healthy since server responds.
	if !ch.Primary.Healthy {
		t.Errorf("Primary should be healthy, got: Healthy=%v, Failures=%d, Successes=%d",
			ch.Primary.Healthy, ch.Primary.ConsecutiveFailures, ch.Primary.ConsecutiveSuccesses)
	}
	if !ch.Secondary.Healthy {
		t.Errorf("Secondary should be healthy, got: Healthy=%v, Failures=%d, Successes=%d",
			ch.Secondary.Healthy, ch.Secondary.ConsecutiveFailures, ch.Secondary.ConsecutiveSuccesses)
	}

	// Check status classification.
	if ch.Primary.LastStatus != StatusHealthy && ch.Primary.LastStatus != StatusDegraded {
		t.Errorf("Primary.LastStatus = %v, want healthy or degraded", ch.Primary.LastStatus)
	}
	if ch.Primary.LastError != "" {
		t.Errorf("Primary.LastError should be empty on success, got %q", ch.Primary.LastError)
	}

	// Bootstrap should be set.
	if ch.Primary.Bootstrap != addr {
		t.Errorf("Primary.Bootstrap = %q, want %q", ch.Primary.Bootstrap, addr)
	}
}

func TestChecker_HealthCheckFailure(t *testing.T) {
	// Use a port that's very unlikely to have a Kafka broker.
	unreachableAddr := "127.0.0.1:19999"

	clusters := map[string]config.ClusterConfig{
		"test-bu": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: unreachableAddr,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: unreachableAddr,
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:           true,
				Interval:          config.Duration(50 * time.Millisecond),
				FailureThreshold:  2,
				RecoveryThreshold: 2,
			},
		},
	}

	c := New(clusters)
	c.Start()

	// Wait for enough check cycles to trigger DOWN (failureThreshold=2).
	// Adaptive fast interval is 2s after first failure, so allow extra time.
	time.Sleep(3 * time.Second)

	c.Stop()

	health := c.Health()
	ch := health["test-bu"]

	// After failureThreshold failures, endpoint should be DOWN.
	if ch.Primary.Healthy {
		t.Errorf("Primary should be DOWN after %d consecutive failures, got: Healthy=%v, Failures=%d",
			2, ch.Primary.Healthy, ch.Primary.ConsecutiveFailures)
	}
	if ch.Primary.LastStatus != StatusUnreachable {
		t.Errorf("Primary.LastStatus = %v, want %v", ch.Primary.LastStatus, StatusUnreachable)
	}
	if ch.Primary.LastError == "" {
		t.Error("Primary.LastError should not be empty on failure")
	}
}

func TestChecker_RecoveryAfterFailure(t *testing.T) {
	// Start a server that fails initially, then recovers.
	var mu sync.Mutex
	shouldFail := true

	addr := startPlainTCPServer(t, func(conn net.Conn) {
		mu.Lock()
		fail := shouldFail
		mu.Unlock()

		if fail {
			conn.Close()
			return
		}
		metadataHandler(conn)
	})

	clusters := map[string]config.ClusterConfig{
		"test-bu": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: "", // no secondary
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:           true,
				Interval:          config.Duration(50 * time.Millisecond),
				FailureThreshold:  2,
				RecoveryThreshold: 2,
			},
		},
	}

	c := New(clusters)
	c.Start()

	// Wait for failures to accumulate.
	time.Sleep(200 * time.Millisecond)

	// Verify it went DOWN.
	health := c.Health()
	if health["test-bu"].Primary.Healthy {
		t.Fatal("Primary should be DOWN after failures")
	}

	// Now let the server succeed.
	mu.Lock()
	shouldFail = false
	mu.Unlock()

	// Wait for recovery.
	time.Sleep(200 * time.Millisecond)

	c.Stop()

	health = c.Health()
	if !health["test-bu"].Primary.Healthy {
		t.Errorf("Primary should have recovered, got: Healthy=%v, Successes=%d",
			health["test-bu"].Primary.Healthy, health["test-bu"].Primary.ConsecutiveSuccesses)
	}
}

func TestChecker_MultipleClusters(t *testing.T) {
	addr1 := startPlainTCPServer(t, metadataHandler)
	addr2 := startPlainTCPServer(t, metadataHandler)

	clusters := map[string]config.ClusterConfig{
		"bu-sales": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: addr1,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: addr2,
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:  true,
				Interval: config.Duration(50 * time.Millisecond),
			},
		},
		"bu-logistics": {
			Mode: config.ModeLoadBalance,
			Primary: config.ClusterEndpoint{
				Bootstrap: addr2,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: addr1,
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:  true,
				Interval: config.Duration(50 * time.Millisecond),
			},
		},
	}

	c := New(clusters)
	c.Start()

	time.Sleep(200 * time.Millisecond)
	c.Stop()

	health := c.Health()
	if len(health) != 2 {
		t.Fatalf("Health() returned %d clusters, want 2", len(health))
	}

	for _, name := range []string{"bu-sales", "bu-logistics"} {
		ch, ok := health[name]
		if !ok {
			t.Errorf("%s should be present in Health()", name)
			continue
		}
		if !ch.Primary.Healthy {
			t.Errorf("%s primary should be healthy: %+v", name, ch.Primary)
		}
		if !ch.Secondary.Healthy {
			t.Errorf("%s secondary should be healthy: %+v", name, ch.Secondary)
		}
	}
}

func TestChecker_EmptyEndpoint_Skipped(t *testing.T) {
	addr := startPlainTCPServer(t, metadataHandler)

	clusters := map[string]config.ClusterConfig{
		"test-bu": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: "", // empty — should be skipped
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:  true,
				Interval: config.Duration(50 * time.Millisecond),
			},
		},
	}

	c := New(clusters)
	c.Start()

	time.Sleep(150 * time.Millisecond)
	c.Stop()

	health := c.Health()
	ch := health["test-bu"]

	if !ch.Primary.Healthy {
		t.Error("Primary should be healthy")
	}

	// Secondary should have zero LastCheckAt (was skipped).
	if !ch.Secondary.LastCheckAt.IsZero() {
		t.Error("Secondary.LastCheckAt should be zero (empty addr was skipped)")
	}
}

func TestChecker_DefaultThresholds(t *testing.T) {
	addr := startPlainTCPServer(t, metadataHandler)

	clusters := map[string]config.ClusterConfig{
		"test-bu": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:  true,
				Interval: config.Duration(50 * time.Millisecond),
				// FailureThreshold and RecoveryThreshold are zero → defaults.
			},
		},
	}

	c := New(clusters)
	c.Start()

	time.Sleep(200 * time.Millisecond)
	c.Stop()

	health := c.Health()
	ch := health["test-bu"]

	if !ch.Primary.Healthy {
		t.Error("Primary should be healthy with default thresholds")
	}
	if !ch.Secondary.Healthy {
		t.Error("Secondary should be healthy with default thresholds")
	}
}

func TestChecker_HealthSnapshotThreadSafe(t *testing.T) {
	addr := startPlainTCPServer(t, metadataHandler)

	clusters := map[string]config.ClusterConfig{
		"test-bu": {
			Mode: config.ModeActivePassive,
			Primary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			Secondary: config.ClusterEndpoint{
				Bootstrap: addr,
			},
			HealthCheck: config.HealthCheckConfig{
				Enabled:  true,
				Interval: config.Duration(10 * time.Millisecond),
			},
		},
	}

	c := New(clusters)
	c.Start()

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Concurrent Health() calls while checker is running.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = c.Health()
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()

	c.Stop()
}
