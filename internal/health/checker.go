// Package health implements per-cluster autonomous health checks for the Kafka proxy.
//
// Each configured cluster gets a background goroutine that periodically opens a
// dedicated short-lived TLS connection to each endpoint (primary/secondary),
// sends a lightweight MetadataRequest (API Key 3, empty topics), measures
// round-trip latency, and reports healthy/degraded/unreachable.
//
// These connections are DIFFERENT from the data-plane pool — they use their own
// short-lived TLS dial, so health checks never compete with client traffic.
package health

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

// ClusterHealth binds a cluster name to its two endpoint health state snapshots.
type ClusterHealth struct {
	Name       string
	Primary   Snapshot
	Secondary Snapshot
}

// Checker drives autonomous health checks for all configured clusters.
//
// One goroutine per cluster sends MetadataRequests at the configured interval
// to both the primary and secondary endpoints using dedicated short-lived
// TLS connections.
type Checker struct {
	mu       sync.RWMutex
	clusters map[string]*clusterRunner
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// New creates a Checker from cluster configurations.  If health_check.enabled
// is false (or absent) for a cluster, that cluster is skipped.
func New(clusters map[string]config.ClusterConfig) *Checker {
	c := &Checker{
		clusters: make(map[string]*clusterRunner),
		stopCh:   make(chan struct{}),
	}

	for name, cluster := range clusters {
		if !cluster.HealthCheck.Enabled {
			logger.L().Info("health check disabled for cluster",
				"cluster", name)
			continue
		}

		interval := cluster.HealthCheck.Interval.Duration()
		if interval <= 0 {
			interval = 10 * time.Second
		}

		failureThreshold := cluster.HealthCheck.FailureThreshold
		if failureThreshold <= 0 {
			failureThreshold = 3
		}

		recoveryThreshold := cluster.HealthCheck.RecoveryThreshold
		if recoveryThreshold <= 0 {
			recoveryThreshold = 2
		}

		c.clusters[name] = &clusterRunner{
			name:              name,
			primaryAddr:      cluster.Primary.Bootstrap,
			secondaryAddr:    cluster.Secondary.Bootstrap,
			interval:          interval,
			failureThreshold:  failureThreshold,
			recoveryThreshold: recoveryThreshold,
			saslUsername:      cluster.HealthCheck.SaslUsername,
			saslPassword:      cluster.HealthCheck.SaslPassword,
			primary:          NewHealthState(cluster.Primary.Bootstrap),
			secondary:        NewHealthState(cluster.Secondary.Bootstrap),
		}
	}

	return c
}

// Start launches per-cluster health-check goroutines. It is safe to call
// after New; calling it again after Stop is a no-op.
func (c *Checker) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range c.clusters {
		if r.running {
			continue
		}
		r.running = true
		c.wg.Add(1)
		go c.runCluster(r)
	}
}

// Stop signals all health-check goroutines to exit and waits for them.
func (c *Checker) Stop() {
	select {
	case <-c.stopCh:
		return
	default:
	}
	close(c.stopCh)
	c.wg.Wait()
}

// Health returns a snapshot of current health state for every cluster.
func (c *Checker) Health() map[string]ClusterHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]ClusterHealth, len(c.clusters))
	for name, r := range c.clusters {
		result[name] = ClusterHealth{
			Name:       name,
			Primary:   r.primary.Snapshot(),
			Secondary: r.secondary.Snapshot(),
		}
	}
	return result
}

// clusterRunner holds the configuration and runtime state for one cluster.
type clusterRunner struct {
	name              string
	primaryAddr      string
	secondaryAddr    string
	interval          time.Duration
	failureThreshold  int
	recoveryThreshold int
	saslUsername      string
	saslPassword      string

	primary   *HealthState
	secondary *HealthState

	running bool
}

// runCluster is the per-cluster goroutine. It checks both endpoints on a
// ticker, with one immediate check on start.
func (c *Checker) runCluster(r *clusterRunner) {
	defer c.wg.Done()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	c.checkEndpoint(r, r.primary, r.primaryAddr, "primary")
	c.checkEndpoint(r, r.secondary, r.secondaryAddr, "secondary")

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.checkEndpoint(r, r.primary, r.primaryAddr, "primary")
			c.checkEndpoint(r, r.secondary, r.secondaryAddr, "secondary")
		}
	}
}

// checkEndpoint opens a dedicated short-lived connection to addr, performs SASL
// authentication if credentials are configured, sends a MetadataRequest, reads
// the response, and records the result.
func (c *Checker) checkEndpoint(r *clusterRunner, state *HealthState, addr, label string) {
	if addr == "" {
		return
	}

	start := time.Now()

	conn, err := dialHealth(addr, false) // false = plain TCP for local dev
	if err != nil {
		latency := time.Since(start)
		state.RecordFailure(latency, StatusUnreachable, err.Error(), r.failureThreshold)
		logger.L().Warn("health check failed",
			"cluster", r.name, "endpoint", label, "addr", addr,
			"latency", latency, "error", err)
		return
	}
	defer conn.Close()

	// SASL authentication before Metadata request (required for SASL-enabled brokers).
	if err := saslAuth(conn, r.saslUsername, r.saslPassword); err != nil {
		latency := time.Since(start)
		state.RecordFailure(latency, StatusUnreachable,
			fmt.Sprintf("sasl auth: %v", err), r.failureThreshold)
		logger.L().Warn("health check sasl failed",
			"cluster", r.name, "endpoint", label, "addr", addr,
			"latency", latency, "error", err)
		return
	}

	if err := writeMetadataRequest(conn); err != nil {
		latency := time.Since(start)
		state.RecordFailure(latency, StatusUnreachable,
			fmt.Sprintf("write metadata request: %v", err), r.failureThreshold)
		logger.L().Warn("health check write failed",
			"cluster", r.name, "endpoint", label, "addr", addr,
			"latency", latency, "error", err)
		return
	}

	if err := readMetadataResponse(conn); err != nil {
		latency := time.Since(start)
		state.RecordFailure(latency, StatusUnreachable,
			fmt.Sprintf("read metadata response: %v", err), r.failureThreshold)
		logger.L().Warn("health check read failed",
			"cluster", r.name, "endpoint", label, "addr", addr,
			"latency", latency, "error", err)
		return
	}

	latency := time.Since(start)

	var status Status
	switch {
	case latency > 5*time.Second:
		status = StatusDegraded
	default:
		status = StatusHealthy
	}

	state.RecordSuccess(latency, status, r.failureThreshold, r.recoveryThreshold)

	if status == StatusDegraded {
		logger.L().Warn("health check degraded",
			"cluster", r.name, "endpoint", label, "addr", addr,
			"latency", latency)
	}
}

const (
	healthDialTimeout  = 10 * time.Second
	healthReadTimeout  = 10 * time.Second
	healthWriteTimeout = 5 * time.Second
)

// dialHealth creates a short-lived connection to the given address.
// If useTLS is false, a plain TCP connection is returned.
// This is intentionally separate from the connection pool — health checks
// must not compete with data-plane connections.
func dialHealth(addr string, useTLS bool) (net.Conn, error) {
	d := net.Dialer{
		Timeout: healthDialTimeout,
	}

	rawConn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	if !useTLS {
		return rawConn, nil
	}

	tlsConn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	})

	if err := tlsConn.Handshake(); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("TLS handshake: %w", err)
	}

	return tlsConn, nil
}

// KafkaMetadataAPIKey is API key 3 (Metadata).
const KafkaMetadataAPIKey int16 = protocol.APIKeyMetadata

// writeMetadataRequest constructs and sends a lightweight MetadataRequest
// with an empty topics array (we don't need topology data for a health ping).
func writeMetadataRequest(conn net.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(healthWriteTimeout))

	clientID := "kafkaproxy-health"

	headerLen := 2 + 2 + 4 + 2 + len(clientID)
	header := make([]byte, headerLen)
	binary.BigEndian.PutUint16(header[0:2], uint16(KafkaMetadataAPIKey))
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint32(header[4:8], 0)
	binary.BigEndian.PutUint16(header[8:10], uint16(len(clientID)))
	copy(header[10:], clientID)

	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body[0:4], 0)

	frame := protocol.WriteFrame(header, body)
	defer protocol.ReleaseFrame(frame)

	_, err := conn.Write(frame)
	return err
}

// readMetadataResponse reads and validates a MetadataResponse from the broker.
// We only care that the TCP/TLS layer is healthy; response body contents are
// irrelevant for health checks.
func readMetadataResponse(conn net.Conn) error {
	conn.SetReadDeadline(time.Now().Add(healthReadTimeout))

	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return fmt.Errorf("read size prefix: %w", err)
	}

	respSize := binary.BigEndian.Uint32(sizeBuf[:])

	respBuf := make([]byte, respSize)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if len(respBuf) < 4 {
		return fmt.Errorf("response too short: %d bytes", len(respBuf))
	}

	correlationID := int32(binary.BigEndian.Uint32(respBuf[0:4]))
	_ = correlationID

	return nil
}

// saslAuth performs SASL/PLAIN authentication on the connection.
// Credentials must be provided by the caller from configuration.
const (
	saslHandshakeAPIKey    int16 = 17
	saslAuthenticateAPIKey int16 = 36
)

func saslAuth(conn net.Conn, username, password string) error {
	if username == "" {
		return nil // no SASL configured, skip
	}
	// Frame: [size:4] [api_key:2] [api_version:2] [corr_id:4] [client_id_len:2] [client_id] [mechanism_len:2] [mechanism]
	mechanism := "PLAIN"
	clientID := "kafkaproxy-health"
	bodyLen := 2 + len(mechanism)
	totalSize := 2 + 2 + 4 + 2 + len(clientID) + bodyLen

	buf := make([]byte, 4+totalSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalSize))
	binary.BigEndian.PutUint16(buf[4:6], uint16(saslHandshakeAPIKey))
	binary.BigEndian.PutUint16(buf[6:8], 1) // api_version
	binary.BigEndian.PutUint32(buf[8:12], 1) // correlation_id
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:], clientID)
	binary.BigEndian.PutUint16(buf[14+len(clientID):], uint16(len(mechanism)))
	copy(buf[16+len(clientID):], mechanism)

	conn.SetWriteDeadline(time.Now().Add(healthWriteTimeout))
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("write sasl handshake: %w", err)
	}

	// Read SaslHandshake Response
	conn.SetReadDeadline(time.Now().Add(healthReadTimeout))
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return fmt.Errorf("read handshake size: %w", err)
	}
	respSize := binary.BigEndian.Uint32(sizeBuf[:])
	respBuf := make([]byte, respSize)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return fmt.Errorf("read handshake body: %w", err)
	}
	if len(respBuf) < 6 {
		return fmt.Errorf("handshake response too short")
	}
	// Response: [correlation_id:4] [error_code:2] [mechanisms...]
	errorCode := int16(binary.BigEndian.Uint16(respBuf[4:6]))
	if errorCode != 0 {
		return fmt.Errorf("sasl handshake error code: %d", errorCode)
	}

	// -- SaslAuthenticate Request --
	// Auth bytes for PLAIN: \x00username\x00password
	// Wire format: auth_bytes_len (INT32) + auth_bytes
	authBytes := append([]byte{0}, []byte(username)...)
	authBytes = append(authBytes, 0)
	authBytes = append(authBytes, []byte(password)...)

	bodyLen = 4 + len(authBytes) // INT32 length prefix + bytes
	totalSize = 2 + 2 + 4 + 2 + len(clientID) + bodyLen

	buf2 := make([]byte, 4+totalSize)
	binary.BigEndian.PutUint32(buf2[0:4], uint32(totalSize))
	binary.BigEndian.PutUint16(buf2[4:6], uint16(saslAuthenticateAPIKey))
	binary.BigEndian.PutUint16(buf2[6:8], 1) // api_version
	binary.BigEndian.PutUint32(buf2[8:12], 2) // correlation_id
	binary.BigEndian.PutUint16(buf2[12:14], uint16(len(clientID)))
	copy(buf2[14:], clientID)
	authLenOffset := 14 + len(clientID)
	binary.BigEndian.PutUint32(buf2[authLenOffset:], uint32(len(authBytes)))
	copy(buf2[authLenOffset+4:], authBytes)

	conn.SetWriteDeadline(time.Now().Add(healthWriteTimeout))
	if _, err := conn.Write(buf2); err != nil {
		return fmt.Errorf("write sasl authenticate: %w", err)
	}

	// Read SaslAuthenticate Response
	conn.SetReadDeadline(time.Now().Add(healthReadTimeout))
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return fmt.Errorf("read authenticate size: %w", err)
	}
	respSize = binary.BigEndian.Uint32(sizeBuf[:])
	respBuf = make([]byte, respSize)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return fmt.Errorf("read authenticate body: %w", err)
	}
	if len(respBuf) < 6 {
		return fmt.Errorf("authenticate response too short")
	}
	// Response: [correlation_id:4] [error_code:2] [error_message] [auth_bytes]
	errorCode = int16(binary.BigEndian.Uint16(respBuf[4:6]))
	if errorCode != 0 {
		return fmt.Errorf("sasl authenticate error code: %d", errorCode)
	}

	return nil
}
