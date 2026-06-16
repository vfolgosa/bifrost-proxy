// Package pool implements a per-broker TLS connection pool for the Kafka proxy.
//
// The connection pool uses a buffered channel of *tls.Conn as a semaphore
// pattern: a semaphore channel (capacity = maxSize) controls the total number
// of connections per broker, while an idle channel holds connections ready
// for reuse. Background goroutines handle idle connection cleanup and periodic
// health checks via Kafka MetadataRequest.
package pool

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

const (
	defaultMaxConnections = 50
	defaultIdleTimeout    = 30 * time.Second
	defaultKeepAlive      = 30 * time.Second
	cleanupTickInterval   = 5 * time.Second
)

// KafkaMetadataAPIKey is the Kafka protocol API key for Metadata requests.
const KafkaMetadataAPIKey int16 = 3

// pooledConn wraps a TLS connection with its last-used timestamp for idle
// timeout tracking.
type pooledConn struct {
	conn     net.Conn
	lastUsed time.Time
}

// brokerPool manages a bounded set of TLS connections to a single Kafka broker.
// Uses a buffered channel as a semaphore pattern:
//   - idle channel holds available connections ready for reuse
//   - sem channel (capacity = maxSize) controls total connection count
//
// Every connection in existence occupies one sem slot. Closing a connection
// (idle timeout or failed health check) releases its sem slot.
type brokerPool struct {
	idle    chan *pooledConn // buffered channel of idle connections
	sem     chan struct{}    // semaphore, capacity = maxSize
	maxSize int
	addr    string
}

// ConnectionPool manages per-broker connection pools with idle timeout
// cleanup, TCP keep-alive, and periodic health checks via MetadataRequest.
// When tlsCfg is nil, plain TCP connections are used (for non-TLS upstreams).
type ConnectionPool struct {
	pools  map[string]*brokerPool
	mu     sync.Mutex
	cfg    config.ConnectionPoolConfig
	tlsCfg *tls.Config // nil = plain TCP

	// Buffer pool for connection read/write buffers (as specified).
	bufPool sync.Pool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New creates a ConnectionPool from the proxy pool configuration and starts
// background goroutines for idle connection cleanup and health checks.
// If tlsCfg is nil, plain TCP connections are used.
func New(cfg config.ConnectionPoolConfig, tlsCfg *tls.Config) *ConnectionPool {
	cp := &ConnectionPool{
		pools:  make(map[string]*brokerPool),
		cfg:    cfg,
		stopCh: make(chan struct{}),
		bufPool: sync.Pool{
			New: func() any {
				buf := make([]byte, 0, 4096)
				return &buf
			},
		},
	}

	idleTimeout := cfg.IdleTimeout.Duration()
	if idleTimeout <= 0 {
		idleTimeout = defaultIdleTimeout
	}

	keepAliveInterval := cfg.KeepAliveInterval.Duration()
	if keepAliveInterval <= 0 {
		keepAliveInterval = defaultKeepAlive
	}

	cp.wg.Add(1)
	go cp.idleCleanupLoop(idleTimeout)

	cp.wg.Add(1)
	go cp.healthCheckLoop(keepAliveInterval)

	return cp
}

// Get retrieves a connection for the given broker address.
// If an idle connection is available and not expired, it is returned.
// Otherwise, a new connection is created (up to maxSize per broker).
// If the pool is at capacity, Get blocks until a connection is returned
// via Put or an idle connection is closed by the cleanup goroutine.
func (cp *ConnectionPool) Get(addr string) (net.Conn, error) {
	bp := cp.getOrCreatePool(addr)

	for {
		// Try to retrieve an idle connection (non-blocking).
		select {
		case pc := <-bp.idle:
			// Check if the connection is still alive and not idle-expired.
			if pc.isAlive(cp.idleTimeout()) {
				pc.lastUsed = time.Now()
				return pc.conn, nil
			}
			// Connection expired or dead — close it and release the sem slot.
			pc.close()
			<-bp.sem
			// Loop and try again.
			continue
		default:
		}

		// No idle connection available. Try to acquire a semaphore slot
		// to create a new connection (non-blocking).
		select {
		case bp.sem <- struct{}{}:
			// Slot acquired — create a new connection.
			conn, err := dialTLS(addr, cp.keepAliveInterval(), cp.tlsCfg)
			if err != nil {
				// Release the slot on failure.
				<-bp.sem
				return nil, fmt.Errorf("dial broker %s: %w", addr, err)
			}
			return conn, nil
		default:
		}

		// Pool is at capacity — block until a connection becomes available.
		pc := <-bp.idle
		if pc.isAlive(cp.idleTimeout()) {
			pc.lastUsed = time.Now()
			return pc.conn, nil
		}
		pc.close()
		<-bp.sem
	}
}

// Put returns a TLS connection to the pool for reuse. The caller must not
// use the connection after calling Put.
func (cp *ConnectionPool) Put(addr string, conn net.Conn) {
	cp.mu.Lock()
	bp, ok := cp.pools[addr]
	if !ok {
		// Pool doesn't exist for this address (unusual, but handle gracefully).
		cp.mu.Unlock()
		conn.Close()
		return
	}
	cp.mu.Unlock()

	pc := &pooledConn{
		conn:     conn,
		lastUsed: time.Now(),
	}

	bp.idle <- pc
}

// Close shuts down all background goroutines, drains idle connections,
// and closes them.
func (cp *ConnectionPool) Close() {
	close(cp.stopCh)
	cp.wg.Wait()

	cp.mu.Lock()
	defer cp.mu.Unlock()

	for addr, bp := range cp.pools {
		cp.drainPool(bp)
		delete(cp.pools, addr)
	}
}

// getOrCreatePool returns the brokerPool for the given address, creating one
// if it doesn't exist. Must be called without holding cp.mu (acquires it
// internally, or the caller may hold it).
func (cp *ConnectionPool) getOrCreatePool(addr string) *brokerPool {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if bp, ok := cp.pools[addr]; ok {
		return bp
	}

	maxSize := cp.cfg.MaxConnectionsPerBroker
	if maxSize <= 0 {
		maxSize = defaultMaxConnections
	}

	bp := &brokerPool{
		idle:    make(chan *pooledConn, maxSize),
		sem:     make(chan struct{}, maxSize),
		maxSize: maxSize,
		addr:    addr,
	}
	cp.pools[addr] = bp
	return bp
}

// ── Background goroutines ──────────────────────────────────────────────

// idleCleanupLoop periodically drains idle connections and closes those
// that have been idle longer than the configured timeout.
func (cp *ConnectionPool) idleCleanupLoop(idleTimeout time.Duration) {
	defer cp.wg.Done()

	ticker := time.NewTicker(cleanupTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cp.stopCh:
			return
		case <-ticker.C:
			cp.cleanupIdleConns(idleTimeout)
		}
	}
}

// cleanupIdleConns drains each broker pool's idle channel (non-blocking),
// closes expired connections, and returns still-valid ones.
func (cp *ConnectionPool) cleanupIdleConns(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, bp := range cp.pools {
		cp.cleanupBrokerPool(bp, idleTimeout)
	}
}

// cleanupBrokerPool drains all idle connections from a single broker pool,
// closes expired ones, and puts back still-valid ones.
func (cp *ConnectionPool) cleanupBrokerPool(bp *brokerPool, idleTimeout time.Duration) {
	// Drain all currently idle connections (non-blocking).
	var idleConns []*pooledConn
	for {
		select {
		case pc := <-bp.idle:
			idleConns = append(idleConns, pc)
		default:
			goto drained
		}
	}
drained:

	for _, pc := range idleConns {
		if time.Since(pc.lastUsed) > idleTimeout {
			// Connection idle too long — close and release sem slot.
			pc.close()
			<-bp.sem
		} else {
			// Still valid — return to idle channel.
			bp.idle <- pc
		}
	}
}

// healthCheckLoop periodically sends Kafka MetadataRequests to verify
// broker connections are alive.
func (cp *ConnectionPool) healthCheckLoop(interval time.Duration) {
	defer cp.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-cp.stopCh:
			return
		case <-ticker.C:
			cp.runHealthChecks()
		}
	}
}

// runHealthChecks issues a MetadataRequest to each broker that has a pool.
func (cp *ConnectionPool) runHealthChecks() {
	cp.mu.Lock()
	addrs := make([]string, 0, len(cp.pools))
	for addr := range cp.pools {
		addrs = append(addrs, addr)
	}
	cp.mu.Unlock()

	for _, addr := range addrs {
		// Use Get/Put so the pool capacity is respected.
		conn, err := cp.Get(addr)
		if err != nil {
			continue
		}

		if err := writeMetadataRequest(conn); err != nil {
			conn.Close()
			// The connection was obtained via Get, so we consumed a sem slot.
			// Release it since we're closing the connection.
			cp.releaseSem(addr)
			continue
		}

		if err := readMetadataResponse(conn); err != nil {
			conn.Close()
			cp.releaseSem(addr)
			continue
		}

		// Connection is healthy — return it to the pool.
		cp.Put(addr, conn)
	}
}

// releaseSem releases a semaphore slot for the given broker address.
func (cp *ConnectionPool) releaseSem(addr string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if bp, ok := cp.pools[addr]; ok {
		select {
		case <-bp.sem:
		default:
			// Already released (shouldn't happen normally).
		}
	}
}

// ── Helpers ────────────────────────────────────────────────────────────

// drainPool empties the idle channel of a broker pool and closes all
// connections. Assumes cp.mu is held.
func (cp *ConnectionPool) drainPool(bp *brokerPool) {
	close(bp.idle)
	for pc := range bp.idle {
		pc.close()
	}
}

// isAlive checks whether a pooled connection is still usable.
func (pc *pooledConn) isAlive(idleTimeout time.Duration) bool {
	if pc.conn == nil {
		return false
	}
	if idleTimeout > 0 && time.Since(pc.lastUsed) > idleTimeout {
		return false
	}
	return true
}

// close closes the underlying TLS connection.
func (pc *pooledConn) close() {
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}

// idleTimeout returns the configured idle timeout, falling back to the default.
func (cp *ConnectionPool) idleTimeout() time.Duration {
	d := cp.cfg.IdleTimeout.Duration()
	if d <= 0 {
		return defaultIdleTimeout
	}
	return d
}

// keepAliveInterval returns the configured keep-alive interval, falling back
// to the default.
func (cp *ConnectionPool) keepAliveInterval() time.Duration {
	d := cp.cfg.KeepAliveInterval.Duration()
	if d <= 0 {
		return defaultKeepAlive
	}
	return d
}

// dialTLS creates a new connection to the given address with TCP KeepAlive.
// If tlsCfg is nil, a plain TCP connection is returned (for non-TLS upstreams).
func dialTLS(addr string, keepAlive time.Duration, tlsCfg *tls.Config) (net.Conn, error) {
	d := net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: keepAlive,
	}

	rawConn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	// Plain TCP — no TLS wrapping.
	if tlsCfg == nil {
		return rawConn, nil
	}

	// Wrap in TLS. For the proxy's upstream connections, we use TLS but
	// skip hostname verification since we're connecting by IP or internal
	// broker addresses.
	tlsConn := tls.Client(rawConn, tlsCfg)

	if err := tlsConn.Handshake(); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("TLS handshake to %s: %w", addr, err)
	}

	return tlsConn, nil
}

// ── Kafka MetadataRequest helpers ─────────────────────────────────────

// writeMetadataRequest constructs and sends a Kafka MetadataRequest (API key 3)
// to verify the connection is alive.
func writeMetadataRequest(conn net.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// Build the request header bytes (without the size prefix).
	// Wire format after size: APIKey(2) + APIVersion(2) + CorrelationID(4) + ClientID
	clientID := "kafkaproxy-health"
	headerLen := 2 + 2 + 4 + 2 + len(clientID)
	header := make([]byte, headerLen)
	binary.BigEndian.PutUint16(header[0:2], uint16(KafkaMetadataAPIKey))
	binary.BigEndian.PutUint16(header[2:4], 0) // APIVersion 0
	binary.BigEndian.PutUint32(header[4:8], 0) // CorrelationID 0
	binary.BigEndian.PutUint16(header[8:10], uint16(len(clientID)))
	copy(header[10:], clientID)

	// Body: topics_count = 0 (request all topics, or empty response).
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body[0:4], 0)

	frame := protocol.WriteFrame(header, body)
	defer protocol.ReleaseFrame(frame)

	_, err := conn.Write(frame)
	return err
}

// readMetadataResponse reads and validates a MetadataResponse. A successful
// read (even with errors in the response body) indicates the connection is
// healthy — we only care that the TCP/TLS layer is functional.
func readMetadataResponse(conn net.Conn) error {
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Read the size prefix (4 bytes).
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return fmt.Errorf("read response size: %w", err)
	}

	respSize := binary.BigEndian.Uint32(sizeBuf[:])

	// Read the rest of the frame.
	respBuf := make([]byte, respSize)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	// Parse the response header to validate it's a proper Kafka response.
	// Response header: CorrelationID (int32).
	if len(respBuf) < 4 {
		return fmt.Errorf("response too short: %d bytes", len(respBuf))
	}
	correlationID := int32(binary.BigEndian.Uint32(respBuf[0:4]))
	_ = correlationID // Acknowledge it echoes our request; success is reaching this point.

	return nil
}
