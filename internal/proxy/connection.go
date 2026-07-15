package proxy

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/failover"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
	"github.com/vfolgosa/bifrost-proxy/internal/pool"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
	"github.com/vfolgosa/bifrost-proxy/internal/routing"
)

// metadataForwardTimeout is the maximum time to wait for a single cluster's
// MetadataResponse before treating it as a timeout in load_balance mode.
const metadataForwardTimeout = 5 * time.Second

// upstreamDialTimeout is the maximum time to wait for an upstream TCP/TLS
// connection to be established.
const upstreamDialTimeout = 10 * time.Second

// MaxFrameSize is the maximum allowed Kafka frame size to prevent memory
// exhaustion attacks. 100MB should accommodate any legitimate produce batch.
const MaxFrameSize = 100 * 1024 * 1024

// dialUpstream opens a TCP or TLS connection to the given address.
// When useTLS is true, a TLS connection is established with verification
// disabled (Confluent Cloud uses publicly trusted certificates; InsecureSkipVerify
// is set for internal/private CA scenarios and development testing).
func dialUpstream(addr string, useTLS bool) (net.Conn, error) {
	if useTLS {
		tlsCfg := &tls.Config{
			// InsecureSkipVerify is acceptable because:
			// 1. Confluent Cloud endpoints use publicly trusted certificates.
			// 2. Internal/private CA scenarios need flexibility.
			// Production deployments should set this based on their PKI.
			InsecureSkipVerify: true,
		}
		return tls.DialWithDialer(&net.Dialer{Timeout: upstreamDialTimeout}, "tcp", addr, tlsCfg)
	}
	return net.DialTimeout("tcp", addr, upstreamDialTimeout)
}

// endpointUsesTLS always returns false — upstream connections use plain TCP.
func endpointUsesTLS(_ config.ClusterConfig, _ string) bool { return false }

// rebalancers holds per-cluster Rebalancer instances, keyed by cluster name.
var rebalancers sync.Map // map[string]*failover.Rebalancer

// getRebalancer returns the Rebalancer for the given cluster, creating one
// lazily if it doesn't exist.
func getRebalancer(clusterName string, clusterCfg config.ClusterConfig) *failover.Rebalancer {
	if v, ok := rebalancers.Load(clusterName); ok {
		return v.(*failover.Rebalancer)
	}
	rb := failover.NewRebalancer(clusterCfg)
	actual, _ := rebalancers.LoadOrStore(clusterName, rb)
	return actual.(*failover.Rebalancer)
}

// upstreamTarget resolves the target upstream address from a cluster config.
// For load_balance mode, it uses the Rebalancer's SelectTarget.
// If drCoord is non-nil, the DR state machine is consulted during DRAINING
// to determine where new connections should route.
func upstreamTarget(clusterCfg config.ClusterConfig, drCoord *DRCoordinator, clusterName string) (addr string, target string, ok bool) {
	switch clusterCfg.Mode {
	case config.ModeActivePassive:
		// Consult the DR coordinator for state-aware routing.
		if drCoord != nil {
			target, ok := drCoord.TargetForRouting(clusterName)
			if ok {
				if target == config.ActivePrimary {
					return clusterCfg.Primary.Bootstrap, "primary", true
				}
				return clusterCfg.Secondary.Bootstrap, "secondary", true
			}
		}
		// Fall back to config-based routing.
		if clusterCfg.Active == config.ActivePrimary {
			return clusterCfg.Primary.Bootstrap, "primary", true
		}
		return clusterCfg.Secondary.Bootstrap, "secondary", true
	case config.ModeLoadBalance:
		// Return primary bootstrap as default; selectLoadBalanceTarget
		// refines this per-connection based on effective weights.
		return clusterCfg.Primary.Bootstrap, "primary", true
	case config.ModeSingle:
		return clusterCfg.Primary.Bootstrap, "primary", true
	default:
		return "", "", false
	}
}

// selectLoadBalanceTarget uses the Rebalancer to pick the target endpoint
// for load_balance mode. Returns the address and target name (\"primary\"
// or \"secondary\").
func selectLoadBalanceTarget(clusterName string, clusterCfg config.ClusterConfig, router *routing.Router) (addr string, target string, ok bool) {
	// Use the Router's effective weights (adjusted by health-based rebalancer).
	if router != nil {
		primW, secW := router.GetEffectiveWeights(clusterName, clusterCfg)
		if primW == 0 && secW > 0 {
			return clusterCfg.Secondary.Bootstrap, config.ActiveSecondary, true
		}
		if secW == 0 && primW > 0 {
			return clusterCfg.Primary.Bootstrap, config.ActivePrimary, true
		}
		// Both healthy — use effective weights for weighted selection.
		if primW > 0 && secW > 0 {
			n := atomic.AddUint64(&selectCounter, 1)
			if int(n%100) < primW {
				return clusterCfg.Primary.Bootstrap, config.ActivePrimary, true
			}
			return clusterCfg.Secondary.Bootstrap, config.ActiveSecondary, true
		}
	}
	// Fallback: use failover.Rebalancer connection-based selection.
	rb := getRebalancer(clusterName, clusterCfg)
	addr = rb.SelectTarget()
	if addr == rb.PrimaryAddr() {
		return addr, config.ActivePrimary, true
	}
	return addr, config.ActiveSecondary, true
}

var selectCounter uint64

// hashToPrimary uses configured primary weight for legacy failover.Rebalancer path.
func hashToPrimary(cfg config.ClusterConfig) bool {
	n := atomic.AddUint64(&selectCounter, 1)
	return int(n%100) < cfg.Primary.Weight
}

// recordLoadBalanceSuccess records a successful connection to the given
// address for rebalancing purposes.
func recordLoadBalanceSuccess(clusterName string, clusterCfg config.ClusterConfig, addr string) {
	if clusterCfg.Mode == config.ModeLoadBalance {
		rb := getRebalancer(clusterName, clusterCfg)
		rb.RecordSuccess(addr)
	}
}

// recordLoadBalanceFailure records a failed connection to the given address
// for rebalancing purposes.
func recordLoadBalanceFailure(clusterName string, clusterCfg config.ClusterConfig, addr string) {
	if clusterCfg.Mode == config.ModeLoadBalance {
		rb := getRebalancer(clusterName, clusterCfg)
		rb.RecordFailure(addr)
	}
}

// handlePlaintextConnection does simple TCP upstream connect + bidirectional
// passthrough without any Kafka protocol awareness. Used for plaintext
// (non-TLS) connections in test/dev scenarios.
//
// If dm is non-nil, the connection is registered with the DrainManager
// for active-connection tracking and drain support.
// If drCoord is non-nil, routing decisions consult the DR state machine.
func handlePlaintextConnection(log *logger.Logger, conn net.Conn, clusterName string, clusterCfg config.ClusterConfig, dm *DrainManager, router *routing.Router, drCoord *DRCoordinator) {
	defer conn.Close()

	var upstreamAddr, targetCluster string
	var ok bool
	if clusterCfg.Mode == config.ModeLoadBalance {
		upstreamAddr, targetCluster, ok = selectLoadBalanceTarget(clusterName, clusterCfg, router)
	} else {
		upstreamAddr, targetCluster, ok = upstreamTarget(clusterCfg, drCoord, clusterName)
	}
	if !ok {
		log.Error("unknown proxy mode for cluster", "mode", clusterCfg.Mode, "cluster", clusterName)
		return
	}
	if upstreamAddr == "" {
		log.Error("empty bootstrap address for cluster", "cluster", clusterName, "target", targetCluster)
		return
	}

	// Register with DrainManager for active connection tracking.
	var regID uint64
	if dm != nil {
		regID = dm.Register(clusterName, conn, targetCluster)
		defer dm.Unregister(clusterName, regID)
	}

	log.Info("opening upstream connection",
		"upstream_addr", upstreamAddr, "cluster", clusterName,
		"mode", clusterCfg.Mode, "target", targetCluster)

	useTLS := endpointUsesTLS(clusterCfg, targetCluster)
	upstreamConn, err := dialUpstream(upstreamAddr, useTLS)
	if err != nil {
		log.Error("failed to connect to upstream",
			"upstream_addr", upstreamAddr, "cluster", clusterName, "error", err)
		recordLoadBalanceFailure(clusterName, clusterCfg, upstreamAddr)
		return
	}
	defer upstreamConn.Close()

	// Record successful connection for rebalancing.
	recordLoadBalanceSuccess(clusterName, clusterCfg, upstreamAddr)

	log.Info("connected to upstream",
		"upstream_addr", upstreamAddr, "cluster", clusterName, "target", targetCluster)

	// Simple bidirectional passthrough.
	done := make(chan struct{}, 2)

	go func() {
		n, err := io.Copy(upstreamConn, conn)
		if err != nil && err != io.EOF {
			log.Error("client→upstream copy error",
				"error", err, "bytes", n, "cluster", clusterName, "target", targetCluster)
		}
		done <- struct{}{}
	}()

	go func() {
		n, err := io.Copy(conn, upstreamConn)
		if err != nil && err != io.EOF {
			log.Error("upstream→client copy error",
				"error", err, "bytes", n, "cluster", clusterName, "target", targetCluster)
		}
		done <- struct{}{}
	}()

	<-done
	upstreamConn.Close()
	conn.Close()

	log.Info("plaintext connection closed",
		"cluster", clusterName, "target", targetCluster, "upstream_addr", upstreamAddr)
}

// handleConnection manages a single client connection lifecycle.
// Opens a TCP or TLS connection to the upstream Kafka bootstrap server,
// handles SASL authentication (blind passthrough for Handshake + Authenticate),
// intercepts Metadata requests (API Key 3) for broker list rewriting,
// routes Produce/Fetch requests (API Key 0/1) through the partition-aware
// Router, and performs bidirectional passthrough for all other traffic.
//
// If dm is non-nil, the connection is registered with the DrainManager
// for active-connection tracking and drain support.
// If drCoord is non-nil, routing decisions consult the DR state machine.
func handleConnection(log *logger.Logger, conn net.Conn, clusterName string, clusterCfg config.ClusterConfig, proxyPort int32, advertiseHost string, dm *DrainManager, router *routing.Router, drCoord *DRCoordinator) {
	defer conn.Close()

	// Determine target upstream bootstrap address based on cluster config.
	// For load_balance mode, use the Rebalancer for weighted selection.
	var upstreamAddr, targetCluster string
	var ok bool
	if clusterCfg.Mode == config.ModeLoadBalance {
		upstreamAddr, targetCluster, ok = selectLoadBalanceTarget(clusterName, clusterCfg, router)
	} else {
		upstreamAddr, targetCluster, ok = upstreamTarget(clusterCfg, drCoord, clusterName)
	}
	if !ok {
		log.Error("unknown proxy mode for cluster", "mode", clusterCfg.Mode, "cluster", clusterName)
		return
	}

	if upstreamAddr == "" {
		log.Error("empty bootstrap address for cluster", "cluster", clusterName, "target", targetCluster)
		return
	}

	// Register with DrainManager for active connection tracking.
	// Unregister on return so drain force-close doesn't double-close.
	var regID uint64
	if dm != nil {
		regID = dm.Register(clusterName, conn, targetCluster)
		defer dm.Unregister(clusterName, regID)
	}

	log.Info("opening upstream connection",
		"upstream_addr", upstreamAddr, "cluster", clusterName,
		"mode", clusterCfg.Mode, "target", targetCluster)

	// Open TCP or TLS connection to upstream Kafka based on endpoint config.
	useTLS := endpointUsesTLS(clusterCfg, targetCluster)
	upstreamConn, err := dialUpstream(upstreamAddr, useTLS)
	if err != nil {
		log.Error("failed to connect to upstream",
			"upstream_addr", upstreamAddr, "cluster", clusterName, "error", err)
		recordLoadBalanceFailure(clusterName, clusterCfg, upstreamAddr)
		return
	}
	defer upstreamConn.Close()

	// Record successful connection for rebalancing.
	recordLoadBalanceSuccess(clusterName, clusterCfg, upstreamAddr)

	log.Info("connected to upstream",
		"upstream_addr", upstreamAddr, "cluster", clusterName, "target", targetCluster)

	// ── SASL Authentication Passthrough (T14) ─────────────────────────
	//
	// Kafka clients that use SASL will send SaslHandshake (API Key 17)
	// followed by one or more SaslAuthenticate (API Key 36) frames
	// before any other API calls. For multi-step mechanisms like SCRAM,
	// multiple SaslAuthenticate round-trips occur.
	//
	// HandleSASLExchange loops, forwarding all SASL frames byte-for-byte
	// between client and upstream. It returns the full Kafka frame header
	// of the first non-SASL frame, including correlation_id and client_id.
	saslHandler := &routing.SASLHandler{}
	result, err := routing.HandleSASLExchange(saslHandler, conn, upstreamConn)
	if err != nil {
		log.Error("SASL exchange failed", "error", err)
		return
	}

	// Attach correlation_id and client_id to the connection logger.
	if result != nil {
		if result.ClientID != "" {
			log = log.WithClientID(result.ClientID)
		}
		if result.CorrelationID != 0 {
			log = log.WithCorrelationID(result.CorrelationID)
		}
	}

	if saslHandler.Authenticated() {
		log.Info("SASL authentication completed",
			"remote_addr", conn.RemoteAddr(), "cluster", clusterName)
	}

	// ── First non-SASL frame routing ──────────────────────────────────
	//
	// After SASL, result.Header contains the full Kafka frame header
	// (size + api_key + api_version + correlation_id + client_id).
	// Route based on API key:
	//   - Metadata (3): intercept and rewrite broker list
	//   - Produce (0) / Fetch (1): partition-aware routing via Router (T12)
	//   - Everything else: forward to upstream + bidirectional passthrough
	didIntercept := false

	if result != nil {
		firstBytes := result.Header
		apiKey := int16(binary.BigEndian.Uint16(firstBytes[4:6]))
		frameSize := int32(binary.BigEndian.Uint32(firstBytes[0:4]))

		// Track produce/fetch by target cluster for dashboard metrics.
		if apiKey == protocol.APIKeyProduce {
			routing.RecordClusterProduce(targetCluster)
		} else if apiKey == protocol.APIKeyFetch {
			routing.RecordClusterFetch(targetCluster)
		}

		if apiKey == protocol.APIKeyMetadata && frameSize >= 2 {
			// Validate frame size to prevent memory exhaustion attacks.
			if frameSize < 14 || frameSize > MaxFrameSize {
				log.Error("invalid frame size in non-SASL frame",
					"api_key", apiKey, "frame_size", frameSize)
				return
			}
			switch {

			// ── active_passive: single-cluster rewrite ─────────
			case clusterCfg.Mode == config.ModeActivePassive:
				didIntercept = handleMetadataActivePassive(log, conn, upstreamConn, firstBytes, frameSize, advertiseHost, proxyPort)

			// ── load_balance: synthetic merge ─────────────────
			case clusterCfg.Mode == config.ModeLoadBalance:
				didIntercept = handleMetadataLoadBalance(log, conn, firstBytes, frameSize, advertiseHost, clusterCfg, proxyPort)
			}
		}

		// ── Produce/Fetch: partition-aware routing (T12) ───────────────
		if !didIntercept && (apiKey == protocol.APIKeyProduce || apiKey == protocol.APIKeyFetch) {
			log.Info("routing Produce/Fetch request, handing off to Router",
				"api_key", apiKey)

			// Close the bootstrap upstream connection — routing will use
			// the Router's own connection pool to connect directly to the
			// partition leader.
			upstreamConn.Close()

			// Prepend the already-read firstBytes to the client connection
			// so Router.Route can parse the full frame.
			prependedConn := io.MultiReader(
				bytesReader(firstBytes),
				conn,
			)

			// Router.Route reads the full frame, parses topic+partition,
			// looks up the leader from cache, gets a pooled connection,
			// forwards the request, streams the response, and continues
			// with bidirectional passthrough for the rest of the connection.
			if err := router.Route(log, &readWriteConn{Reader: prependedConn, Conn: conn}, clusterName, clusterCfg); err != nil {
				log.Error("error routing Produce/Fetch", "error", err)
			}
			return
		}

		// If not intercepted and not routed, forward the first bytes to upstream
		if !didIntercept {
			upstreamConn.Write(firstBytes)
		}
	}

	// ── Bidirectional passthrough for remaining traffic ───────────
	done := make(chan struct{}, 2)

	// Client → Upstream (produce direction)
	go func() {
		bn, berr := io.Copy(upstreamConn, conn)
		routing.RecordClusterProduceBytes(targetCluster, bn)
		if berr != nil && berr != io.EOF {
			log.Error("client→upstream copy error",
				"error", berr, "bytes", bn, "cluster", clusterName, "target", targetCluster)
		}
		done <- struct{}{}
	}()

	// Upstream → Client (fetch direction)
	go func() {
		bn, berr := io.Copy(conn, upstreamConn)
		routing.RecordClusterFetchBytes(targetCluster, bn)
		if berr != nil && berr != io.EOF {
			log.Error("upstream→client copy error",
				"error", berr, "bytes", bn, "cluster", clusterName, "target", targetCluster)
		}
		done <- struct{}{}
	}()

	// Wait for either direction to finish
	<-done

	// Close both ends to unblock the remaining goroutine
	upstreamConn.Close()
	conn.Close()

	log.Info("connection closed",
		"cluster", clusterName, "target", targetCluster, "upstream_addr", upstreamAddr)
}

// handleMetadataActivePassive handles Metadata interception for active_passive
// mode: forwards the request to the active cluster, rewrites broker host:port
// in the response, and sends the altered response to the client.
// Returns true if interception succeeded.
func handleMetadataActivePassive(log *logger.Logger, conn net.Conn, upstreamConn net.Conn, firstBytes []byte, frameSize int32, advertiseHost string, proxyPort int32) bool {
	// Read the rest of the Metadata request.
	// firstBytes includes the full Kafka header (size + api_key + api_version + correlation_id + client_id).
	// Subtract the header bytes (minus the 4-byte size prefix) from frameSize.
	remaining := make([]byte, frameSize-int32(len(firstBytes)-4))
	if _, err := io.ReadFull(conn, remaining); err != nil {
		log.Error("failed to read metadata request body", "error", err)
		return false
	}

	// Reconstruct full request frame.
	// firstBytes includes: size[0:4] + header[4:...]
	headerLen := len(firstBytes) - 4
	fullReq := make([]byte, 4+frameSize)
	copy(fullReq[0:4], firstBytes[0:4])
	copy(fullReq[4:4+headerLen], firstBytes[4:])
	copy(fullReq[4+headerLen:], remaining)

	// Forward request to upstream
	if _, err := upstreamConn.Write(fullReq); err != nil {
		log.Error("failed to forward metadata request", "error", err)
		return false
	}

	// Read response from upstream
	respSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(upstreamConn, respSizeBuf); err != nil {
		log.Error("failed to read metadata response size", "error", err)
		return false
	}
	respSize := int32(binary.BigEndian.Uint32(respSizeBuf))
	if respSize < 0 || respSize > pool.MaxMetadataResponseSize {
		log.Error("metadata response size out of range",
			"size", respSize, "max", pool.MaxMetadataResponseSize)
		return false
	}

	respBody := make([]byte, respSize)
	if _, err := io.ReadFull(upstreamConn, respBody); err != nil {
		log.Error("failed to read metadata response body", "error", err)
		return false
	}

	rawResp := make([]byte, 4+respSize)
	copy(rawResp[0:4], respSizeBuf)
	copy(rawResp[4:], respBody)

	// Extract API version from request
	apiVersion := int16(binary.BigEndian.Uint16(fullReq[6:8]))

	// Rewrite broker list + recalculate frame size
	rewritten, err := routing.RewriteMetadataResponse(rawResp, advertiseHost, proxyPort, apiVersion)
	if err != nil {
		log.Error("failed to rewrite metadata response", "error", err)
		conn.Write(rawResp) // fall through: send original
	} else {
		log.Info("metadata rewritten",
			"advertise_host", advertiseHost, "original_bytes", len(rawResp),
			"rewritten_bytes", len(rewritten), "proxy_port", proxyPort)
		conn.Write(rewritten)
	}
	return true
}

// handleMetadataLoadBalance handles Metadata interception for load_balance
// mode: forwards the request concurrently to both primary and secondary,
// merges the responses via SynthesizeMetadataResponse, and sends the
// synthetic response to the client.
// Returns true if interception succeeded.
func handleMetadataLoadBalance(log *logger.Logger, conn net.Conn, firstBytes []byte, frameSize int32, advertiseHost string, clusterCfg config.ClusterConfig, proxyPort int32) bool {
	// Read the rest of the Metadata request.
	// firstBytes includes the full Kafka header (size + api_key + api_version + correlation_id + client_id).
	// Subtract the header bytes (minus the 4-byte size prefix) from frameSize.
	remaining := make([]byte, frameSize-int32(len(firstBytes)-4))
	if _, err := io.ReadFull(conn, remaining); err != nil {
		log.Error("failed to read metadata request body", "error", err)
		return false
	}

	// Reconstruct full request frame.
	// firstBytes includes: size[0:4] + header[4:...]
	headerLen := len(firstBytes) - 4
	fullReq := make([]byte, 4+frameSize)
	copy(fullReq[0:4], firstBytes[0:4])
	copy(fullReq[4:4+headerLen], firstBytes[4:])
	copy(fullReq[4+headerLen:], remaining)

	// Extract API version from request
	apiVersion := int16(binary.BigEndian.Uint16(fullReq[6:8]))

	log.Info("metadata load_balance: forwarding to both clusters",
		"primary", clusterCfg.Primary.Bootstrap,
		"secondary", clusterCfg.Secondary.Bootstrap,
		"api_version", apiVersion)

	// Forward request to both clusters concurrently
	type result struct {
		data []byte
		err  error
	}
	chPrimary := make(chan result, 1)
	chSecondary := make(chan result, 1)

	forwardOne := func(bootstrap string, useTLS bool) []byte {
		resp, err := forwardMetadataRequest(bootstrap, fullReq, useTLS)
		if err != nil {
			log.Warn("metadata forward failed",
				"bootstrap", bootstrap, "error", err)
			return nil
		}
		return resp
	}

	go func() {
		r := result{data: forwardOne(clusterCfg.Primary.Bootstrap, false)}
		chPrimary <- r
	}()
	go func() {
		r := result{data: forwardOne(clusterCfg.Secondary.Bootstrap, false)}
		chSecondary <- r
	}()

	// Wait for both (or let them time out internally)
	priResp := <-chPrimary
	secResp := <-chSecondary

	if priResp.data == nil && secResp.data == nil {
		log.Error("both clusters failed to respond to metadata request")
		return false
	}

	// Synthesize the merged response
	synthetic, err := routing.SynthesizeMetadataResponse(priResp.data, secResp.data, advertiseHost, proxyPort, apiVersion)
	if err != nil {
		log.Error("failed to synthesize metadata response", "error", err)
		// Fall back: send whichever response we have (primary preferred)
		if priResp.data != nil {
			conn.Write(priResp.data)
		} else {
			conn.Write(secResp.data)
		}
	} else {
		log.Info("metadata synthesized",
			"advertise_host", advertiseHost, "bytes", len(synthetic),
			"primary_ok", priResp.data != nil, "secondary_ok", secResp.data != nil)
		conn.Write(synthetic)
	}
	return true
}

// forwardMetadataRequest connects to a Kafka bootstrap server, sends a
// MetadataRequest frame, reads the MetadataResponse frame, and returns
// it. Returns nil and an error on failure or timeout.
// When useTLS is true, the connection is established over TLS.
func forwardMetadataRequest(bootstrap string, requestFrame []byte, useTLS bool) ([]byte, error) {
	conn, err := dialUpstream(bootstrap, useTLS)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", bootstrap, err)
	}
	defer conn.Close()

	// Set deadline for the entire request-response cycle
	conn.SetDeadline(time.Now().Add(metadataForwardTimeout))

	// Send request
	if _, err := conn.Write(requestFrame); err != nil {
		return nil, fmt.Errorf("write request to %s: %w", bootstrap, err)
	}

	// Read response size prefix
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, fmt.Errorf("read response size from %s: %w", bootstrap, err)
	}
	respSize := int32(binary.BigEndian.Uint32(sizeBuf))

	if respSize < 4 || respSize > pool.MaxMetadataResponseSize {
		return nil, fmt.Errorf("invalid response size %d from %s (max %d)", respSize, bootstrap, pool.MaxMetadataResponseSize)
	}

	// Read response body
	respBody := make([]byte, respSize)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		return nil, fmt.Errorf("read response body from %s: %w", bootstrap, err)
	}

	// Build complete response frame
	rawResp := make([]byte, 4+respSize)
	copy(rawResp[0:4], sizeBuf)
	copy(rawResp[4:], respBody)

	return rawResp, nil
}

// ── Route helpers ────────────────────────────────────────────────────

// bytesReader wraps []byte as an io.Reader for use with io.MultiReader.
func bytesReader(data []byte) io.Reader {
	return &byteReader{data: data}
}

type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// readWriteConn wraps a separate io.Reader and net.Conn so that the
// MultiReader (which prepends pre-read bytes) is used for reads while
// the real connection is used for writes.
// This is used to pass pre-read frame data to Router.Route while
// keeping the write path to the original client connection.
type readWriteConn struct {
	io.Reader
	net.Conn
}

// Read delegates to the embedded io.Reader (the MultiReader).
// Without an explicit Read method, the embedded io.Reader and net.Conn
// both expose Read, causing an ambiguous selector — so this resolves it.
func (rw *readWriteConn) Read(b []byte) (int, error) {
	return rw.Reader.Read(b)
}

// Write delegates to the underlying net.Conn for writing.
func (rw *readWriteConn) Write(b []byte) (int, error) {
	return rw.Conn.Write(b)
}
