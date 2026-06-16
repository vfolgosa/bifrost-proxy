package routing

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
	"github.com/vfolgosa/bifrost-proxy/internal/pool"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

// TopicStats tracks per-topic produce/fetch records for observability.
type TopicStats struct {
	Topic   string
	Produce int64
	Fetch   int64
}

var (
	topicStatsMu    sync.RWMutex
	topicStatsMap   = make(map[string]*[2]int64) // topic -> [produce, fetch]
	topicProduceSeq atomic.Int64
	topicFetchSeq   atomic.Int64
)

// RecordTopicProduce increments the produce counter for a topic.
func RecordTopicProduce(topic string) {
	topicStatsMu.Lock()
	e, ok := topicStatsMap[topic]
	if !ok {
		e = &[2]int64{}
		topicStatsMap[topic] = e
	}
	atomic.AddInt64(&e[0], 1)
	topicStatsMu.Unlock()
}

// RecordTopicFetch increments the fetch counter for a topic.
func RecordTopicFetch(topic string) {
	topicStatsMu.Lock()
	e, ok := topicStatsMap[topic]
	if !ok {
		e = &[2]int64{}
		topicStatsMap[topic] = e
	}
	atomic.AddInt64(&e[1], 1)
	topicStatsMu.Unlock()
}

// GetTopicStats returns a snapshot of all per-topic produce/fetch counts.
func GetTopicStats() []TopicStats {
	topicStatsMu.RLock()
	defer topicStatsMu.RUnlock()
	out := make([]TopicStats, 0, len(topicStatsMap))
	for t, e := range topicStatsMap {
		out = append(out, TopicStats{
			Topic:   t,
			Produce: atomic.LoadInt64(&e[0]),
			Fetch:   atomic.LoadInt64(&e[1]),
		})
	}
	return out
}

// ClusterStats tracks per-cluster produce/fetch records.
type ClusterStats struct {
	Cluster string `json:"cluster"` // "primary" or "secondary"
	Produce int64  `json:"produce"` // bytes
	Fetch   int64  `json:"fetch"`   // bytes
	Records int64  `json:"records"` // number of produce requests
}

var (
	clusterStatsMu  sync.RWMutex
	clusterStatsMap = make(map[string]*[3]int64) // cluster -> [produce_bytes, fetch_bytes, records]
)

// RecordClusterProduce increments produce counter for a target cluster.
func RecordClusterProduce(cluster string) {
	clusterStatsMu.Lock()
	e, ok := clusterStatsMap[cluster]
	if !ok {
		e = &[3]int64{}
		clusterStatsMap[cluster] = e
	}
	atomic.AddInt64(&e[0], 1)
	atomic.AddInt64(&e[2], 1) // also count as record
	clusterStatsMu.Unlock()
}

// RecordClusterFetch increments fetch counter for a target cluster.
func RecordClusterFetch(cluster string) {
	clusterStatsMu.Lock()
	e, ok := clusterStatsMap[cluster]
	if !ok {
		e = &[3]int64{}
		clusterStatsMap[cluster] = e
	}
	atomic.AddInt64(&e[1], 1)
	clusterStatsMu.Unlock()
}

// GetClusterStats returns a snapshot of per-cluster produce/fetch counts.
func GetClusterStats() []ClusterStats {
	clusterStatsMu.RLock()
	defer clusterStatsMu.RUnlock()
	out := make([]ClusterStats, 0, len(clusterStatsMap))
	for c, e := range clusterStatsMap {
		out = append(out, ClusterStats{
			Cluster: c,
			Produce: atomic.LoadInt64(&e[0]),
			Fetch:   atomic.LoadInt64(&e[1]),
			Records: atomic.LoadInt64(&e[2]),
		})
	}
	return out
}

// RecordClusterProduceBytes adds bytes to the produce counter and increments records.
func RecordClusterProduceBytes(cluster string, bytes int64) {
	if bytes <= 0 {
		return
	}
	clusterStatsMu.Lock()
	e, ok := clusterStatsMap[cluster]
	if !ok {
		e = &[3]int64{}
		clusterStatsMap[cluster] = e
	}
	atomic.AddInt64(&e[0], bytes)
	atomic.AddInt64(&e[2], 1) // count as a produce request
	clusterStatsMu.Unlock()
}

// RecordClusterFetchBytes adds bytes to the fetch counter for a target cluster.
func RecordClusterFetchBytes(cluster string, bytes int64) {
	if bytes <= 0 {
		return
	}
	clusterStatsMu.Lock()
	e, ok := clusterStatsMap[cluster]
	if !ok {
		e = &[3]int64{}
		clusterStatsMap[cluster] = e
	}
	atomic.AddInt64(&e[1], bytes)
	clusterStatsMu.Unlock()
}

// RoutedRequest holds the parsed metadata extracted from a Produce or Fetch
// Kafka request frame.
type RoutedRequest struct {
	APIKey      int16
	APIVersion  int16
	ClientID    string
	TopicName   string
	PartitionID int32
}

// ── Request Body Parsing ──────────────────────────────────────────────

// parseProduceBody extracts the first topic name and partition ID from a
// Produce request body (API Key 0). It skips past the acks/timeout/transactional
// prefix and returns the first topic+partition it finds, or an error.
//
// Wire format (after header):
//
//	[transactional_id: nullable string, v3+]  acks(int16)  timeout_ms(int32)
//	topic_array_len(int32)  [ topic_name(string)  partition_array_len(int32)
//	  [ partition_id(int32)  message_set_size(int32)  message_set(bytes) ]* ]*
func parseProduceBody(body []byte, version int16) (*RoutedRequest, error) {
	pos := 0

	// Transactional ID (nullable string) — only present in v3+
	if version >= 3 {
		txnLen := int16(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		if txnLen > 0 {
			pos += int(txnLen)
		}
	}

	// Acks (int16)
	if pos+2 > len(body) {
		return nil, fmt.Errorf("produce body truncated at acks")
	}
	pos += 2

	// Timeout (int32)
	if pos+4 > len(body) {
		return nil, fmt.Errorf("produce body truncated at timeout")
	}
	pos += 4

	// Topic array length
	if pos+4 > len(body) {
		return nil, fmt.Errorf("produce body truncated at topic array length")
	}
	topicCount := int32(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	if topicCount == 0 {
		return nil, fmt.Errorf("produce request has no topics")
	}

	// Read first topic
	tlen, topicName, newPos, err := readString(body, pos)
	if err != nil {
		return nil, fmt.Errorf("produce body: reading topic name: %w", err)
	}
	_ = tlen
	pos = newPos

	// Partition array length
	if pos+4 > len(body) {
		return nil, fmt.Errorf("produce body truncated at partition array length for topic %q", topicName)
	}
	partCount := int32(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	if partCount == 0 {
		return nil, fmt.Errorf("produce request: topic %q has no partitions", topicName)
	}

	// Read first partition
	if pos+4 > len(body) {
		return nil, fmt.Errorf("produce body truncated at partition id for topic %q", topicName)
	}
	partitionID := int32(binary.BigEndian.Uint32(body[pos:]))

	return &RoutedRequest{
		APIKey:      protocol.APIKeyProduce,
		TopicName:   topicName,
		PartitionID: partitionID,
	}, nil
}

// parseFetchBody extracts the first topic name and partition ID from a
// Fetch request body (API Key 1).
//
// Wire format (after header):
//
//	replica_id(int32)  max_wait_ms(int32)  min_bytes(int32)
//	[max_bytes(int32), v3+]  [isolation_level(int8), v4+]
//	[session_id(int32) session_epoch(int32), v7+]
//	topic_array_len(int32)
//	  [ topic_name(string)  partition_array_len(int32)
//	    [ partition_id(int32)  [current_leader_epoch(int32), v9+]
//	      fetch_offset(int64)  [log_start_offset(int64), v5+]
//	      partition_max_bytes(int32) ]* ]*
func parseFetchBody(body []byte, version int16) (*RoutedRequest, error) {
	pos := 0

	// Replica ID (int32)
	if pos+4 > len(body) {
		return nil, fmt.Errorf("fetch body truncated at replica_id")
	}
	pos += 4

	// MaxWait (int32)
	if pos+4 > len(body) {
		return nil, fmt.Errorf("fetch body truncated at max_wait")
	}
	pos += 4

	// MinBytes (int32)
	if pos+4 > len(body) {
		return nil, fmt.Errorf("fetch body truncated at min_bytes")
	}
	pos += 4

	// MaxBytes (int32) — v3+
	if version >= 3 {
		if pos+4 > len(body) {
			return nil, fmt.Errorf("fetch body truncated at max_bytes")
		}
		pos += 4
	}

	// IsolationLevel (int8) — v4+
	if version >= 4 {
		if pos+1 > len(body) {
			return nil, fmt.Errorf("fetch body truncated at isolation_level")
		}
		pos += 1
	}

	// SessionID + SessionEpoch (int32 each) — v7+
	if version >= 7 {
		if pos+8 > len(body) {
			return nil, fmt.Errorf("fetch body truncated at session fields")
		}
		pos += 8
	}

	// Topic array length
	if pos+4 > len(body) {
		return nil, fmt.Errorf("fetch body truncated at topic array length")
	}
	topicCount := int32(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	if topicCount == 0 {
		return nil, fmt.Errorf("fetch request has no topics")
	}

	// Read first topic name
	_, topicName, newPos, err := readString(body, pos)
	if err != nil {
		return nil, fmt.Errorf("fetch body: reading topic name: %w", err)
	}
	pos = newPos

	// Partition array length
	if pos+4 > len(body) {
		return nil, fmt.Errorf("fetch body truncated at partition array length for topic %q", topicName)
	}
	partCount := int32(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	if partCount == 0 {
		return nil, fmt.Errorf("fetch request: topic %q has no partitions", topicName)
	}

	// Read first partition
	if pos+4 > len(body) {
		return nil, fmt.Errorf("fetch body truncated at partition id for topic %q", topicName)
	}
	partitionID := int32(binary.BigEndian.Uint32(body[pos:]))

	return &RoutedRequest{
		APIKey:      protocol.APIKeyFetch,
		TopicName:   topicName,
		PartitionID: partitionID,
	}, nil
}

// ParseRequestBody detects the API key and parses the body to extract
// the first topic + partition. It is the main entry point for routing
// decision extraction from a Kafka request.
func ParseRequestBody(apiKey, apiVersion int16, body []byte) (*RoutedRequest, error) {
	switch apiKey {
	case protocol.APIKeyProduce:
		req, err := parseProduceBody(body, apiVersion)
		if err != nil {
			return nil, err
		}
		req.APIKey = apiKey
		req.APIVersion = apiVersion
		return req, nil
	case protocol.APIKeyFetch:
		req, err := parseFetchBody(body, apiVersion)
		if err != nil {
			return nil, err
		}
		req.APIKey = apiKey
		req.APIVersion = apiVersion
		return req, nil
	default:
		return nil, fmt.Errorf("unsupported API key %d for partition-aware routing", apiKey)
	}
}

// ── Router ────────────────────────────────────────────────────────────

// Router handles Produce and Fetch request routing with partition awareness.
// For requests that are not Produce or Fetch, it falls through to standard
// passthrough to the cluster bootstrap.
type Router struct {
	cfg              *config.Config
	cache            PartitionLeaderCache
	pool             *pool.ConnectionPool
	effectiveWeights map[string]clusterWeights // cluster BU -> {primary, secondary} weights
	weightsMu        sync.RWMutex
}

// clusterWeights holds the effective weight for each sub-cluster in load_balance
// mode. These may differ from the static config during auto-rebalance.
type clusterWeights struct {
	Primary   int
	Secondary int
}

// NewRouter creates a new Router. It initialises effective weights from the
// static config; these can later be overridden during auto-rebalance via
// SetEffectiveWeights.
func NewRouter(cfg *config.Config, cache PartitionLeaderCache) *Router {
	r := &Router{
		cfg:              cfg,
		cache:            cache,
		pool:             pool.New(cfg.Proxy.ConnectionPool, nil), // nil tlsCfg = plain TCP for local dev
		effectiveWeights: make(map[string]clusterWeights),
	}

	// Seed effective weights from static config for load_balance clusters.
	for name, cc := range cfg.Clusters {
		if cc.Mode == config.ModeLoadBalance {
			r.effectiveWeights[name] = clusterWeights{
				Primary:   cc.Primary.Weight,
				Secondary: cc.Secondary.Weight,
			}
		}
	}

	return r
}

// SetEffectiveWeights overrides the effective weight for a load_balance cluster.
// This is called by the auto-rebalance subsystem to temporarily shift traffic.
// primaryW + secondaryW should sum to 100.
func (r *Router) SetEffectiveWeights(cluster string, primaryW, secondaryW int) {
	r.weightsMu.Lock()
	defer r.weightsMu.Unlock()
	r.effectiveWeights[cluster] = clusterWeights{
		Primary:   primaryW,
		Secondary: secondaryW,
	}
}

// GetEffectiveWeights returns the current effective weights for a cluster.
// If no override has been set, falls back to the static config values.
func (r *Router) GetEffectiveWeights(cluster string, cfg config.ClusterConfig) (primaryW, secondaryW int) {
	r.weightsMu.RLock()
	defer r.weightsMu.RUnlock()
	if ew, ok := r.effectiveWeights[cluster]; ok {
		return ew.Primary, ew.Secondary
	}
	// Fall back to config weights (for active_passive, return default).
	if cfg.Mode == config.ModeLoadBalance {
		return cfg.Primary.Weight, cfg.Secondary.Weight
	}
	return 0, 0
}

// Route handles a client connection with protocol-aware routing.
// It reads the first Kafka request frame from the client, parses the header
// and body, determines the appropriate upstream broker (partition leader for
// Produce/Fetch, cluster bootstrap for everything else), and forwards the
// request + response bidirectionally.
//
// The baseLogger should already carry bu (business unit/cluster name).
// This method enriches it with correlation_id and client_id from the Kafka
// request header.
//
// Returns an error if routing or forwarding fails.
func (r *Router) Route(baseLogger *logger.Logger, clientConn net.Conn, clusterName string, clusterCfg config.ClusterConfig) error {
	defer clientConn.Close()

	// ── Step 1: Read the request frame ────────────────────────────────

	// Read the size prefix (4 bytes, big-endian int32)
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, sizeBuf); err != nil {
		return fmt.Errorf("reading frame size: %w", err)
	}
	frameSize := int32(binary.BigEndian.Uint32(sizeBuf))

	if frameSize <= 0 {
		return fmt.Errorf("invalid frame size %d", frameSize)
	}

	// Read the rest of the frame (header + body)
	frameData := make([]byte, frameSize)
	if _, err := io.ReadFull(clientConn, frameData); err != nil {
		return fmt.Errorf("reading frame data (%d bytes): %w", frameSize, err)
	}

	// ── Step 2: Parse the request header ──────────────────────────────

	// Reconstruct the full frame (size + data) for ParseRequestHeader
	// which expects the size prefix at offset 0.
	fullFrame := make([]byte, 4+frameSize)
	binary.BigEndian.PutUint32(fullFrame[0:4], uint32(frameSize))
	copy(fullFrame[4:], frameData)

	header, err := protocol.ParseRequestHeader(fullFrame)
	if err != nil {
		return fmt.Errorf("parsing request header: %w", err)
	}

	// Enrich the logger with Kafka request context fields.
	routingLog := baseLogger.WithCorrelationID(header.CorrelationID)
	if header.ClientID != "" {
		routingLog = routingLog.WithClientID(header.ClientID)
	}

	routingLog.Info("routing request",
		"api_key", header.APIKey, "api_key_name", apiKeyName(header.APIKey),
		"api_version", header.APIVersion)

	// ── Step 3: Determine the upstream address ────────────────────────

	var upstreamAddr string

	switch header.APIKey {
	case protocol.APIKeyProduce, protocol.APIKeyFetch:
		// Parse the body to extract topic + partition
		bodyStart := 14 + len(header.ClientID)
		body := fullFrame[bodyStart:]

		routed, err := ParseRequestBody(header.APIKey, header.APIVersion, body)
		if err != nil {
			return fmt.Errorf("parsing request body for API key %d: %w", header.APIKey, err)
		}
		routed.ClientID = header.ClientID

		routingLog.Info("routing Produce/Fetch", "topic", routed.TopicName, "partition", routed.PartitionID)

		// Track per-topic produce/fetch for dashboard metrics.
		if header.APIKey == protocol.APIKeyProduce {
			RecordTopicProduce(routed.TopicName)
		} else if header.APIKey == protocol.APIKeyFetch {
			RecordTopicFetch(routed.TopicName)
		}

		// Determine the target cluster (bootstrap) based on mode
		var cacheLookupKey string

		switch clusterCfg.Mode {
		case config.ModeActivePassive, config.ModeSingle:
			// Active/Passive: route to the configured active cluster.
			targetBootstrap := resolveClusterBootstrap(clusterCfg)
			if targetBootstrap == "" {
				return fmt.Errorf("no bootstrap address for cluster %q (mode=%s)", clusterName, clusterCfg.Mode)
			}
			cacheLookupKey = clusterName

			// Look up leader broker from the partition leader cache
			leaderAddr, found := r.cache.GetLeader(cacheLookupKey, routed.TopicName, routed.PartitionID)
			if !found {
				routingLog.Warn("leader unknown, triggering cache refresh",
					"topic", routed.TopicName, "partition", routed.PartitionID)
				if err := r.cache.RefreshMetadata(clusterName); err != nil {
					routingLog.Error("cache refresh failed", "error", err)
				}
				upstreamAddr = targetBootstrap
			} else {
				upstreamAddr = leaderAddr
				routingLog.Info("leader resolved", "leader_addr", upstreamAddr,
					"topic", routed.TopicName, "partition", routed.PartitionID)
			}

		case config.ModeLoadBalance:
			// Load Balance: use sticky hash + effective weight to pick a sub-cluster.
			primW, secW := r.GetEffectiveWeights(clusterName, clusterCfg)
			subCluster, targetBootstrap := selectClusterByWeight(
				clusterCfg, primW, secW,
				routed.TopicName, routed.PartitionID)

			cacheLookupKey = subClusterCacheKey(clusterName, subCluster)

			routingLog.Info("load_balance sticky hash",
				"sub_cluster", subCluster, "primary_weight", primW, "secondary_weight", secW)

			// Look up leader broker from partition leader cache per chosen sub-cluster.
			leaderAddr, found := r.cache.GetLeader(cacheLookupKey, routed.TopicName, routed.PartitionID)
			if !found {
				routingLog.Warn("leader unknown, triggering cache refresh",
					"cache_key", cacheLookupKey, "topic", routed.TopicName, "partition", routed.PartitionID)
				if err := r.cache.RefreshMetadata(clusterName); err != nil {
					routingLog.Error("cache refresh failed", "error", err)
				}
				upstreamAddr = targetBootstrap
			} else {
				upstreamAddr = leaderAddr
				routingLog.Info("leader resolved", "leader_addr", upstreamAddr,
					"cache_key", cacheLookupKey, "topic", routed.TopicName, "partition", routed.PartitionID)
			}

		default:
			return fmt.Errorf("unknown cluster mode %q for cluster %q", clusterCfg.Mode, clusterName)
		}

	default:
		// Passthrough: route to the cluster bootstrap.
		upstreamAddr = resolveClusterBootstrap(clusterCfg)
		if upstreamAddr == "" {
			return fmt.Errorf("no bootstrap address for cluster %q (mode=%s)", clusterName, clusterCfg.Mode)
		}
	}

	// ── Step 4: Get a pooled connection to the upstream broker ─────────

	upstreamConn, err := r.pool.Get(upstreamAddr)
	if err != nil {
		return fmt.Errorf("connecting to upstream %s: %w", upstreamAddr, err)
	}
	defer r.pool.Put(upstreamAddr, upstreamConn)

	routingLog.Info("connected to upstream", "upstream_addr", upstreamAddr)

	// ── Step 5: Forward the request payload ───────────────────────────

	// Write the complete frame to the upstream
	if _, err := upstreamConn.Write(fullFrame); err != nil {
		return fmt.Errorf("forwarding request to upstream %s: %w", upstreamAddr, err)
	}

	// ── Step 6: Stream the response back ──────────────────────────────

	// Read the response header (size prefix + correlation id) to validate
	respSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(upstreamConn, respSizeBuf); err != nil {
		return fmt.Errorf("reading response size from upstream %s: %w", upstreamAddr, err)
	}

	respSize := int32(binary.BigEndian.Uint32(respSizeBuf))
	if respSize < 4 {
		return fmt.Errorf("invalid response size %d from upstream %s", respSize, upstreamAddr)
	}

	// Write the response size back to the client
	if _, err := clientConn.Write(respSizeBuf); err != nil {
		return fmt.Errorf("forwarding response size to client: %w", err)
	}

	// Stream the rest of the response (correlation id + body) from upstream to client
	written, err := io.CopyN(clientConn, upstreamConn, int64(respSize))
	if err != nil {
		return fmt.Errorf("streaming response from upstream %s: %w (wrote %d/%d bytes)",
			upstreamAddr, err, written, respSize)
	}

	routingLog.Info("response forwarded", "bytes", respSize+4, "api_key_name", apiKeyName(header.APIKey))

	// Record successful routing for rebalancing.

	// ── Step 7: Continue with bidirectional passthrough ───────────────
	// After the first routed request+response, switch to blind passthrough
	// for the remainder of the connection (handles pipelined requests and
	// non-Produce/Fetch traffic on the same connection).

	done := make(chan struct{}, 2)

	go func() {
		n, err := io.Copy(upstreamConn, clientConn)
		if err != nil && err != io.EOF {
			routingLog.Error("client→upstream copy error", "error", err, "bytes", n)
		}
		done <- struct{}{}
	}()

	go func() {
		n, err := io.Copy(clientConn, upstreamConn)
		if err != nil && err != io.EOF {
			routingLog.Error("upstream→client copy error", "error", err, "bytes", n)
		}
		done <- struct{}{}
	}()

	<-done
	upstreamConn.Close()
	clientConn.Close()

	return nil
}


// ── Helpers ───────────────────────────────────────────────────────────

// resolveClusterBootstrap returns the upstream bootstrap address for a
// cluster based on its mode and active setting.
//
// - active_passive: routes to the configured active cluster (primary or
//   secondary).
// - load_balance (stub): routes to primary only for now.
func resolveClusterBootstrap(cfg config.ClusterConfig) string {
	switch cfg.Mode {
	case config.ModeActivePassive:
		switch cfg.Active {
		case config.ActivePrimary:
			return cfg.Primary.Bootstrap
		case config.ActiveSecondary:
			return cfg.Secondary.Bootstrap
		default:
			return ""
		}
	case config.ModeLoadBalance:
		// Default passthrough: route non-Produce/Fetch traffic to primary.
		return cfg.Primary.Bootstrap
	case config.ModeSingle:
		return cfg.Primary.Bootstrap
	default:
		return ""
	}
}

// readString reads a Kafka string from the byte slice at the given offset.
// Kafka strings are prefixed by a 2-byte big-endian int16 length.
// Returns the length, the string value, the new offset after the string,
// and any error.
func readString(data []byte, offset int) (int16, string, int, error) {
	if offset+2 > len(data) {
		return 0, "", offset, fmt.Errorf("string truncated at position %d (need 2 bytes for length)", offset)
	}
	length := int16(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if length < 0 {
		return length, "", offset, nil // null string
	}
	end := offset + int(length)
	if end > len(data) {
		return 0, "", offset, fmt.Errorf("string truncated: length=%d but only %d bytes left", length, len(data)-offset)
	}
	return length, string(data[offset:end]), end, nil
}

// apiKeyName returns a human-readable name for a Kafka API key.
func apiKeyName(key int16) string {
	switch key {
	case protocol.APIKeyProduce:
		return "Produce"
	case protocol.APIKeyFetch:
		return "Fetch"
	case protocol.APIKeyMetadata:
		return "Metadata"
	default:
		return fmt.Sprintf("Unknown(%d)", key)
	}
}

// Ensure Router satisfies a useful interface pattern.
var _ interface {
	Route(log *logger.Logger, clientConn net.Conn, clusterName string, clusterCfg config.ClusterConfig) error
} = (*Router)(nil)


