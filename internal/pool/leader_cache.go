// Package pool provides the partition leader cache for routing Kafka
// produce/fetch requests to the correct broker in Confluent Cloud clusters.
//
// The PartitionLeaderCache maintains a map of (clusterBootstrap, topic, partition)
// → leader broker address, refreshed every 30s via background Metadata requests.
package pool

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/logger"
)

// DefaultRefreshInterval is the interval between automated metadata refreshes.
const DefaultRefreshInterval = 30 * time.Second

// DefaultMetadataTimeout is the dial + read timeout for Metadata requests.
const DefaultMetadataTimeout = 10 * time.Second

// MaxMetadataResponseSize caps the Metadata response body to prevent OOM.
const MaxMetadataResponseSize = 10 << 20 // 10 MiB

// KafkaMetadata is the API key for MetadataRequest / MetadataResponse.
const KafkaMetadata = 3

// PartitionLeaderCache maps cluster bootstrap addresses to per-topic,
// per-partition leader broker addresses. It supports concurrent reads,
// background refresh, and on-demand immediate refresh.
//
// Map structure:
//
//	leaders[clusterBootstrap][topic][partition] → brokerAddr (host:port)
type PartitionLeaderCache struct {
	mu      sync.RWMutex
	leaders map[string]map[string]map[int32]string

	refreshInterval time.Duration
	dialTimeout     time.Duration
	readTimeout     time.Duration

	refreshers map[string]*clusterRefresher
	refMu      sync.Mutex

	// TLS config for connecting to Confluent Cloud brokers.
	// If nil, plain TCP is used (suitable for non-TLS Kafka brokers).
	tlsConfig *tls.Config
}

// clusterRefresher manages the background refresh goroutine for one cluster.
type clusterRefresher struct {
	bootstrap string
	trigger   chan struct{} // buffered (1) — immediate refresh signal
	stop      chan struct{} // closed to terminate the goroutine
}

// NewPartitionLeaderCache creates a new PartitionLeaderCache with
// default refresh interval (30s) and timeouts (10s).
func NewPartitionLeaderCache() *PartitionLeaderCache {
	return &PartitionLeaderCache{
		leaders:         make(map[string]map[string]map[int32]string),
		refreshInterval: DefaultRefreshInterval,
		dialTimeout:     DefaultMetadataTimeout,
		readTimeout:     DefaultMetadataTimeout,
		refreshers:      make(map[string]*clusterRefresher),
	}
}

// NewPartitionLeaderCacheWithTLS creates a cache that connects to Kafka
// brokers using TLS (required for Confluent Cloud SASL_SSL endpoints).
// If tlsCfg is nil, uses default system root CAs with TLS 1.2 minimum.
func NewPartitionLeaderCacheWithTLS(tlsCfg *tls.Config) *PartitionLeaderCache {
	c := NewPartitionLeaderCache()
	if tlsCfg == nil {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	c.tlsConfig = tlsCfg
	return c
}

// GetLeader returns the broker address for a given (cluster, topic, partition).
// Returns ("", false) when no leader is cached for that key.
func (c *PartitionLeaderCache) GetLeader(cluster, topic string, partition int32) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getLeaderLocked(cluster, topic, partition)
}

// getLeaderLocked is the internal read-locked lookup.
func (c *PartitionLeaderCache) getLeaderLocked(cluster, topic string, partition int32) (string, bool) {
	topics, ok := c.leaders[cluster]
	if !ok {
		return "", false
	}
	partitions, ok := topics[topic]
	if !ok {
		return "", false
	}
	addr, ok := partitions[partition]
	return addr, ok
}

// SetLeader records a single leader broker address for a given
// (cluster, topic, partition) key. If the topic or cluster entry does not
// exist yet it is created automatically.
func (c *PartitionLeaderCache) SetLeader(cluster, topic string, partition int32, brokerAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	topics, ok := c.leaders[cluster]
	if !ok {
		topics = make(map[string]map[int32]string)
		c.leaders[cluster] = topics
	}

	partitions, ok := topics[topic]
	if !ok {
		partitions = make(map[int32]string)
		topics[topic] = partitions
	}

	partitions[partition] = brokerAddr
}

// Invalidate removes all cached leader entries for a given cluster.
// Safe to call when the cluster has no entries (no-op).
func (c *PartitionLeaderCache) Invalidate(cluster string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.leaders, cluster)
}

// RefreshMetadata triggers a metadata refresh for the given cluster.
// It is an alias for Refresh and implements the routing.PartitionLeaderCache
// interface.
func (c *PartitionLeaderCache) RefreshMetadata(cluster string) error {
	return c.Refresh(cluster)
}

// Refresh sends a MetadataRequest to the given cluster bootstrap, parses the
// response, and atomically swaps the leader map for that cluster under a
// write lock.  Existing reads are not blocked.
//
// On success the cluster entry in the cache is replaced entirely.
// On error the existing cache for that cluster is preserved (no partial update).
func (c *PartitionLeaderCache) Refresh(clusterBootstrap string) error {
	leaders, err := c.fetchMetadata(clusterBootstrap)
	if err != nil {
		return fmt.Errorf("refresh %s: %w", clusterBootstrap, err)
	}

	c.mu.Lock()
	c.leaders[clusterBootstrap] = leaders
	c.mu.Unlock()

	return nil
}

// StartBackgroundRefresh begins a background goroutine that calls Refresh for
// the given cluster every refreshInterval (default 30s).  The initial refresh
// runs immediately.
//
// Idempotent: calling StartBackgroundRefresh for an already-active cluster is
// a no-op.
func (c *PartitionLeaderCache) StartBackgroundRefresh(clusterBootstrap string) {
	c.refMu.Lock()
	defer c.refMu.Unlock()

	if _, exists := c.refreshers[clusterBootstrap]; exists {
		return // already refreshing
	}

	r := &clusterRefresher{
		bootstrap: clusterBootstrap,
		trigger:   make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
	c.refreshers[clusterBootstrap] = r

	go c.refreshLoop(r)
}

// StopBackgroundRefresh stops the background goroutine for a cluster.
// It is a no-op if the cluster was never started or already stopped.
func (c *PartitionLeaderCache) StopBackgroundRefresh(clusterBootstrap string) {
	c.refMu.Lock()
	defer c.refMu.Unlock()

	r, exists := c.refreshers[clusterBootstrap]
	if !exists {
		return
	}
	close(r.stop)
	delete(c.refreshers, clusterBootstrap)
}

// StopAllBackgroundRefreshes stops all background refresh goroutines.
func (c *PartitionLeaderCache) StopAllBackgroundRefreshes() {
	c.refMu.Lock()
	defer c.refMu.Unlock()

	for bootstrap, r := range c.refreshers {
		close(r.stop)
		delete(c.refreshers, bootstrap)
	}
}

// TriggerRefresh signals an immediate refresh for the given cluster.
// Non-blocking: if a trigger is already pending, the call is silently dropped.
//
// Use this on config reload or failover to get fresh topology without
// waiting for the next periodic tick.
func (c *PartitionLeaderCache) TriggerRefresh(clusterBootstrap string) {
	c.refMu.Lock()
	r, exists := c.refreshers[clusterBootstrap]
	c.refMu.Unlock()

	if !exists {
		return
	}

	select {
	case r.trigger <- struct{}{}:
	default:
		// trigger already pending
	}
}

// ActiveClusters returns the set of cluster bootstraps currently being
// refreshed in the background.
func (c *PartitionLeaderCache) ActiveClusters() []string {
	c.refMu.Lock()
	defer c.refMu.Unlock()

	clusters := make([]string, 0, len(c.refreshers))
	for b := range c.refreshers {
		clusters = append(clusters, b)
	}
	return clusters
}

// ── Background refresh loop ───────────────────────────────────────────

func (c *PartitionLeaderCache) refreshLoop(r *clusterRefresher) {
	// Initial refresh
	if err := c.Refresh(r.bootstrap); err != nil {
		logger.Default().Error("leader_cache: initial metadata refresh failed",
			"cluster", r.bootstrap, "error", err)
	}

	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.Refresh(r.bootstrap); err != nil {
				logger.Default().Error("leader_cache: periodic metadata refresh failed",
					"cluster", r.bootstrap, "error", err)
			}

		case <-r.trigger:
			if err := c.Refresh(r.bootstrap); err != nil {
				logger.Default().Error("leader_cache: triggered metadata refresh failed",
					"cluster", r.bootstrap, "error", err)
			}

		case <-r.stop:
			return
		}
	}
}

// ── Kafka Metadata wire protocol ──────────────────────────────────────

// fetchMetadata opens a connection to the bootstrap server, sends a
// MetadataRequest (v0), reads the response, and builds the leader map.
func (c *PartitionLeaderCache) fetchMetadata(bootstrap string) (map[string]map[int32]string, error) {
	conn, err := c.dial(bootstrap)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// SASL authentication before Metadata request.
	if err := poolSASLAuth(conn); err != nil {
		return nil, fmt.Errorf("sasl auth: %w", err)
	}

	if err := conn.SetDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return nil, fmt.Errorf("set deadline: %w", err)
	}

	// Send MetadataRequest (API key 3, version 0)
	req := encodeMetadataRequestV0()
	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	// Read response
	resp, err := readKafkaResponse(conn, MaxMetadataResponseSize)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return parseMetadataResponseV0(resp)
}

// dial creates a connection to the bootstrap server. Uses TLS when configured.
func (c *PartitionLeaderCache) dial(bootstrap string) (net.Conn, error) {
	if c.tlsConfig != nil {
		dialer := &net.Dialer{Timeout: c.dialTimeout}
		return tls.DialWithDialer(dialer, "tcp", bootstrap, c.tlsConfig)
	}
	return net.DialTimeout("tcp", bootstrap, c.dialTimeout)
}

// encodeMetadataRequestV0 builds a Kafka Metadata request frame (API key 3,
// version 0) requesting metadata for all topics (null topics array).
//
// Wire format:
//
//	Offset  Bytes  Field
//	  0       4    Size              (int32) — excludes this field
//	  4       2    API Key           (int16) = 3
//	  6       2    API Version       (int16) = 0
//	  8       4    Correlation ID    (int32)
//	 12       2    Client ID Len     (int16) — string length or -1
//	 14       N    Client ID         (string)
//	 14+N     4    Topics Array Len  (int32) — -1 for null = all topics
func encodeMetadataRequestV0() []byte {
	const clientID = "kafkaproxy-metadata"

	// Total size (excluding the 4-byte size prefix):
	// apiKey(2) + apiVersion(2) + correlationID(4) + clientID(2+len) + topics(4)
	totalSize := 2 + 2 + 4 + 2 + len(clientID) + 4

	buf := make([]byte, 4+totalSize)

	// Size prefix
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalSize))

	// API key
	binary.BigEndian.PutUint16(buf[4:6], KafkaMetadata)

	// API version
	binary.BigEndian.PutUint16(buf[6:8], 0)

	// Correlation ID
	binary.BigEndian.PutUint32(buf[8:12], 1)

	// Client ID (non-null string)
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:14+len(clientID)], clientID)

	// Topics array: null (-1) = request all topics
	topicsOffset := 14 + len(clientID)
	binary.BigEndian.PutUint32(buf[topicsOffset:topicsOffset+4], 0xFFFFFFFF) // -1 as uint32

	return buf
}

// readKafkaResponse reads a complete Kafka response frame from the connection.
// Returns the response body (after the 4-byte size prefix).
func readKafkaResponse(conn net.Conn, maxSize int) ([]byte, error) {
	// Read the 4-byte size prefix
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return nil, fmt.Errorf("read size prefix: %w", err)
	}
	bodySize := int(binary.BigEndian.Uint32(sizeBuf[:]))

	if bodySize < 0 || bodySize > maxSize {
		return nil, fmt.Errorf("response body size %d out of range [0, %d]", bodySize, maxSize)
	}

	body := make([]byte, bodySize)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return body, nil
}

// parseMetadataResponseV0 parses a Kafka Metadata response (API key 3,
// version 0) body and returns a topic→partition→leaderAddr map.
//
// Response body layout (v0):
//
//	  0       4    Correlation ID       (int32)
//	  4       4    Broker Count         (int32)
//	  8       *    Brokers array
//	  *       4    Topic Count          (int32)
//	  *       *    Topics array          (each has partitions array)
//
// Broker array entry:
//
//	  0       4    Node ID              (int32)
//	  4       2    Host Length          (int16)
//	  6       N    Host                 (string)
//	6+N       4    Port                 (int32)
//
// Topic array entry:
//
//	  0       2    Error Code           (int16)
//	  2       2    Name Length          (int16)
//	  4       N    Name                 (string)
//	4+N       4    Partition Count      (int32)
//	8+N       *    Partitions array
//
// Partition array entry:
//
//	  0       2    Error Code           (int16)
//	  2       4    Partition Index      (int32)
//	  6       4    Leader ID            (int32)
//	 10       4    Replica Count        (int32)
//	 14       *    Replicas             (int32 each)
//	  *       4    ISR Count            (int32)
//	  *       *    ISR                   (int32 each)
func parseMetadataResponseV0(body []byte) (map[string]map[int32]string, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("response body too short: %d bytes", len(body))
	}

	pos := 0

	// Correlation ID
	_ = int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4

	// ── Brokers: build nodeID → addr map ──
	if pos+4 > len(body) {
		return nil, fmt.Errorf("truncated at broker count")
	}
	brokerCount := int(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4

	brokerMap := make(map[int32]string, brokerCount)
	for i := 0; i < brokerCount; i++ {
		if pos+10 > len(body) {
			return nil, fmt.Errorf("truncated at broker %d", i)
		}
		nodeID := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4

		hostLen := int(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2

		if pos+hostLen > len(body) {
			return nil, fmt.Errorf("truncated at broker %d host", i)
		}
		host := string(body[pos : pos+hostLen])
		pos += hostLen

		if pos+4 > len(body) {
			return nil, fmt.Errorf("truncated at broker %d port", i)
		}
		port := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4

		brokerMap[nodeID] = fmt.Sprintf("%s:%d", host, port)
	}

	// ── Topics: extract partition leaders ──
	if pos+4 > len(body) {
		return nil, fmt.Errorf("truncated at topic count")
	}
	topicCount := int(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4

	leaders := make(map[string]map[int32]string, topicCount)

	for i := 0; i < topicCount; i++ {
		// Error code
		if pos+2 > len(body) {
			return nil, fmt.Errorf("truncated at topic %d error code", i)
		}
		topicErr := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2

		// Topic name
		if pos+2 > len(body) {
			return nil, fmt.Errorf("truncated at topic %d name length", i)
		}
		nameLen := int(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2

		if pos+nameLen > len(body) {
			return nil, fmt.Errorf("truncated at topic %d name", i)
		}
		topicName := string(body[pos : pos+nameLen])
		pos += nameLen

		// Partition count
		if pos+4 > len(body) {
			return nil, fmt.Errorf("truncated at topic %q partition count", topicName)
		}
		partCount := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4

		// Skip topics with errors (e.g. UNKNOWN_TOPIC_OR_PARTITION)
		if topicErr != 0 {
			// Skip partitions: each is at least 20 bytes
			skipBytes := partCount * 20 // rough estimate, will skip properly below
			_ = skipBytes
			for j := 0; j < partCount; j++ {
				adv, err := skipPartitionV0(body, pos)
				if err != nil {
					return nil, fmt.Errorf("skipping partition %d of topic %q: %w", j, topicName, err)
				}
				pos += adv
			}
			continue
		}

		partitions := make(map[int32]string, partCount)

		for j := 0; j < partCount; j++ {
			partIdx, leaderAddr, adv, err := parsePartitionV0(body, pos, brokerMap)
			if err != nil {
				return nil, fmt.Errorf("parsing partition %d of topic %q: %w", j, topicName, err)
			}
			pos += adv
			if leaderAddr != "" {
				partitions[partIdx] = leaderAddr
			}
		}

		leaders[topicName] = partitions
	}

	return leaders, nil
}

// parsePartitionV0 parses one partition entry from a MetadataResponse v0,
// returns the leader broker address (host:port) and the number of bytes
// consumed. Returns ("", ...) when the partition has an error.
//
// Partition wire layout (v0):
//
//	 0       2    Error Code           (int16)
//	 2       4    Partition Index      (int32)
//	 6       4    Leader ID            (int32)
//	10       4    Replica Count        (int32)
//	14     N*4    Replicas             (int32 each)
//	 *       4    ISR Count            (int32)
//	 *     M*4    ISR                   (int32 each)
func parsePartitionV0(body []byte, start int, brokerMap map[int32]string) (partIdx int32, leaderAddr string, consumed int, err error) {
	pos := start

	if pos+2 > len(body) {
		return 0, "", 0, fmt.Errorf("truncated at error code")
	}
	partErr := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2

	if pos+4 > len(body) {
		return 0, "", 0, fmt.Errorf("truncated at partition index")
	}
	partIdx = int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4

	if pos+4 > len(body) {
		return 0, "", 0, fmt.Errorf("truncated at leader id")
	}
	leaderID := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4

	// Replica count
	if pos+4 > len(body) {
		return 0, "", 0, fmt.Errorf("truncated at replica count")
	}
	replicaCount := int(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4

	// Skip replica IDs
	replicasBytes := replicaCount * 4
	if pos+replicasBytes > len(body) {
		return 0, "", 0, fmt.Errorf("truncated at replica list")
	}
	pos += replicasBytes

	// ISR count
	if pos+4 > len(body) {
		return 0, "", 0, fmt.Errorf("truncated at isr count")
	}
	isrCount := int(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4

	// Skip ISR IDs
	isrBytes := isrCount * 4
	if pos+isrBytes > len(body) {
		return 0, "", 0, fmt.Errorf("truncated at isr list")
	}
	pos += isrBytes

	consumed = pos - start

	if partErr != 0 {
		return partIdx, "", consumed, nil // partition has error, no leader
	}

	addr, ok := brokerMap[leaderID]
	if !ok {
		return partIdx, "", consumed, fmt.Errorf("partition %d references unknown leader broker ID %d", partIdx, leaderID)
	}

	return partIdx, addr, consumed, nil
}

// skipPartitionV0 advances past a partition entry without parsing it.
func skipPartitionV0(body []byte, start int) (consumed int, err error) {
	pos := start

	// Error code (2) + Partition Index (4) + Leader ID (4) = 10
	if pos+10 > len(body) {
		return 0, fmt.Errorf("truncated at skip partition start")
	}
	pos += 10

	// Replica count
	if pos+4 > len(body) {
		return 0, fmt.Errorf("truncated at skip replica count")
	}
	replicaCount := int(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4 + replicaCount*4

	// ISR count
	if pos+4 > len(body) {
		return 0, fmt.Errorf("truncated at skip isr count")
	}
	isrCount := int(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4 + isrCount*4

	return pos - start, nil
}

// poolSASLAuth performs SASL/PLAIN authentication for the leader cache's
// metadata refresh connections. Currently skipped — SASL credentials should
// come from cluster health_check config. For plaintext Kafka clusters this
// is a no-op.
func poolSASLAuth(conn net.Conn) error {
	// TODO: accept SASL credentials from config when leader cache refresh
	// needs to authenticate against SASL-enabled clusters.
	return nil
}
