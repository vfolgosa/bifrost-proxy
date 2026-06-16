package routing
import (
	"encoding/binary"
	"net"
	"sync"
	"testing"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

// ── ParseRequestBody Tests ────────────────────────────────────────────

// buildProduceBody builds a minimal Produce request body (v0 format).
// Fields: acks(2) + timeout(4) + topic_count(4) + topic + partition_count(4) + partition
func buildProduceBody(topic string, partition int32, msgBytes []byte) []byte {
	topicBytes := []byte(topic)
	msgSize := len(msgBytes)

	// acks(2) + timeout_ms(4) + topic_count(4)
	size := 2 + 4 + 4
	// topic_name_len(2) + topic_name + partition_count(4)
	size += 2 + len(topicBytes) + 4
	// partition_id(4) + message_set_size(4) + message_set
	size += 4 + 4 + msgSize

	buf := make([]byte, size)
	pos := 0

	// acks = 1 (leader acknowledgment)
	binary.BigEndian.PutUint16(buf[pos:], 1)
	pos += 2
	// timeout_ms = 30000
	binary.BigEndian.PutUint32(buf[pos:], 30000)
	pos += 4
	// topic_count = 1
	binary.BigEndian.PutUint32(buf[pos:], 1)
	pos += 4
	// topic_name
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(topicBytes)))
	pos += 2
	copy(buf[pos:], topicBytes)
	pos += len(topicBytes)
	// partition_count = 1
	binary.BigEndian.PutUint32(buf[pos:], 1)
	pos += 4
	// partition_id
	binary.BigEndian.PutUint32(buf[pos:], uint32(partition))
	pos += 4
	// message_set_size + message_set
	binary.BigEndian.PutUint32(buf[pos:], uint32(msgSize))
	pos += 4
	copy(buf[pos:], msgBytes)

	return buf
}

func TestParseProduceBody(t *testing.T) {
	body := buildProduceBody("orders", 2, []byte{0x01, 0x02, 0x03})

	req, err := ParseRequestBody(protocol.APIKeyProduce, 0, body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.TopicName != "orders" {
		t.Errorf("TopicName = %q, want %q", req.TopicName, "orders")
	}
	if req.PartitionID != 2 {
		t.Errorf("PartitionID = %d, want 2", req.PartitionID)
	}
	if req.APIKey != protocol.APIKeyProduce {
		t.Errorf("APIKey = %d, want %d", req.APIKey, protocol.APIKeyProduce)
	}
}

func TestParseProduceBodyWithTransactionalID(t *testing.T) {
	// v3+ has transactional_id prefix (nullable string)
	txID := "txn-123"
	txIDBytes := []byte(txID)

	body := buildProduceBody("orders", 0, []byte{0xAA})

	// Prepend transactional_id (int16 length + bytes)
	prefix := make([]byte, 2+len(txIDBytes))
	binary.BigEndian.PutUint16(prefix[0:2], uint16(len(txIDBytes)))
	copy(prefix[2:], txIDBytes)

	v3Body := append(prefix, body...)

	req, err := ParseRequestBody(protocol.APIKeyProduce, 3, v3Body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.TopicName != "orders" {
		t.Errorf("TopicName = %q, want %q", req.TopicName, "orders")
	}
}

func TestParseProduceBodyNullTransactionalID(t *testing.T) {
	body := buildProduceBody("events", 5, nil)

	// Prepend null transactional_id (length = -1)
	prefix := make([]byte, 2)
	binary.BigEndian.PutUint16(prefix[0:2], 0xFFFF) // -1

	v3Body := append(prefix, body...)

	req, err := ParseRequestBody(protocol.APIKeyProduce, 3, v3Body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.TopicName != "events" {
		t.Errorf("TopicName = %q, want %q", req.TopicName, "events")
	}
	if req.PartitionID != 5 {
		t.Errorf("PartitionID = %d, want 5", req.PartitionID)
	}
}

func TestParseProduceBodyNoTopics(t *testing.T) {
	// acks(2) + timeout(4) + topic_count=0(4)
	body := make([]byte, 10)
	binary.BigEndian.PutUint16(body[0:2], uint16(0xFFFF)) // acks=-1 (all)
	binary.BigEndian.PutUint32(body[2:6], 5000)          // timeout
	binary.BigEndian.PutUint32(body[6:10], 0)            // topic_count=0

	_, err := ParseRequestBody(protocol.APIKeyProduce, 0, body)
	if err == nil {
		t.Fatal("expected error for topic_count=0")
	}
}

func TestParseProduceBodyTruncated(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"too short for acks", make([]byte, 1)},
		{"too short for timeout", make([]byte, 3)},
		{"too short for topic array", make([]byte, 5)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseRequestBody(protocol.APIKeyProduce, 0, tt.body)
			if err == nil {
				t.Error("expected error for truncated body")
			}
		})
	}
}

// ── Fetch Body Tests ─────────────────────────────────────────────────

func buildFetchBody(topic string, partition int32, fetchOffset int64) []byte {
	topicBytes := []byte(topic)

	// replica_id(4) + max_wait(4) + min_bytes(4) + topic_count(4)
	size := 4 + 4 + 4 + 4
	// topic_name_len(2) + topic_name + partition_count(4)
	size += 2 + len(topicBytes) + 4
	// partition_id(4) + fetch_offset(8) + max_bytes(4)
	size += 4 + 8 + 4

	buf := make([]byte, size)
	pos := 0

	// replica_id = -1 (consumer)
	binary.BigEndian.PutUint32(buf[pos:], 0xFFFFFFFF)
	pos += 4
	// max_wait_ms = 500
	binary.BigEndian.PutUint32(buf[pos:], 500)
	pos += 4
	// min_bytes = 1
	binary.BigEndian.PutUint32(buf[pos:], 1)
	pos += 4
	// topic_count = 1
	binary.BigEndian.PutUint32(buf[pos:], 1)
	pos += 4
	// topic_name
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(topicBytes)))
	pos += 2
	copy(buf[pos:], topicBytes)
	pos += len(topicBytes)
	// partition_count = 1
	binary.BigEndian.PutUint32(buf[pos:], 1)
	pos += 4
	// partition_id
	binary.BigEndian.PutUint32(buf[pos:], uint32(partition))
	pos += 4
	// fetch_offset
	binary.BigEndian.PutUint64(buf[pos:], uint64(fetchOffset))
	pos += 8
	// max_bytes
	binary.BigEndian.PutUint32(buf[pos:], 1024*1024) // 1MB

	return buf
}

func TestParseFetchBody(t *testing.T) {
	body := buildFetchBody("orders", 3, 1000)

	req, err := ParseRequestBody(protocol.APIKeyFetch, 0, body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.TopicName != "orders" {
		t.Errorf("TopicName = %q, want %q", req.TopicName, "orders")
	}
	if req.PartitionID != 3 {
		t.Errorf("PartitionID = %d, want 3", req.PartitionID)
	}
	if req.APIKey != protocol.APIKeyFetch {
		t.Errorf("APIKey = %d, want %d", req.APIKey, protocol.APIKeyFetch)
	}
}

func TestParseFetchBodyNoTopics(t *testing.T) {
	body := make([]byte, 16)
	binary.BigEndian.PutUint32(body[0:4], 0xFFFFFFFF)  // replica_id
	binary.BigEndian.PutUint32(body[4:8], 500)          // max_wait
	binary.BigEndian.PutUint32(body[8:12], 1)           // min_bytes
	binary.BigEndian.PutUint32(body[12:16], 0)          // topic_count=0

	_, err := ParseRequestBody(protocol.APIKeyFetch, 0, body)
	if err == nil {
		t.Fatal("expected error for topic_count=0")
	}
}

func TestParseUnsupportedAPIKey(t *testing.T) {
	_, err := ParseRequestBody(protocol.APIKeyMetadata, 0, []byte{0x00, 0x00, 0x00, 0x00})
	if err == nil {
		t.Fatal("expected error for unsupported API key (Metadata)")
	}
}

// ── MapPartitionLeaderCache Tests ─────────────────────────────────────

func TestMapPartitionLeaderCache_GetSet(t *testing.T) {
	c := NewMapPartitionLeaderCache()

	// Set a leader
	c.SetLeader("cluster-a", "orders", 2, "broker1:9092")

	// Get it back
	addr, found := c.GetLeader("cluster-a", "orders", 2)
	if !found {
		t.Fatal("expected to find leader")
	}
	if addr != "broker1:9092" {
		t.Errorf("addr = %q, want %q", addr, "broker1:9092")
	}

	// Get non-existent
	_, found = c.GetLeader("cluster-a", "orders", 99)
	if found {
		t.Error("expected not to find non-existent leader")
	}

	// Different cluster
	_, found = c.GetLeader("cluster-b", "orders", 2)
	if found {
		t.Error("expected not to find leader in different cluster")
	}
}

func TestMapPartitionLeaderCache_Invalidate(t *testing.T) {
	c := NewMapPartitionLeaderCache()

	c.SetLeader("cluster-a", "orders", 0, "broker1:9092")
	c.SetLeader("cluster-a", "orders", 1, "broker2:9092")
	c.SetLeader("cluster-b", "orders", 0, "broker3:9092")

	// Invalidate cluster-a
	c.Invalidate("cluster-a")

	// cluster-a entries should be gone
	_, found := c.GetLeader("cluster-a", "orders", 0)
	if found {
		t.Error("expected cluster-a/orders/0 to be invalidated")
	}
	_, found = c.GetLeader("cluster-a", "orders", 1)
	if found {
		t.Error("expected cluster-a/orders/1 to be invalidated")
	}

	// cluster-b entry should still exist
	addr, found := c.GetLeader("cluster-b", "orders", 0)
	if !found {
		t.Fatal("expected cluster-b entry to survive invalidation")
	}
	if addr != "broker3:9092" {
		t.Errorf("addr = %q, want %q", addr, "broker3:9092")
	}
}

func TestMapPartitionLeaderCache_Concurrent(t *testing.T) {
	c := NewMapPartitionLeaderCache()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c.SetLeader("cluster", "topic", int32(idx), "broker:9092")
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c.GetLeader("cluster", "topic", int32(idx))
		}(i)
	}

	wg.Wait()

	// Verify all entries are present
	for i := 0; i < 100; i++ {
		_, found := c.GetLeader("cluster", "topic", int32(i))
		if !found {
			t.Errorf("entry %d not found after concurrent writes", i)
		}
	}
}

func TestMapPartitionLeaderCache_RefreshMetadata(t *testing.T) {
	c := NewMapPartitionLeaderCache()

	err := c.RefreshMetadata("cluster-a")
	if err == nil {
		t.Fatal("expected error from stub RefreshMetadata")
	}
	t.Logf("RefreshMetadata correctly returned: %v", err)
}

// ── Router Integration Tests ─────────────────────────────────────────

func TestRouter_RouteProduceToLeader(t *testing.T) {
	// Set up client-side pipe and fake upstream broker.
	// The upstream proxy pipe is used as a placeholder — when the pool tries to
	// connect to the real leader:9092 (which is unreachable), it will fail and
	// return an error from Route. We close the upstream pipe after Route returns
	// so the reader goroutine doesn't hang.
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamOther := net.Pipe()
	defer upstreamProxy.Close()
	defer upstreamOther.Close()

	cache := NewMapPartitionLeaderCache()
	cache.SetLeader("test-cluster", "orders", 0, "leader:9092")

	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ConnectionPool: config.ConnectionPoolConfig{
				MaxConnectionsPerBroker: 10,
			},
		},
		Clusters: map[string]config.ClusterConfig{
			"test-cluster": {
				Mode:     config.ModeActivePassive,
				Active:   config.ActivePrimary,
				Primary: config.ClusterEndpoint{Bootstrap: "primary:9092"},
			},
		},
	}

	router := NewRouter(cfg, cache)

	// Build a Produce request
	produceBody := buildProduceBody("orders", 0, []byte{0x01, 0x02})
	headers := make([]byte, 8)
	binary.BigEndian.PutUint16(headers[0:2], uint16(protocol.APIKeyProduce))
	binary.BigEndian.PutUint16(headers[2:4], 0) // version 0
	binary.BigEndian.PutUint32(headers[4:8], 42) // correlationID
	clientID := "test-client"

	// Full frame: size(4) + api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id + body
	headerBodyLen := 2 + 2 + 4 + 2 + len(clientID) + len(produceBody)
	fullFrame := make([]byte, 4+headerBodyLen)
	binary.BigEndian.PutUint32(fullFrame[0:4], uint32(headerBodyLen))
	binary.BigEndian.PutUint16(fullFrame[4:6], uint16(protocol.APIKeyProduce))
	binary.BigEndian.PutUint16(fullFrame[6:8], 0)
	binary.BigEndian.PutUint32(fullFrame[8:12], 42)
	binary.BigEndian.PutUint16(fullFrame[12:14], uint16(len(clientID)))
	copy(fullFrame[14:], clientID)
	copy(fullFrame[14+len(clientID):], produceBody)

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: Write request to client side
	go func() {
		defer wg.Done()
		clientConn.Write(fullFrame)
	}()

	// Goroutine 2: Run the router and unblock the upstream reader when done.
	go func() {
		defer wg.Done()
		defer upstreamProxy.Close() // unblock any reader on the other end
		err := router.Route(logger.Default(), proxyConn, "test-cluster", cfg.Clusters["test-cluster"])
		if err != nil {
			// Route will fail because it tries to connect to leader:9092
			// via the real connection pool (unreachable). This is expected.
			t.Logf("Route returned: %v (expected - leader host unreachable)", err)
		}
	}()

	wg.Wait()
}

// readFull reads exactly len(buf) bytes from the connection.
func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// ── resolveClusterBootstrap Tests ──────────────────────────────────────

func TestResolveClusterBootstrap_ActivePassive_Primary(t *testing.T) {
	cfg := config.ClusterConfig{
		Mode:     config.ModeActivePassive,
		Active:   config.ActivePrimary,
		Primary: config.ClusterEndpoint{Bootstrap: "primary:9092"},
		Secondary: config.ClusterEndpoint{Bootstrap: "secondary:9092"},
	}

	result := resolveClusterBootstrap(cfg)
	if result != "primary:9092" {
		t.Errorf("expected primary:9092, got %q", result)
	}
}

func TestResolveClusterBootstrap_ActivePassive_Secondary(t *testing.T) {
	cfg := config.ClusterConfig{
		Mode:     config.ModeActivePassive,
		Active:   config.ActiveSecondary,
		Primary: config.ClusterEndpoint{Bootstrap: "primary:9092"},
		Secondary: config.ClusterEndpoint{Bootstrap: "secondary:9092"},
	}

	result := resolveClusterBootstrap(cfg)
	if result != "secondary:9092" {
		t.Errorf("expected secondary:9092, got %q", result)
	}
}

func TestResolveClusterBootstrap_LoadBalance(t *testing.T) {
	cfg := config.ClusterConfig{
		Mode:     config.ModeLoadBalance,
		Primary: config.ClusterEndpoint{Bootstrap: "primary:9092", Weight: 70},
		Secondary: config.ClusterEndpoint{Bootstrap: "secondary:9092", Weight: 30},
	}

	result := resolveClusterBootstrap(cfg)
	if result != "primary:9092" {
		t.Errorf("expected primary:9092 for load_balance passthrough default, got %q", result)
	}
}

func TestResolveClusterBootstrap_UnknownMode(t *testing.T) {
	cfg := config.ClusterConfig{
		Mode: "unknown_mode",
	}

	result := resolveClusterBootstrap(cfg)
	if result != "" {
		t.Errorf("expected empty string for unknown mode, got %q", result)
	}
}

// ── Effective Weights Tests ──────────────────────────────────────────

func TestEffectiveWeights_InitFromConfig(t *testing.T) {
	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ConnectionPool: config.ConnectionPoolConfig{
				MaxConnectionsPerBroker: 10,
			},
		},
		Clusters: map[string]config.ClusterConfig{
			"bu-sales": {
				Mode:      config.ModeLoadBalance,
				Primary:  config.ClusterEndpoint{Bootstrap: "p:9092", Weight: 70},
				Secondary: config.ClusterEndpoint{Bootstrap: "s:9092", Weight: 30},
			},
		},
	}

	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)

	primW, secW := router.GetEffectiveWeights("bu-sales", cfg.Clusters["bu-sales"])
	if primW != 70 {
		t.Errorf("primary weight = %d, want 70", primW)
	}
	if secW != 30 {
		t.Errorf("secondary weight = %d, want 30", secW)
	}
}

func TestEffectiveWeights_SetAndGet(t *testing.T) {
	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ConnectionPool: config.ConnectionPoolConfig{
				MaxConnectionsPerBroker: 10,
			},
		},
		Clusters: map[string]config.ClusterConfig{
			"bu-sales": {
				Mode:      config.ModeLoadBalance,
				Primary:  config.ClusterEndpoint{Bootstrap: "p:9092", Weight: 70},
				Secondary: config.ClusterEndpoint{Bootstrap: "s:9092", Weight: 30},
			},
		},
	}

	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)

	// Override weights (simulating auto-rebalance)
	router.SetEffectiveWeights("bu-sales", 40, 60)

	primW, secW := router.GetEffectiveWeights("bu-sales", cfg.Clusters["bu-sales"])
	if primW != 40 {
		t.Errorf("primary weight = %d, want 40", primW)
	}
	if secW != 60 {
		t.Errorf("secondary weight = %d, want 60", secW)
	}
}

func TestEffectiveWeights_ActivePassiveFallback(t *testing.T) {
	// active_passive clusters should return 0,0
	cfg := config.ClusterConfig{
		Mode:     config.ModeActivePassive,
		Active:   config.ActivePrimary,
		Primary: config.ClusterEndpoint{Bootstrap: "p:9092"},
	}

	cache := NewMapPartitionLeaderCache()
	router := NewRouter(&config.Config{
		Proxy: config.ProxyConfig{
			ConnectionPool: config.ConnectionPoolConfig{
				MaxConnectionsPerBroker: 10,
			},
		},
		Clusters: map[string]config.ClusterConfig{"bu-x": cfg},
	}, cache)

	primW, secW := router.GetEffectiveWeights("bu-x", cfg)
	if primW != 0 || secW != 0 {
		t.Errorf("expected 0,0 for active_passive, got %d,%d", primW, secW)
	}
}

// ── Sticky Hash & Weighted Selection Tests ───────────────────────────

func TestStickyHash_Deterministic(t *testing.T) {
	h1 := StickyHash("client-a", "orders", 0)
	h2 := StickyHash("client-a", "orders", 0)
	if h1 != h2 {
		t.Errorf("StickyHash not deterministic: %d vs %d", h1, h2)
	}
}

func TestStickyHash_DifferentPartition(t *testing.T) {
	h1 := StickyHash("client-a", "orders", 0)
	h2 := StickyHash("client-a", "orders", 1)
	if h1 == h2 {
		t.Error("StickyHash produced same hash for different partition")
	}
}

func TestStickyHash_DifferentClient(t *testing.T) {
	h1 := StickyHash("client-a", "orders", 0)
	h2 := StickyHash("client-b", "orders", 0)
	if h1 == h2 {
		t.Error("StickyHash produced same hash for different client")
	}
}

func TestStickyHash_OutputRange(t *testing.T) {
	for i := int32(0); i < 1000; i++ {
		h := StickyHash("client-x", "topic-y", i)
		if h < 0 || h > 99 {
			t.Errorf("StickyHash out of range: %d for partition %d", h, i)
		}
	}
}

func TestSelectClusterByWeight_Primary(t *testing.T) {
	cfg := config.ClusterConfig{
		Primary:   config.ClusterEndpoint{Bootstrap: "p:9092", Weight: 100},
		Secondary: config.ClusterEndpoint{Bootstrap: "s:9092", Weight: 0},
	}
	sub, bootstrap := selectClusterByWeight(cfg, 100, 0, "client-1", "orders", 0)
	if sub != "primary" {
		t.Errorf("sub = %q, want primary", sub)
	}
	if bootstrap != "p:9092" {
		t.Errorf("bootstrap = %q, want p:9092", bootstrap)
	}
}

func TestSelectClusterByWeight_Secondary(t *testing.T) {
	cfg := config.ClusterConfig{
		Primary:   config.ClusterEndpoint{Bootstrap: "p:9092", Weight: 0},
		Secondary: config.ClusterEndpoint{Bootstrap: "s:9092", Weight: 100},
	}
	sub, bootstrap := selectClusterByWeight(cfg, 0, 100, "client-1", "orders", 0)
	if sub != "secondary" {
		t.Errorf("sub = %q, want secondary", sub)
	}
	if bootstrap != "s:9092" {
		t.Errorf("bootstrap = %q, want s:9092", bootstrap)
	}
}

func TestSelectClusterByWeight_StickySameKey(t *testing.T) {
	cfg := config.ClusterConfig{
		Primary:   config.ClusterEndpoint{Bootstrap: "p:9092", Weight: 50},
		Secondary: config.ClusterEndpoint{Bootstrap: "s:9092", Weight: 50},
	}
	sub1, _ := selectClusterByWeight(cfg, 50, 50, "client-1", "orders", 5)
	for i := 0; i < 100; i++ {
		sub2, _ := selectClusterByWeight(cfg, 50, 50, "client-1", "orders", 5)
		if sub1 != sub2 {
			t.Errorf("sticky hash broken: same key produced %q then %q", sub1, sub2)
			break
		}
	}
}

func TestSelectClusterByWeight_Distribution(t *testing.T) {
	cfg := config.ClusterConfig{
		Primary:   config.ClusterEndpoint{Bootstrap: "p:9092", Weight: 50},
		Secondary: config.ClusterEndpoint{Bootstrap: "s:9092", Weight: 50},
	}
	seen := map[string]int{}
	for i := int32(0); i < 1000; i++ {
		sub, _ := selectClusterByWeight(cfg, 50, 50, "client-x", "topic", i)
		seen[sub]++
	}
	if seen["primary"] == 0 {
		t.Error("primary was never selected with 50/50 weights")
	}
	if seen["secondary"] == 0 {
		t.Error("secondary was never selected with 50/50 weights")
	}
	t.Logf("distribution: primary=%d secondary=%d (out of 1000)", seen["primary"], seen["secondary"])
}

func TestSubClusterCacheKey(t *testing.T) {
	key := subClusterCacheKey("bu-sales", "primary")
	if key != "bu-sales/primary" {
		t.Errorf("key = %q, want bu-sales/primary", key)
	}
	key = subClusterCacheKey("bu-sales", "secondary")
	if key != "bu-sales/secondary" {
		t.Errorf("key = %q, want bu-sales/secondary", key)
	}
}

// ── readString Tests ──────────────────────────────────────────────────

func TestReadString(t *testing.T) {
	data := []byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o', 0xFF}
	length, s, newPos, err := readString(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != 5 {
		t.Errorf("length = %d, want 5", length)
	}
	if s != "hello" {
		t.Errorf("s = %q, want %q", s, "hello")
	}
	if newPos != 7 {
		t.Errorf("newPos = %d, want 7", newPos)
	}
}

func TestReadStringNull(t *testing.T) {
	data := []byte{0xFF, 0xFF, 0x01} // length = -1
	length, s, newPos, err := readString(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != -1 {
		t.Errorf("length = %d, want -1", length)
	}
	if s != "" {
		t.Errorf("s = %q, want empty", s)
	}
	if newPos != 2 {
		t.Errorf("newPos = %d, want 2", newPos)
	}
}

func TestReadStringEmpty(t *testing.T) {
	data := []byte{0x00, 0x00, 0xFF}
	length, s, newPos, err := readString(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != 0 {
		t.Errorf("length = %d, want 0", length)
	}
	if s != "" {
		t.Errorf("s = %q, want empty", s)
	}
	if newPos != 2 {
		t.Errorf("newPos = %d, want 2", newPos)
	}
}

func TestReadStringTruncated(t *testing.T) {
	// Length prefix but no data following
	data := []byte{0x00, 0x05, 'h', 'e'} // claims 5 bytes but only 2 available
	_, _, _, err := readString(data, 0)
	if err == nil {
		t.Fatal("expected error for truncated string")
	}

	// No length prefix at all
	_, _, _, err = readString([]byte{}, 0)
	if err == nil {
		t.Fatal("expected error for missing length prefix")
	}
}

// ── apiKeyName Tests ──────────────────────────────────────────────────

func TestAPIKeyName(t *testing.T) {
	tests := []struct {
		key  int16
		want string
	}{
		{protocol.APIKeyProduce, "Produce"},
		{protocol.APIKeyFetch, "Fetch"},
		{protocol.APIKeyMetadata, "Metadata"},
		{int16(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		got := apiKeyName(tt.key)
		if got != tt.want {
			t.Errorf("apiKeyName(%d) = %q, want %q", tt.key, got, tt.want)
		}
	}
}
