package pool

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

// ── GetLeader ─────────────────────────────────────────────────────────

func TestGetLeader_Hit(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.mu.Lock()
	c.leaders["cluster:9092"] = map[string]map[int32]string{
		"orders": {0: "b0:9092", 1: "b1:9092"},
	}
	c.mu.Unlock()

	addr, ok := c.GetLeader("cluster:9092", "orders", 0)
	if !ok {
		t.Fatal("expected leader found")
	}
	if addr != "b0:9092" {
		t.Fatalf("expected b0:9092, got %s", addr)
	}
}

func TestGetLeader_Miss_UnknownCluster(t *testing.T) {
	c := NewPartitionLeaderCache()
	_, ok := c.GetLeader("no-such-cluster:9092", "orders", 0)
	if ok {
		t.Fatal("expected miss for unknown cluster")
	}
}

func TestGetLeader_Miss_UnknownTopic(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.mu.Lock()
	c.leaders["cluster:9092"] = map[string]map[int32]string{
		"orders": {0: "b0:9092"},
	}
	c.mu.Unlock()

	_, ok := c.GetLeader("cluster:9092", "payments", 0)
	if ok {
		t.Fatal("expected miss for unknown topic")
	}
}

func TestGetLeader_Miss_UnknownPartition(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.mu.Lock()
	c.leaders["cluster:9092"] = map[string]map[int32]string{
		"orders": {0: "b0:9092"},
	}
	c.mu.Unlock()

	_, ok := c.GetLeader("cluster:9092", "orders", 99)
	if ok {
		t.Fatal("expected miss for unknown partition")
	}
}

func TestGetLeader_ConcurrentReads(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.mu.Lock()
	c.leaders["cluster:9092"] = map[string]map[int32]string{
		"orders": {0: "b0:9092"},
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, ok := c.GetLeader("cluster:9092", "orders", 0)
			if !ok || addr != "b0:9092" {
				t.Errorf("concurrent read: ok=%v addr=%s", ok, addr)
			}
		}()
	}
	wg.Wait()
}

// ── Refresh parsing (MetadataResponse v0) ────────────────────────────

func TestRefresh_ValidResponse(t *testing.T) {
	// Build a valid MetadataResponse v0 body with:
	// - 2 brokers: {1, "b0.example.com", 9092}, {2, "b1.example.com", 9093}
	// - 1 topic "orders" with 2 partitions: 0→leader 1, 1→leader 2

	resp := encodeMetadataResponseV0(metadataV0{
		brokers: []brokerV0{
			{nodeID: 1, host: "b0.example.com", port: 9092},
			{nodeID: 2, host: "b1.example.com", port: 9093},
		},
		topics: []topicV0{
			{
				name: "orders",
				partitions: []partitionV0{
					{index: 0, leaderID: 1},
					{index: 1, leaderID: 2},
				},
			},
		},
	})

	leaders, err := parseMetadataResponseV0(resp)
	if err != nil {
		t.Fatalf("parseMetadataResponseV0: %v", err)
	}

	if addr, ok := leaders["orders"][0]; !ok || addr != "b0.example.com:9092" {
		t.Fatalf("orders/0: expected b0.example.com:9092, got %q (ok=%v)", addr, ok)
	}
	if addr, ok := leaders["orders"][1]; !ok || addr != "b1.example.com:9093" {
		t.Fatalf("orders/1: expected b1.example.com:9093, got %q (ok=%v)", addr, ok)
	}
}

func TestRefresh_TopicWithError_Skipped(t *testing.T) {
	// Topic "orders" has error code 3 (UNKNOWN_TOPIC_OR_PARTITION)
	// Topic "payments" is healthy
	resp := encodeMetadataResponseV0(metadataV0{
		brokers: []brokerV0{
			{nodeID: 1, host: "b0.example.com", port: 9092},
		},
		topics: []topicV0{
			{
				name:       "orders",
				errorCode:  3, // UNKNOWN_TOPIC_OR_PARTITION
				partitions: []partitionV0{{index: 0, leaderID: 1}},
			},
			{
				name: "payments",
				partitions: []partitionV0{
					{index: 0, leaderID: 1},
				},
			},
		},
	})

	leaders, err := parseMetadataResponseV0(resp)
	if err != nil {
		t.Fatalf("parseMetadataResponseV0: %v", err)
	}

	if _, ok := leaders["orders"]; ok {
		t.Fatal("orders should be skipped (topic error)")
	}
	if addr, ok := leaders["payments"][0]; !ok || addr != "b0.example.com:9092" {
		t.Fatalf("payments/0: expected b0.example.com:9092, got %q (ok=%v)", addr, ok)
	}
}

func TestRefresh_MultipleTopics(t *testing.T) {
	resp := encodeMetadataResponseV0(metadataV0{
		brokers: []brokerV0{
			{nodeID: 1, host: "b0.example.com", port: 9092},
			{nodeID: 2, host: "b1.example.com", port: 9092},
		},
		topics: []topicV0{
			{
				name: "orders",
				partitions: []partitionV0{
					{index: 0, leaderID: 1},
					{index: 1, leaderID: 2},
					{index: 2, leaderID: 1},
				},
			},
			{
				name: "payments",
				partitions: []partitionV0{
					{index: 0, leaderID: 2},
				},
			},
		},
	})

	leaders, err := parseMetadataResponseV0(resp)
	if err != nil {
		t.Fatalf("parseMetadataResponseV0: %v", err)
	}

	if len(leaders) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(leaders))
	}

	// orders
	if got := leaders["orders"][0]; got != "b0.example.com:9092" {
		t.Errorf("orders/0: got %q", got)
	}
	if got := leaders["orders"][1]; got != "b1.example.com:9092" {
		t.Errorf("orders/1: got %q", got)
	}
	if got := leaders["orders"][2]; got != "b0.example.com:9092" {
		t.Errorf("orders/2: got %q", got)
	}

	// payments
	if got := leaders["payments"][0]; got != "b1.example.com:9092" {
		t.Errorf("payments/0: got %q", got)
	}
}

func TestRefresh_TruncatedResponse(t *testing.T) {
	_, err := parseMetadataResponseV0([]byte{0, 0, 0})
	if err == nil {
		t.Fatal("expected error for truncated response")
	}
}

func TestRefresh_EmptyResponse(t *testing.T) {
	// No brokers, no topics — valid edge case
	resp := encodeMetadataResponseV0(metadataV0{})
	leaders, err := parseMetadataResponseV0(resp)
	if err != nil {
		t.Fatalf("parseMetadataResponseV0: %v", err)
	}
	if len(leaders) != 0 {
		t.Fatalf("expected 0 topics, got %d", len(leaders))
	}
}

func TestRefresh_NoBrokers(t *testing.T) {
	// Topic references broker 1 but no brokers in response
	resp := encodeMetadataResponseV0(metadataV0{
		topics: []topicV0{
			{
				name: "orders",
				partitions: []partitionV0{
					{index: 0, leaderID: 1},
				},
			},
		},
	})

	_, err := parseMetadataResponseV0(resp)
	if err == nil {
		t.Fatal("expected error for unknown leader broker")
	}
}

// ── Background refresh lifecycle ─────────────────────────────────────

func TestStartStopBackgroundRefresh(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.refreshInterval = 10 * time.Millisecond

	c.StartBackgroundRefresh("cluster:9092")
	if len(c.ActiveClusters()) != 1 {
		t.Fatal("expected 1 active cluster")
	}

	// Wait for at least one background tick attempt
	// (it will fail because there's no real Kafka, but the loop runs)
	time.Sleep(50 * time.Millisecond)

	c.StopBackgroundRefresh("cluster:9092")
	if len(c.ActiveClusters()) != 0 {
		t.Fatal("expected 0 active clusters after stop")
	}
}

func TestStartBackgroundRefresh_Idempotent(t *testing.T) {
	c := NewPartitionLeaderCache()

	c.StartBackgroundRefresh("cluster:9092")
	c.StartBackgroundRefresh("cluster:9092") // second call should be no-op

	if len(c.ActiveClusters()) != 1 {
		t.Fatalf("expected 1 active cluster, got %d", len(c.ActiveClusters()))
	}

	c.StopAllBackgroundRefreshes()
}

func TestTriggerRefresh_BeforeStart(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.TriggerRefresh("cluster:9092") // should not panic when no refresher exists
}

func TestTriggerRefresh_AfterStop(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.StartBackgroundRefresh("cluster:9092")
	c.StopBackgroundRefresh("cluster:9092")
	c.TriggerRefresh("cluster:9092") // should not panic
}

// ── Refresh swap (with fake network) ─────────────────────────────────

func TestRefresh_SwapsAtomically(t *testing.T) {
	c := NewPartitionLeaderCache()

	// Populate initial state
	c.mu.Lock()
	c.leaders["cluster:9092"] = map[string]map[int32]string{
		"old-topic": {0: "old-broker:9092"},
	}
	c.mu.Unlock()

	// Start a listener to simulate a Kafka broker responding with metadata
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	// Serve one connection with a valid MetadataResponse
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()

		// Read the MetadataRequest (discard it)
		buf := make([]byte, 1024)
		conn.Read(buf)

		// Send MetadataResponse with one topic
		resp := encodeMetadataResponseV0(metadataV0{
			brokers: []brokerV0{
				{nodeID: 10, host: "new-broker.example.com", port: 9092},
			},
			topics: []topicV0{
				{
					name: "new-topic",
					partitions: []partitionV0{
						{index: 0, leaderID: 10},
					},
				},
			},
		})

		// Prepend size prefix that Kafka expects
		sizeBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuf, uint32(len(resp)))
		conn.Write(sizeBuf)
		conn.Write(resp)
	}()

	// Refresh from our fake Kafka
	err = c.Refresh(addr)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// Old data should be gone, new data present
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, ok := c.leaders[addr]["old-topic"]; ok {
		t.Fatal("old-topic should be gone after swap")
	}
	if addr, ok := c.getLeaderLocked(addr, "new-topic", 0); !ok || addr != "new-broker.example.com:9092" {
		t.Fatalf("new-topic/0: expected new-broker.example.com:9092, got %q (ok=%v)", addr, ok)
	}
}

// ── SetLeader / Invalidate / RefreshMetadata ─────────────────────────

func TestSetLeader_NewCluster(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.SetLeader("cluster:9092", "orders", 0, "b0:9092")

	addr, ok := c.GetLeader("cluster:9092", "orders", 0)
	if !ok {
		t.Fatal("expected leader found after SetLeader")
	}
	if addr != "b0:9092" {
		t.Fatalf("expected b0:9092, got %s", addr)
	}
}

func TestSetLeader_Overwrite(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.SetLeader("cluster:9092", "orders", 0, "old-broker:9092")
	c.SetLeader("cluster:9092", "orders", 0, "new-broker:9092")

	addr, ok := c.GetLeader("cluster:9092", "orders", 0)
	if !ok {
		t.Fatal("expected leader found after overwrite")
	}
	if addr != "new-broker:9092" {
		t.Fatalf("expected new-broker:9092, got %s", addr)
	}
}

func TestSetLeader_MultipleTopicsAndPartitions(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.SetLeader("cluster:9092", "orders", 0, "b0:9092")
	c.SetLeader("cluster:9092", "orders", 1, "b1:9092")
	c.SetLeader("cluster:9092", "payments", 0, "b2:9092")

	if addr, ok := c.GetLeader("cluster:9092", "orders", 0); !ok || addr != "b0:9092" {
		t.Fatalf("orders/0: ok=%v addr=%q", ok, addr)
	}
	if addr, ok := c.GetLeader("cluster:9092", "orders", 1); !ok || addr != "b1:9092" {
		t.Fatalf("orders/1: ok=%v addr=%q", ok, addr)
	}
	if addr, ok := c.GetLeader("cluster:9092", "payments", 0); !ok || addr != "b2:9092" {
		t.Fatalf("payments/0: ok=%v addr=%q", ok, addr)
	}
}

func TestInvalidate_RemovesCluster(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.SetLeader("cluster:9092", "orders", 0, "b0:9092")
	c.SetLeader("cluster:9092", "payments", 1, "b1:9092")
	c.SetLeader("other:9092", "inventory", 0, "b2:9092")

	c.Invalidate("cluster:9092")

	// Entries for cluster:9092 should be gone
	if _, ok := c.GetLeader("cluster:9092", "orders", 0); ok {
		t.Fatal("expected miss after Invalidate for orders/0")
	}
	if _, ok := c.GetLeader("cluster:9092", "payments", 1); ok {
		t.Fatal("expected miss after Invalidate for payments/1")
	}

	// Entries for other:9092 should still be present
	if addr, ok := c.GetLeader("other:9092", "inventory", 0); !ok || addr != "b2:9092" {
		t.Fatalf("other cluster should not be affected: ok=%v addr=%q", ok, addr)
	}
}

func TestInvalidate_UnknownCluster_NoOp(t *testing.T) {
	c := NewPartitionLeaderCache()
	c.SetLeader("cluster:9092", "orders", 0, "b0:9092")

	// Should not panic
	c.Invalidate("unknown:9092")

	if _, ok := c.GetLeader("cluster:9092", "orders", 0); !ok {
		t.Fatal("existing entries should not be affected")
	}
}

func TestRefreshMetadata_DelegatesToRefresh(t *testing.T) {
	c := NewPartitionLeaderCache()

	// Set up a fake listener to serve a MetadataResponse
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 1024)
		conn.Read(buf)

		resp := encodeMetadataResponseV0(metadataV0{
			brokers: []brokerV0{
				{nodeID: 5, host: "broker5.example.com", port: 9092},
			},
			topics: []topicV0{
				{
					name: "orders",
					partitions: []partitionV0{
						{index: 0, leaderID: 5},
					},
				},
			},
		})

		sizeBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuf, uint32(len(resp)))
		conn.Write(sizeBuf)
		conn.Write(resp)
	}()

	if err := c.RefreshMetadata(addr); err != nil {
		t.Fatalf("RefreshMetadata: %v", err)
	}

	if addr, ok := c.GetLeader(addr, "orders", 0); !ok || addr != "broker5.example.com:9092" {
		t.Fatalf("orders/0: expected broker5.example.com:9092, got %q (ok=%v)", addr, ok)
	}
}

// ── Interface compliance check ────────────────────────────────────────

// Compile-time assertion that *PartitionLeaderCache implements
// routing.PartitionLeaderCache (avoids import cycle, checked in test).
var _ interface {
	GetLeader(cluster, topic string, partition int32) (string, bool)
	SetLeader(cluster, topic string, partition int32, brokerAddr string)
	Invalidate(cluster string)
	RefreshMetadata(cluster string) error
} = (*PartitionLeaderCache)(nil)
