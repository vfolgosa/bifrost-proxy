// Package routing implements Produce/Fetch request routing with partition
// awareness. It detects API Key 0 (Produce) and API Key 1 (Fetch), parses
// topic and partition from the request body, looks up the leader broker
// from the partition leader cache, and forwards the request to the correct
// broker via a pooled connection.
package routing

import (
	"fmt"
	"sync"
)

// PartitionLeaderCache tracks which broker is the leader for each partition.
// It is populated by metadata requests and used by the Router to determine
// where to forward Produce and Fetch requests.
//
// The cluster parameter in all methods is the upstream bootstrap address
// (host:port), not the BU/cluster name.
type PartitionLeaderCache interface {
	// GetLeader returns the broker address for the given cluster bootstrap,
	// topic, and partition. The bool is false when the leader is not known.
	GetLeader(cluster, topic string, partition int32) (string, bool)

	// SetLeader records a leader entry.
	SetLeader(cluster, topic string, partition int32, brokerAddr string)

	// Invalidate clears all cached entries for a given cluster (e.g. after
	// a failover or when metadata needs to be refreshed).
	Invalidate(cluster string)

	// RefreshMetadata triggers a metadata refresh for the given cluster
	// bootstrap address. Returns an error if the refresh fails. This is called
	// when the cache has no entry for a requested partition (leader unknown).
	RefreshMetadata(cluster string) error
}

// MapPartitionLeaderCache is a simple in-memory implementation of
// PartitionLeaderCache backed by a sync.RWMutex-protected map.
type MapPartitionLeaderCache struct {
	mu   sync.RWMutex
	data map[string]string // key: "cluster/topic/partition" → broker address
}

// NewMapPartitionLeaderCache creates an empty MapPartitionLeaderCache.
func NewMapPartitionLeaderCache() *MapPartitionLeaderCache {
	return &MapPartitionLeaderCache{
		data: make(map[string]string),
	}
}

func key(cluster, topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/%d", cluster, topic, partition)
}

// GetLeader implements PartitionLeaderCache.
func (c *MapPartitionLeaderCache) GetLeader(cluster, topic string, partition int32) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	addr, ok := c.data[key(cluster, topic, partition)]
	return addr, ok
}

// SetLeader implements PartitionLeaderCache.
func (c *MapPartitionLeaderCache) SetLeader(cluster, topic string, partition int32, brokerAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key(cluster, topic, partition)] = brokerAddr
}

// Invalidate implements PartitionLeaderCache.
func (c *MapPartitionLeaderCache) Invalidate(cluster string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := cluster + "/"
	for k := range c.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(c.data, k)
		}
	}
}

// RefreshMetadata implements PartitionLeaderCache. For now it is a stub —
// metadata-driven cache population will be implemented in a later task.
func (c *MapPartitionLeaderCache) RefreshMetadata(cluster string) error {
	// TODO: Implement metadata request to cluster bootstrap, parse
	// response, and populate the cache with leader entries.
	return fmt.Errorf("metadata refresh not yet implemented for cluster %q", cluster)
}
