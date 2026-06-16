// Package routing implements protocol-aware Kafka message routing.
// This file provides sticky hash routing for load_balance mode clusters.
package routing

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

// StickyHash computes a deterministic hash value in [0, 99] from the
// clientID, topic, and partition routing key. It uses FNV-64a to produce
// a stable mapping so that the same client-topic-partition tuple always
// routes to the same cluster in load_balance mode.
//
// The function is deterministic per process instance (FNV-64a with the
// same inputs always produces the same output).
func StickyHash(clientID, topic string, partition int32) int {
	h := fnv.New64a()

	// Write the routing key components into the hash.
	h.Write([]byte(clientID))
	h.Write([]byte(topic))

	// Partition as 4 bytes big-endian for consistent byte representation.
	var partBuf [4]byte
	binary.BigEndian.PutUint32(partBuf[:], uint32(partition))
	h.Write(partBuf[:])

	return int(h.Sum64() % 100)
}

// selectClusterByWeight uses FNV-64a sticky hashing of (clientID, topic,
// partition) to choose between primary and secondary based on effective
// weights. Returns the chosen sub-cluster name and its bootstrap address.
func selectClusterByWeight(
	cfg config.ClusterConfig,
	primaryW, secondaryW int,
	clientID, topic string, partition int32,
) (subCluster, bootstrap string) {
	hash := StickyHash(clientID, topic, partition)
	if hash < primaryW {
		return "primary", cfg.Primary.Bootstrap
	}
	return "secondary", cfg.Secondary.Bootstrap
}

// subClusterCacheKey builds a cache lookup key for partition leader cache
// entries keyed by (buCluster, subCluster).
func subClusterCacheKey(buCluster, subCluster string) string {
	return fmt.Sprintf("%s/%s", buCluster, subCluster)
}

// stickyHash is a legacy compatibility wrapper that takes a pre-serialized
// key string. It delegates to StickyHash after splitting on colons.
// Format: "topic:partition" or "clientID:topic:partition"
func stickyHash(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % 100)
}

// selectClusterByWeightLegacy is a compatibility wrapper that accepts a
// pre-formatted key string.
func selectClusterByWeightLegacy(cfg config.ClusterConfig, primaryW, secondaryW int, key string) (subCluster, bootstrap string) {
	hash := stickyHash(key)
	if hash < primaryW {
		return "primary", cfg.Primary.Bootstrap
	}
	return "secondary", cfg.Secondary.Bootstrap
}
