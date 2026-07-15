// Package routing implements protocol-aware Kafka message routing.
// This file provides sticky hash routing for load_balance mode clusters.
package routing

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

// StickyHash computes a deterministic hash value in [0, 99] from the topic
// and partition. Unlike the previous version that included clientID, this
// uses only (topic, partition) so that ALL clients — producers and consumers —
// route to the same cluster for a given partition. This is essential when
// clusters are NOT mirrored: each partition must have a fixed "owner" cluster.
//
// Uses FNV-64a for stable, deterministic output per process instance.
func StickyHash(topic string, partition int32) int {
	h := fnv.New64a()

	h.Write([]byte(topic))

	var partBuf [4]byte
	binary.BigEndian.PutUint32(partBuf[:], uint32(partition))
	h.Write(partBuf[:])

	return int(h.Sum64() % 100)
}

// selectClusterByWeight uses FNV-64a sticky hashing of (topic, partition)
// to choose between primary and secondary based on effective weights.
// Returns the chosen sub-cluster name and its bootstrap address.
// Every client (producer or consumer) will route to the same cluster for
// the same partition, because the hash is client-agnostic.
func selectClusterByWeight(
	cfg config.ClusterConfig,
	primaryW, secondaryW int,
	topic string, partition int32,
) (subCluster, bootstrap string) {
	hash := StickyHash(topic, partition)
	if hash < primaryW {
		return "primary", cfg.Primary.Bootstrap
	}
	return "secondary", cfg.Secondary.Bootstrap
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
