// ── Test helpers: MetadataResponse v0 encoding ────────────────────────
//
// These types and functions build valid Kafka MetadataResponse v0 byte
// streams so we can exercise parseMetadataResponseV0 without a real broker.

package pool

import "encoding/binary"
type brokerV0 struct {
	nodeID int32
	host   string
	port   int32
}

type partitionV0 struct {
	index    int32
	leaderID int32
}

type topicV0 struct {
	name       string
	errorCode  int16
	partitions []partitionV0
}

type metadataV0 struct {
	brokers []brokerV0
	topics  []topicV0
}

// encodeMetadataResponseV0 builds a MetadataResponse v0 body (without the
// 4-byte size prefix, which is added by readKafkaResponse at the transport
// layer — the parseMetadataResponseV0 function expects just the body).
func encodeMetadataResponseV0(m metadataV0) []byte {
	// Calculate total size
	size := 4 // correlation ID (placeholder)
	size += 4 // broker count
	for _, b := range m.brokers {
		size += 4 + 2 + len(b.host) + 4 // nodeID + hostLen + host + port
	}
	size += 4 // topic count
	for _, t := range m.topics {
		size += 2 + 2 + len(t.name) // errorCode + nameLen + name
		size += 4                    // partition count
		for range t.partitions {
			size += 2 + 4 + 4  // errorCode + index + leaderID
			size += 4           // replica count (0)
			size += 4           // ISR count (0)
		}
	}

	buf := make([]byte, size)
	pos := 0

	// Correlation ID (placeholder)
	binary.BigEndian.PutUint32(buf[pos:pos+4], 42)
	pos += 4

	// Brokers
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(m.brokers)))
	pos += 4
	for _, b := range m.brokers {
		binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(b.nodeID))
		pos += 4
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(b.host)))
		pos += 2
		copy(buf[pos:pos+len(b.host)], b.host)
		pos += len(b.host)
		binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(b.port))
		pos += 4
	}

	// Topics
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(m.topics)))
	pos += 4
	for _, t := range m.topics {
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(t.errorCode))
		pos += 2
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(t.name)))
		pos += 2
		copy(buf[pos:pos+len(t.name)], t.name)
		pos += len(t.name)

		binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(t.partitions)))
		pos += 4
		for _, p := range t.partitions {
			binary.BigEndian.PutUint16(buf[pos:pos+2], 0) // errorCode = 0 (ok)
			pos += 2
			binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(p.index))
			pos += 4
			binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(p.leaderID))
			pos += 4
			// Replicas: empty array
			binary.BigEndian.PutUint32(buf[pos:pos+4], 0)
			pos += 4
			// ISR: empty array
			binary.BigEndian.PutUint32(buf[pos:pos+4], 0)
			pos += 4
		}
	}

	return buf
}
