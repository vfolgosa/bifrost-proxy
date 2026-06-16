// Package routing implements synthetic MetadataResponse merging for
// load_balance mode per the proxy spec Section 5.2.
//
// When a client issues a MetadataRequest and the cluster is configured
// for load_balance mode, the proxy forwards the request to both the
// primary and secondary clusters concurrently, then merges the two
// responses into a single synthetic MetadataResponse:
//
//   - Broker lists are merged, deduplicated by NodeID, and all
//     host:port entries are rewritten to the proxy address.
//   - Topic metadata is merged with primary preferred (topics
//     present in both clusters use the primary's metadata).
//   - Other fields (throttle time, cluster ID, controller ID)
//     are taken from the primary response.
//   - If one cluster times out or fails, the response from the
//     healthy cluster is used (with brokers rewritten).
package routing

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// topicEntry holds the parsed metadata for one topic from a MetadataResponse.
type topicEntry struct {
	Name     string // Kafka topic name
	RawBytes []byte // full wire-format bytes for this topic entry (name + partitions + ...)
}

// SynthesizeMetadataResponse merges two raw MetadataResponse frames from the
// primary and secondary clusters into a single synthetic response suitable
// for load_balance mode clients.
//
// Both primaryResp and secondaryResp must be complete Kafka response frames:
// size prefix (4 bytes) + correlation_id (4 bytes) + body. If secondaryResp
// is nil (e.g. secondary timed out), the function falls back to the primary
// response with broker rewriting only.
//
// All broker host:port entries are rewritten to proxyHost:proxyPort.
// Topic metadata is merged with primary preferred.
// The returned slice is a complete Kafka response frame with a recalculated
// size prefix.
func SynthesizeMetadataResponse(
	primaryResp, secondaryResp []byte,
	proxyHost string, proxyPort int32,
	apiVersion int16,
) ([]byte, error) {
	// Validate proxy host length upfront
	if len(proxyHost) > MaxStringLen {
		return nil, fmt.Errorf("%w: proxy host is %d bytes", ErrBrokerHostTooLong, len(proxyHost))
	}

	// Parse the primary response
	primaryParsed, primaryCorrID, err := parseResponseFrame(primaryResp, apiVersion)
	if err != nil {
		return nil, fmt.Errorf("parsing primary metadata response: %w", err)
	}

	// Parse primary topics from bodyAfterBrokers
	primaryTopics, interBrokerBytes, err := parseTopicArray(primaryParsed.bodyAfterBrokers, apiVersion)
	if err != nil {
		return nil, fmt.Errorf("parsing primary topics: %w", err)
	}

	// If no secondary response, fall back to primary-only with brokers rewritten
	if secondaryResp == nil {
		return buildSyntheticResponse(
			primaryCorrID,
			primaryParsed.bodyBeforeBrokers,
			primaryParsed.Brokers,
			nil, // no secondary brokers
			interBrokerBytes,
			primaryTopics,
			nil, // no secondary topics
			proxyHost, proxyPort,
			apiVersion,
		)
	}

	// Parse the secondary response
	secondaryParsed, _, err := parseResponseFrame(secondaryResp, apiVersion)
	if err != nil {
		// Secondary failed to parse — fall back to primary-only
		return buildSyntheticResponse(
			primaryCorrID,
			primaryParsed.bodyBeforeBrokers,
			primaryParsed.Brokers,
			nil,
			interBrokerBytes,
			primaryTopics,
			nil,
			proxyHost, proxyPort,
			apiVersion,
		)
	}

	// Parse secondary topics
	secondaryTopics, _, err := parseTopicArray(secondaryParsed.bodyAfterBrokers, apiVersion)
	if err != nil {
		// Secondary topics unparseable — fall back to primary-only
		return buildSyntheticResponse(
			primaryCorrID,
			primaryParsed.bodyBeforeBrokers,
			primaryParsed.Brokers,
			nil,
			interBrokerBytes,
			primaryTopics,
			nil,
			proxyHost, proxyPort,
			apiVersion,
		)
	}

	return buildSyntheticResponse(
		primaryCorrID,
		primaryParsed.bodyBeforeBrokers,
		primaryParsed.Brokers,
		secondaryParsed.Brokers,
		interBrokerBytes,
		primaryTopics,
		secondaryTopics,
		proxyHost, proxyPort,
		apiVersion,
	)
}

// parseResponseFrame extracts the correlation ID and parses the body of a
// raw MetadataResponse frame.
func parseResponseFrame(raw []byte, version int16) (*MetadataResponse, int32, error) {
	if len(raw) > MaxMetadataResponseSize {
		return nil, 0, ErrMetadataResponseTooLarge
	}
	if len(raw) < 8 {
		return nil, 0, fmt.Errorf("%w: response frame too short (%d bytes)", ErrMetadataTooShort, len(raw))
	}

	frameSize := int32(binary.BigEndian.Uint32(raw[0:4]))
	correlationID := int32(binary.BigEndian.Uint32(raw[4:8]))

	if frameSize < 4 {
		return nil, 0, fmt.Errorf("invalid frame size %d (min 4 for correlation_id)", frameSize)
	}
	expectedLen := 4 + int(frameSize)
	if len(raw) < expectedLen {
		return nil, 0, fmt.Errorf("%w: frame size=%d but only %d bytes available",
			ErrMetadataTooShort, frameSize, len(raw))
	}

	body := raw[8:expectedLen]
	parsed, err := ParseMetadataResponseBody(body, version)
	if err != nil {
		return nil, 0, err
	}
	parsed.CorrelationID = correlationID
	return parsed, correlationID, nil
}

// parseTopicArray extracts topic entries from the bodyAfterBrokers bytes.
// Returns the list of topic entries, the inter-broker bytes (fields between
// the broker array and the topic array: ClusterId, ControllerId, etc.), and
// any error.
func parseTopicArray(data []byte, version int16) ([]topicEntry, []byte, error) {
	pos := 0

	// ── ClusterId (nullable string, v2+) ────────────────────────────
	if version >= 2 {
		if pos+2 > len(data) {
			return nil, nil, fmt.Errorf("%w: missing ClusterId length", ErrMetadataTooShort)
		}
		clusterIDLen := int(int16(binary.BigEndian.Uint16(data[pos : pos+2])))
		pos += 2
		if clusterIDLen >= 0 {
			if pos+clusterIDLen > len(data) {
				return nil, nil, fmt.Errorf("%w: truncated ClusterId", ErrMetadataTooShort)
			}
			pos += clusterIDLen
		}
	}

	// ── ControllerId (int32, v1+) ──────────────────────────────────
	if version >= 1 {
		if pos+4 > len(data) {
			return nil, nil, fmt.Errorf("%w: missing ControllerId", ErrMetadataTooShort)
		}
		pos += 4
	}

	// ── ClusterAuthorizedOperations (int32, v8+) ───────────────────
	if version >= 8 {
		if pos+4 > len(data) {
			return nil, nil, fmt.Errorf("%w: missing ClusterAuthorizedOperations", ErrMetadataTooShort)
		}
		pos += 4
	}

	// Inter-broker bytes = everything up to the topic array length
	interBrokerBytes := make([]byte, pos)
	copy(interBrokerBytes, data[:pos])

	// ── Topic array ────────────────────────────────────────────────
	if pos+4 > len(data) {
		return nil, nil, fmt.Errorf("%w: missing topic array length", ErrMetadataTooShort)
	}
	topicCount := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if topicCount < 0 {
		return nil, nil, fmt.Errorf("negative topic count %d", topicCount)
	}

	if topicCount == 0 {
		return nil, interBrokerBytes, nil
	}

	topics := make([]topicEntry, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		entry, bytesRead, err := parseOneTopic(data[pos:], version)
		if err != nil {
			return nil, nil, fmt.Errorf("topic[%d]: %w", i, err)
		}
		topics = append(topics, entry)
		pos += bytesRead
	}

	return topics, interBrokerBytes, nil
}

// parseOneTopic parses a single topic entry from raw bytes, returning the
// parsed topicEntry, the number of bytes consumed, and any error.
//
// Wire format:
//
//	ErrorCode(int16)
//	TopicName(string)
//	[IsInternal(bool), v1+]
//	Partitions array (count + entries)
//	[TopicAuthorizedOperations(int32), v8+]
func parseOneTopic(data []byte, version int16) (topicEntry, int, error) {
	pos := 0
	start := pos

	// ErrorCode (int16)
	if pos+2 > len(data) {
		return topicEntry{}, 0, fmt.Errorf("%w: missing topic ErrorCode", ErrMetadataTooShort)
	}
	pos += 2

	// TopicName (string: int16 length + UTF-8)
	if pos+2 > len(data) {
		return topicEntry{}, 0, fmt.Errorf("%w: missing TopicName length", ErrMetadataTooShort)
	}
	nameLen := int(int16(binary.BigEndian.Uint16(data[pos : pos+2])))
	pos += 2

	if nameLen < 0 {
		return topicEntry{}, 0, fmt.Errorf("invalid TopicName length: %d", nameLen)
	}
	if nameLen > MaxStringLen {
		return topicEntry{}, 0, fmt.Errorf("TopicName exceeds maximum length: %d", nameLen)
	}
	if pos+nameLen > len(data) {
		return topicEntry{}, 0, fmt.Errorf("%w: truncated TopicName", ErrMetadataTooShort)
	}
	name := string(data[pos : pos+nameLen])
	pos += nameLen

	// IsInternal (bool, v1+)
	if version >= 1 {
		if pos+1 > len(data) {
			return topicEntry{}, 0, fmt.Errorf("%w: missing IsInternal", ErrMetadataTooShort)
		}
		pos++
	}

	// Partitions array
	if pos+4 > len(data) {
		return topicEntry{}, 0, fmt.Errorf("%w: missing partition array length for topic %q", ErrMetadataTooShort, name)
	}
	partCount := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if partCount < 0 {
		return topicEntry{}, 0, fmt.Errorf("negative partition count %d for topic %q", partCount, name)
	}

	for i := int32(0); i < partCount; i++ {
		bytesRead, err := skipPartition(data[pos:], version)
		if err != nil {
			return topicEntry{}, 0, fmt.Errorf("topic %q partition[%d]: %w", name, i, err)
		}
		pos += bytesRead
	}

	// TopicAuthorizedOperations (int32, v8+)
	if version >= 8 {
		if pos+4 > len(data) {
			return topicEntry{}, 0, fmt.Errorf("%w: missing TopicAuthorizedOperations for topic %q", ErrMetadataTooShort, name)
		}
		pos += 4
	}

	return topicEntry{
		Name:     name,
		RawBytes: makeCopy(data[start:pos]),
	}, pos, nil
}

// skipPartition skips past a single partition entry in the topic metadata.
// Returns the number of bytes consumed.
//
// Wire format:
//
//	ErrorCode(int16)
//	PartitionIndex(int32)
//	LeaderId(int32)
//	[LeaderEpoch(int32), v7+]
//	ReplicaNodes array (count + int32[])
//	IsrNodes array (count + int32[])
//	[OfflineReplicas array (count + int32[]), v5+]
func skipPartition(data []byte, version int16) (int, error) {
	pos := 0

	// ErrorCode (int16)
	if pos+2 > len(data) {
		return 0, fmt.Errorf("%w: missing partition ErrorCode", ErrMetadataTooShort)
	}
	pos += 2

	// PartitionIndex (int32)
	if pos+4 > len(data) {
		return 0, fmt.Errorf("%w: missing PartitionIndex", ErrMetadataTooShort)
	}
	pos += 4

	// LeaderId (int32)
	if pos+4 > len(data) {
		return 0, fmt.Errorf("%w: missing LeaderId", ErrMetadataTooShort)
	}
	pos += 4

	// LeaderEpoch (int32, v7+)
	if version >= 7 {
		if pos+4 > len(data) {
			return 0, fmt.Errorf("%w: missing LeaderEpoch", ErrMetadataTooShort)
		}
		pos += 4
	}

	// ReplicaNodes array
	bytesRead, err := skipInt32Array(data[pos:])
	if err != nil {
		return 0, fmt.Errorf("ReplicaNodes: %w", err)
	}
	pos += bytesRead

	// IsrNodes array
	bytesRead, err = skipInt32Array(data[pos:])
	if err != nil {
		return 0, fmt.Errorf("IsrNodes: %w", err)
	}
	pos += bytesRead

	// OfflineReplicas array (v5+)
	if version >= 5 {
		bytesRead, err = skipInt32Array(data[pos:])
		if err != nil {
			return 0, fmt.Errorf("OfflineReplicas: %w", err)
		}
		pos += bytesRead
	}

	return pos, nil
}

// skipInt32Array skips past a Kafka int32 array (count prefix + N int32 values).
// Returns the number of bytes consumed.
func skipInt32Array(data []byte) (int, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("%w: missing array length", ErrMetadataTooShort)
	}
	count := int32(binary.BigEndian.Uint32(data[0:4]))
	if count < 0 {
		return 0, fmt.Errorf("negative array length %d", count)
	}
	elemSize := 4 + 4*int(count)
	if len(data) < elemSize {
		return 0, fmt.Errorf("%w: truncated array (need %d bytes, have %d)",
			ErrMetadataTooShort, elemSize, len(data))
	}
	return elemSize, nil
}

// buildSyntheticResponse constructs the final synthetic MetadataResponse
// from the parsed components.
func buildSyntheticResponse(
	correlationID int32,
	bodyBeforeBrokers []byte,
	primaryBrokers, secondaryBrokers []Broker,
	interBrokerBytes []byte,
	primaryTopics, secondaryTopics []topicEntry,
	proxyHost string, proxyPort int32,
	version int16,
) ([]byte, error) {
	// ── Merge brokers: deduplicate by NodeID, primary wins ─────────
	mergedBrokers := mergeBrokers(primaryBrokers, secondaryBrokers)

	// ── Merge topics: primary preferred for duplicates ─────────────
	mergedTopics := mergeTopics(primaryTopics, secondaryTopics)

	// ── Build broker array bytes (all rewritten) ───────────────────
	var brokerBuf bytes.Buffer
	if err := binary.Write(&brokerBuf, binary.BigEndian, int32(len(mergedBrokers))); err != nil {
		return nil, fmt.Errorf("writing broker array length: %w", err)
	}
	for _, b := range mergedBrokers {
		// NodeId (unchanged)
		if err := binary.Write(&brokerBuf, binary.BigEndian, b.NodeID); err != nil {
			return nil, fmt.Errorf("writing NodeId: %w", err)
		}
		// Host (rewritten)
		hostBytes := []byte(proxyHost)
		if err := binary.Write(&brokerBuf, binary.BigEndian, int16(len(hostBytes))); err != nil {
			return nil, fmt.Errorf("writing Host length: %w", err)
		}
		brokerBuf.Write(hostBytes)
		// Port (rewritten)
		if err := binary.Write(&brokerBuf, binary.BigEndian, proxyPort); err != nil {
			return nil, fmt.Errorf("writing Port: %w", err)
		}
		// Rack (preserved if set, v1+)
		if version >= 1 {
			if b.RackIsSet {
				rackBytes := []byte(b.Rack)
				if err := binary.Write(&brokerBuf, binary.BigEndian, int16(len(rackBytes))); err != nil {
					return nil, fmt.Errorf("writing Rack length: %w", err)
				}
				brokerBuf.Write(rackBytes)
			} else {
				if err := binary.Write(&brokerBuf, binary.BigEndian, int16(-1)); err != nil {
					return nil, fmt.Errorf("writing null Rack: %w", err)
				}
			}
		}
	}

	// ── Build topic array bytes ────────────────────────────────────
	var topicBuf bytes.Buffer
	if err := binary.Write(&topicBuf, binary.BigEndian, int32(len(mergedTopics))); err != nil {
		return nil, fmt.Errorf("writing topic array length: %w", err)
	}
	for _, t := range mergedTopics {
		topicBuf.Write(t.RawBytes)
	}

	// ── Assemble the response frame ────────────────────────────────
	//
	// Frame layout:
	//   [4] MessageLength (excludes this field)
	//   [4] CorrelationID
	//   [bodyBeforeBrokers] [brokerArray] [interBrokerBytes] [topicArray]
	//
	newBodyLen := len(bodyBeforeBrokers) + brokerBuf.Len() + len(interBrokerBytes) + topicBuf.Len()
	sizeFieldValue := 4 + newBodyLen
	newFrameLen := 4 + sizeFieldValue

	if newFrameLen > MaxMetadataResponseSize {
		return nil, ErrMetadataResponseTooLarge
	}

	result := make([]byte, newFrameLen)

	// Size prefix
	binary.BigEndian.PutUint32(result[0:4], uint32(sizeFieldValue))
	// CorrelationID
	binary.BigEndian.PutUint32(result[4:8], uint32(correlationID))
	// Body
	offset := 8
	copy(result[offset:], bodyBeforeBrokers)
	offset += len(bodyBeforeBrokers)
	copy(result[offset:], brokerBuf.Bytes())
	offset += brokerBuf.Len()
	copy(result[offset:], interBrokerBytes)
	offset += len(interBrokerBytes)
	copy(result[offset:], topicBuf.Bytes())

	// Validate the frame size is consistent
	if int32(binary.BigEndian.Uint32(result[0:4])) != int32(len(result)-4) {
		return nil, fmt.Errorf("internal error: frame size mismatch")
	}
	return result, nil
}

// mergeBrokers merges two broker lists, deduplicating by NodeID.
// Brokers from primaryBrokers take precedence; secondary brokers with
// NodeIDs not in primary are appended.
func mergeBrokers(primary, secondary []Broker) []Broker {
	seen := make(map[int32]bool, len(primary))
	merged := make([]Broker, 0, len(primary)+len(secondary))

	for _, b := range primary {
		seen[b.NodeID] = true
		merged = append(merged, b)
	}
	for _, b := range secondary {
		if !seen[b.NodeID] {
			seen[b.NodeID] = true
			merged = append(merged, b)
		}
	}
	return merged
}

// mergeTopics merges two topic lists. Topics present in both lists use
// the primary's metadata; topics only in one list are appended.
func mergeTopics(primary, secondary []topicEntry) []topicEntry {
	if len(primary) == 0 && len(secondary) == 0 {
		return nil
	}

	seen := make(map[string]bool, len(primary))
	merged := make([]topicEntry, 0, len(primary)+len(secondary))

	for _, t := range primary {
		seen[t.Name] = true
		merged = append(merged, t)
	}
	for _, t := range secondary {
		if !seen[t.Name] {
			seen[t.Name] = true
			merged = append(merged, t)
		}
	}
	return merged
}

// makeCopy returns a copy of b.
func makeCopy(b []byte) []byte {
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
