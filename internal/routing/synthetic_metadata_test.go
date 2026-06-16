package routing

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// ── Helpers ───────────────────────────────────────────────────────────

// makeMetaFrame builds a complete MetadataResponse frame (size + corrID + body).
func makeMetaFrame(correlationID int32, body []byte) []byte {
	frame := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(body)))
	binary.BigEndian.PutUint32(frame[4:8], uint32(correlationID))
	copy(frame[8:], body)
	return frame
}

// makeFullBody builds a MetadataResponse body with brokers, inter-broker
// fields, and topic data (pre-built).
// useVersion controls throttle time (v3+), cluster ID (v2+), controller ID (v1+),
// cluster authorized ops (v8+).
func makeFullBody(useVersion int16, brokerBytes []byte, brokerCount int, topicBytes []byte, topicCount int) []byte {
	var buf bytes.Buffer
	if useVersion >= 3 {
		binary.Write(&buf, binary.BigEndian, int32(100)) // throttle time
	}
	binary.Write(&buf, binary.BigEndian, int32(brokerCount))
	buf.Write(brokerBytes)
	if useVersion >= 2 {
		cid := []byte("test-cluster")
		binary.Write(&buf, binary.BigEndian, int16(len(cid)))
		buf.Write(cid)
	}
	if useVersion >= 1 {
		binary.Write(&buf, binary.BigEndian, int32(7)) // controller ID
	}
	if useVersion >= 8 {
		binary.Write(&buf, binary.BigEndian, int32(0)) // cluster authorized ops
	}
	binary.Write(&buf, binary.BigEndian, int32(topicCount))
	buf.Write(topicBytes)
	return buf.Bytes()
}

// makeTopicBytes constructs topic entries for N topics with P partitions each.
// Topic names are "topic-0", "topic-1", etc.
func makeTopicBytes(numTopics, numPartitions int, useVersion int16) []byte {
	var buf bytes.Buffer
	for t := 0; t < numTopics; t++ {
		name := "topic-" + string(rune('0'+t))
		buf.Write(makeTopicEntry(name, numPartitions, useVersion))
	}
	return buf.Bytes()
}

// makeTopicEntry builds a single topic entry with the given name and partition count.
func makeTopicEntry(name string, numPartitions int, useVersion int16) []byte {
	var buf bytes.Buffer
	// ErrorCode
	binary.Write(&buf, binary.BigEndian, int16(0))
	// TopicName
	nameBytes := []byte(name)
	binary.Write(&buf, binary.BigEndian, int16(len(nameBytes)))
	buf.Write(nameBytes)
	// IsInternal (v1+)
	if useVersion >= 1 {
		binary.Write(&buf, binary.BigEndian, false)
	}
	// Partition count
	binary.Write(&buf, binary.BigEndian, int32(numPartitions))
	for p := 0; p < numPartitions; p++ {
		buf.Write(makePartitionEntry(int32(p)))
	}
	// TopicAuthorizedOperations (v8+)
	if useVersion >= 8 {
		binary.Write(&buf, binary.BigEndian, int32(0))
	}
	return buf.Bytes()
}

// makePartitionEntry builds a single partition entry.
func makePartitionEntry(partID int32) []byte {
	var buf bytes.Buffer
	// ErrorCode
	binary.Write(&buf, binary.BigEndian, int16(0))
	// PartitionIndex
	binary.Write(&buf, binary.BigEndian, partID)
	// LeaderId
	binary.Write(&buf, binary.BigEndian, int32(partID+1))
	// LeaderEpoch (v7+) — skip for simplicity
	// ReplicaNodes (empty array)
	binary.Write(&buf, binary.BigEndian, int32(0))
	// IsrNodes (empty array)
	binary.Write(&buf, binary.BigEndian, int32(0))
	// OfflineReplicas (v5+) — skip
	return buf.Bytes()
}

// ── Tests ─────────────────────────────────────────────────────────────

func TestSynthesize_BasicMerge_v3(t *testing.T) {
	version := int16(3)

	// Primary: brokers [0, 1], topics [topic-0, topic-1]
	priBrokers := make([]byte, 0)
	priBrokers = append(priBrokers, makeBrokerBytesWithRack(0, "b0.kafka.primary", 9092, "rack-a")...)
	priBrokers = append(priBrokers, makeBrokerBytesWithRack(1, "b1.kafka.primary", 9093, "rack-b")...)
	priTopics := makeTopicBytes(2, 1, version)
	priBody := makeFullBody(version, priBrokers, 2, priTopics, 2)
	priFrame := makeMetaFrame(42, priBody)

	// Secondary: brokers [1, 2], topics [topic-1, topic-2]
	secBrokers := make([]byte, 0)
	secBrokers = append(secBrokers, makeBrokerBytesWithRack(1, "b1.kafka.secondary", 9093, "rack-b2")...)
	secBrokers = append(secBrokers, makeBrokerBytesWithRack(2, "b2.kafka.secondary", 9094, "rack-c")...)
	secTopics := makeTopicBytes(3, 1, version) // topic-0, topic-1, topic-2
	secBody := makeFullBody(version, secBrokers, 2, secTopics, 3)
	secFrame := makeMetaFrame(99, secBody)

	result, err := SynthesizeMetadataResponse(priFrame, secFrame, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	// Verify frame size
	frameSize := int32(binary.BigEndian.Uint32(result[0:4]))
	if frameSize != int32(len(result)-4) {
		t.Errorf("frame size %d != body length %d", frameSize, len(result)-4)
	}

	// Verify correlation ID (from primary)
	corrID := int32(binary.BigEndian.Uint32(result[4:8]))
	if corrID != 42 {
		t.Errorf("correlation ID = %d, want 42", corrID)
	}

	// No original broker hosts should remain
	if bytes.Contains(result, []byte("kafka.primary")) {
		t.Error("primary broker host still present")
	}
	if bytes.Contains(result, []byte("kafka.secondary")) {
		t.Error("secondary broker host still present")
	}

	// Proxy host should appear
	if !bytes.Contains(result, []byte("proxy.local")) {
		t.Error("proxy host not found in response")
	}

	// All 3 merged topics should be present (topic-0, topic-1, topic-2)
	for _, name := range []string{"topic-0", "topic-1", "topic-2"} {
		if !bytes.Contains(result, []byte(name)) {
			t.Errorf("topic %q not found in merged response", name)
		}
	}

	// Racks should be preserved
	if !bytes.Contains(result, []byte("rack-a")) {
		t.Error("rack-a not preserved")
	}
	if !bytes.Contains(result, []byte("rack-c")) {
		t.Error("rack-c not preserved")
	}

	// Throttle time preserved
	respBody := result[8:]
	throttleMs := int32(binary.BigEndian.Uint32(respBody[0:4]))
	if throttleMs != 100 {
		t.Errorf("ThrottleTimeMs = %d, want 100", throttleMs)
	}
}

func TestSynthesize_SecondaryNil(t *testing.T) {
	version := int16(0)

	priBrokers := makeBrokerBytes(0, "broker0.kafka", 9092, 0)
	priBody := makeFullBody(version, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(42, priBody)

	result, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse with secondary=nil: %v", err)
	}

	// Should have the proxy host rewritten
	if !bytes.Contains(result, []byte("proxy.local")) {
		t.Error("proxy host not found")
	}
	if bytes.Contains(result, []byte("broker0.kafka")) {
		t.Error("original broker host still present")
	}

	// Frame size valid
	frameSize := int32(binary.BigEndian.Uint32(result[0:4]))
	if frameSize != int32(len(result)-4) {
		t.Errorf("frame size %d != body length %d", frameSize, len(result)-4)
	}
}

func TestSynthesize_SecondaryUnparseable(t *testing.T) {
	version := int16(0)

	priBrokers := makeBrokerBytes(0, "broker0.kafka", 9092, 0)
	priBody := makeFullBody(version, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(42, priBody)

	// Garbage secondary response
	garbageSec := []byte{0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0xFF, 0x99}

	result, err := SynthesizeMetadataResponse(priFrame, garbageSec, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse with unparseable secondary: %v", err)
	}

	// Should fall back to primary-only
	if !bytes.Contains(result, []byte("proxy.local")) {
		t.Error("proxy host not found")
	}
}

func TestSynthesize_BrokerDeduplication(t *testing.T) {
	version := int16(0)

	// Both primary and secondary have the same broker NodeID=0
	priBrokers := makeBrokerBytes(0, "same-broker-pri.kafka", 9092, 0)
	priBody := makeFullBody(version, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(1, priBody)

	secBrokers := makeBrokerBytes(0, "same-broker-sec.kafka", 9093, 0)
	secBody := makeFullBody(version, secBrokers, 1, nil, 0)
	secFrame := makeMetaFrame(2, secBody)

	result, err := SynthesizeMetadataResponse(priFrame, secFrame, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	// Should only have 1 broker (deduplicated)
	respBody := result[8:]
	brokerCount := int32(binary.BigEndian.Uint32(respBody[0:4]))
	if brokerCount != 1 {
		t.Errorf("broker count = %d, want 1 (deduped)", brokerCount)
	}

	// Should appear only once
	count := bytes.Count(result, []byte("proxy.local"))
	if count != 1 {
		t.Errorf("'proxy.local' appears %d times in rewritten response, want 1", count)
	}
}

func TestSynthesize_TopicMergePrimaryPreferred(t *testing.T) {
	version := int16(0)

	// Primary: topic-0 with 1 partition, topic-1 with 1 partition
	priTopics := makeTopicBytes(2, 1, version)
	priBody := makeFullBody(version, nil, 0, priTopics, 2)
	priFrame := makeMetaFrame(42, priBody)

	// Secondary: topic-0 with 5 partitions (different), topic-2 (new)
	secTopics := make([]byte, 0)
	secTopics = append(secTopics, makeTopicEntry("topic-0", 5, version)...)
	secTopics = append(secTopics, makeTopicEntry("topic-2", 1, version)...)
	secBody := makeFullBody(version, nil, 0, secTopics, 2)
	secFrame := makeMetaFrame(99, secBody)

	result, err := SynthesizeMetadataResponse(priFrame, secFrame, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	respBody := result[8:]
	// Synthesized response body layout:
	//   [bodyBeforeBrokers][brokerArray(count+entries)][interBroker][topicArray]
	// For v0 with 0 brokers: bodyBeforeBrokers=0, brokerArray=4 bytes (count=0), interBroker=0
	// So topic array starts at offset 4 (past the 4-byte broker count).
	topicArrayOffset := 4
	topicCount := int32(binary.BigEndian.Uint32(respBody[topicArrayOffset : topicArrayOffset+4]))
	if topicCount != 3 {
		t.Errorf("topic count = %d, want 3", topicCount)
	}

	// topic-0 should have primary's partition count (1), not secondary's (5)
	// Check partition count for the first topic (topic-0)
	offset := topicArrayOffset + 4
	offset += 2  // error code
	nameLen := int16(binary.BigEndian.Uint16(respBody[offset : offset+2]))
	offset += 2 + int(nameLen) // skip name
	partCount := int32(binary.BigEndian.Uint32(respBody[offset : offset+4]))
	if partCount != 1 {
		t.Errorf("topic-0 partition count = %d, want 1 (primary preferred)", partCount)
	}
}

func TestSynthesize_v0(t *testing.T) {
	version := int16(0)

	priBrokers := makeBrokerBytes(1, "b1.kafka", 9092, 0)
	priTopics := makeTopicBytes(1, 1, version)
	priBody := makeFullBody(version, priBrokers, 1, priTopics, 1)
	priFrame := makeMetaFrame(10, priBody)

	secBrokers := makeBrokerBytes(2, "b2.kafka", 9093, 0)
	secBody := makeFullBody(version, secBrokers, 1, nil, 0)
	secFrame := makeMetaFrame(20, secBody)

	result, err := SynthesizeMetadataResponse(priFrame, secFrame, "p", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse v0: %v", err)
	}

	if !bytes.Contains(result, []byte("p")) {
		t.Error("proxy host not found in v0 response")
	}
}

func TestSynthesize_v1(t *testing.T) {
	version := int16(1)

	priBrokers := makeBrokerBytesWithRack(1, "b1.kafka", 9092, "r1")
	priBody := makeFullBody(version, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(10, priBody)

	result, err := SynthesizeMetadataResponse(priFrame, nil, "proxy", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse v1: %v", err)
	}

	// Controller ID should be preserved in v1
	if !bytes.Contains(result, []byte("r1")) {
		t.Error("rack not preserved in v1")
	}
}

func TestSynthesize_v12(t *testing.T) {
	version := int16(12)

	priBrokers := makeBrokerBytesWithRack(1, "b1.kafka", 9092, "r1")
	priBody := makeFullBody(version, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(10, priBody)

	result, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.v12", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse v12: %v", err)
	}
	if !bytes.Contains(result, []byte("proxy.v12")) {
		t.Error("proxy host not found in v12")
	}
}

func TestSynthesize_EmptyBrokers(t *testing.T) {
	version := int16(3)

	priBody := makeFullBody(version, nil, 0, makeTopicBytes(1, 1, version), 1)
	priFrame := makeMetaFrame(1, priBody)

	result, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse empty brokers: %v", err)
	}

	frameSize := int32(binary.BigEndian.Uint32(result[0:4]))
	if frameSize != int32(len(result)-4) {
		t.Errorf("frame size %d != body length %d", frameSize, len(result)-4)
	}
}

func TestSynthesize_EmptyTopics(t *testing.T) {
	version := int16(3)

	priBrokers := makeBrokerBytesWithRack(1, "b1.kafka", 9092, "rack")
	priBody := makeFullBody(version, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(1, priBody)

	result, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse empty topics: %v", err)
	}

	frameSize := int32(binary.BigEndian.Uint32(result[0:4]))
	if frameSize != int32(len(result)-4) {
		t.Errorf("frame size %d != body length %d", frameSize, len(result)-4)
	}
}

func TestSynthesize_TopicOnlyInSecondary(t *testing.T) {
	version := int16(0)

	// Primary has no topics
	priBody := makeFullBody(version, nil, 0, nil, 0)
	priFrame := makeMetaFrame(1, priBody)

	// Secondary has topics
	secTopics := makeTopicBytes(2, 1, version)
	secBody := makeFullBody(version, nil, 0, secTopics, 2)
	secFrame := makeMetaFrame(2, secBody)

	result, err := SynthesizeMetadataResponse(priFrame, secFrame, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	// Should have both secondary topics
	for _, name := range []string{"topic-0", "topic-1"} {
		if !bytes.Contains(result, []byte(name)) {
			t.Errorf("topic %q from secondary not found", name)
		}
	}
}

func TestSynthesize_TopicOnlyInPrimary(t *testing.T) {
	version := int16(0)

	priTopics := makeTopicBytes(1, 1, version)
	priBody := makeFullBody(version, nil, 0, priTopics, 1)
	priFrame := makeMetaFrame(1, priBody)

	secBody := makeFullBody(version, nil, 0, nil, 0)
	secFrame := makeMetaFrame(2, secBody)

	result, err := SynthesizeMetadataResponse(priFrame, secFrame, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	if !bytes.Contains(result, []byte("topic-0")) {
		t.Error("topic-0 from primary not found")
	}
}

func TestSynthesize_FrameSizeValid(t *testing.T) {
	version := int16(3)

	priBrokers := make([]byte, 0)
	for i := int32(0); i < 5; i++ {
		priBrokers = append(priBrokers, makeBrokerBytesWithRack(i, "broker.kafka", 9092, "rack")...)
	}
	priBody := makeFullBody(version, priBrokers, 5, makeTopicBytes(3, 2, version), 3)
	priFrame := makeMetaFrame(42, priBody)

	secBrokers := make([]byte, 0)
	for i := int32(3); i < 8; i++ {
		secBrokers = append(secBrokers, makeBrokerBytesWithRack(i, "broker.kafka", 9092, "rack")...)
	}
	secBody := makeFullBody(version, secBrokers, 5, makeTopicBytes(2, 2, version), 2)
	secFrame := makeMetaFrame(99, secBody)

	result, err := SynthesizeMetadataResponse(priFrame, secFrame, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	frameSize := int32(binary.BigEndian.Uint32(result[0:4]))
	if frameSize != int32(len(result)-4) {
		t.Errorf("frame size %d != body length %d", frameSize, len(result)-4)
	}
}

func TestSynthesize_ClusterIDPreserved(t *testing.T) {
	version := int16(3)

	priBrokers := makeBrokerBytesWithRack(1, "b1.kafka", 9092, "rack")
	priBody := makeFullBody(version, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(1, priBody)

	result, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	if !bytes.Contains(result, []byte("test-cluster")) {
		t.Error("cluster ID not preserved")
	}
}

func TestSynthesize_HostTooLong(t *testing.T) {
	priBrokers := makeBrokerBytes(1, "b.kafka", 9092, 0)
	priBody := makeFullBody(0, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(1, priBody)

	tooLong := make([]byte, 32768)
	for i := range tooLong {
		tooLong[i] = 'x'
	}

	_, err := SynthesizeMetadataResponse(priFrame, nil, string(tooLong), 9092, 0)
	if err == nil {
		t.Error("expected error for host exceeding max length")
	}
}

func TestSynthesize_SecondaryNilPreservesTopics(t *testing.T) {
	version := int16(3)

	priBrokers := makeBrokerBytesWithRack(0, "broker0.kafka", 9092, "dc1")
	priTopics := makeTopicBytes(3, 2, version)
	priBody := makeFullBody(version, priBrokers, 1, priTopics, 3)
	priFrame := makeMetaFrame(42, priBody)

	result, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.local", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	for _, name := range []string{"topic-0", "topic-1", "topic-2"} {
		if !bytes.Contains(result, []byte(name)) {
			t.Errorf("topic %q not found when secondary is nil", name)
		}
	}
}

func TestSynthesize_PortRewritten(t *testing.T) {
	version := int16(0)

	priBrokers := makeBrokerBytes(1, "broker1.kafka", 9093, 0)
	priBody := makeFullBody(version, priBrokers, 1, nil, 0)
	priFrame := makeMetaFrame(42, priBody)

	result, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.local", 19092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	offset := 4 + 4 + 4 + 4 + 2 + len("proxy.local")
	rewrittenPort := int32(binary.BigEndian.Uint32(result[offset : offset+4]))
	if rewrittenPort != 19092 {
		t.Errorf("rewritten port = %d, want 19092", rewrittenPort)
	}
}

func TestSynthesize_ComplexMerge(t *testing.T) {
	version := int16(3)

	// Primary: 3 brokers, 2 topics with 3 partitions each
	priBrokers := make([]byte, 0)
	priBrokers = append(priBrokers, makeBrokerBytesWithRack(0, "b0.pri.kafka", 9092, "us-east-1a")...)
	priBrokers = append(priBrokers, makeBrokerBytesWithRack(1, "b1.pri.kafka", 9093, "us-east-1b")...)
	priBrokers = append(priBrokers, makeBrokerBytesWithRack(2, "b2.pri.kafka", 9094, "us-east-1c")...)
	priTopics := makeTopicBytes(2, 3, version)
	priBody := makeFullBody(version, priBrokers, 3, priTopics, 2)
	priFrame := makeMetaFrame(100, priBody)

	// Secondary: 4 brokers (2 overlap), 3 topics (1 overlap)
	secBrokers := make([]byte, 0)
	secBrokers = append(secBrokers, makeBrokerBytesWithRack(1, "b1.sec.kafka", 9093, "us-east-1b")...)
	secBrokers = append(secBrokers, makeBrokerBytesWithRack(2, "b2.sec.kafka", 9094, "us-east-1c")...)
	secBrokers = append(secBrokers, makeBrokerBytesWithRack(3, "b3.sec.kafka", 9095, "us-west-2a")...)
	secBrokers = append(secBrokers, makeBrokerBytesWithRack(4, "b4.sec.kafka", 9096, "us-west-2b")...)
	var secTopics bytes.Buffer
	secTopics.Write(makeTopicEntry("topic-0", 5, version))
	secTopics.Write(makeTopicEntry("topic-2", 2, version))
	secTopics.Write(makeTopicEntry("topic-3", 1, version))
	secBody := makeFullBody(version, secBrokers, 4, secTopics.Bytes(), 3)
	secFrame := makeMetaFrame(200, secBody)

	result, err := SynthesizeMetadataResponse(priFrame, secFrame, "my.proxy.com", 9092, version)
	if err != nil {
		t.Fatalf("SynthesizeMetadataResponse: %v", err)
	}

	// Frame size validation
	frameSize := int32(binary.BigEndian.Uint32(result[0:4]))
	if frameSize != int32(len(result)-4) {
		t.Errorf("frame size %d != body length %d", frameSize, len(result)-4)
	}

	// Correlation ID from primary
	corrID := int32(binary.BigEndian.Uint32(result[4:8]))
	if corrID != 100 {
		t.Errorf("correlation ID = %d, want 100", corrID)
	}

	// No original hosts
	if bytes.Contains(result, []byte("pri.kafka")) || bytes.Contains(result, []byte("sec.kafka")) {
		t.Error("original broker hosts still present")
	}

	// All rewritten to proxy
	if !bytes.Contains(result, []byte("my.proxy.com")) {
		t.Error("proxy host not found")
	}

	// Merged broker count: 5 (0,1,2 from primary + 3,4 from secondary)
	respBody := result[8:]
	brokerCount := int32(binary.BigEndian.Uint32(respBody[4:8]))
	if brokerCount != 5 {
		t.Errorf("broker count = %d, want 5", brokerCount)
	}

	// All 4 topic names should appear
	for _, name := range []string{"topic-0", "topic-1", "topic-2", "topic-3"} {
		if !bytes.Contains(result, []byte(name)) {
			t.Errorf("topic %q not found", name)
		}
	}

	// Racks preserved
	for _, rack := range []string{"us-east-1a", "us-east-1b", "us-east-1c", "us-west-2a", "us-west-2b"} {
		if !bytes.Contains(result, []byte(rack)) {
			t.Errorf("rack %q not preserved", rack)
		}
	}
}

// ── Benchmarks ────────────────────────────────────────────────────────

func BenchmarkSynthesizeMetadataResponse_3Brokers(b *testing.B) {
	version := int16(3)

	priBrokers := make([]byte, 0)
	for i := int32(0); i < 3; i++ {
		priBrokers = append(priBrokers, makeBrokerBytesWithRack(i, "broker.kafka", 9092, "rack")...)
	}
	priBody := makeFullBody(version, priBrokers, 3, makeTopicBytes(2, 1, version), 2)
	priFrame := makeMetaFrame(42, priBody)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.local", 9092, version)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSynthesizeMetadataResponse_10Brokers(b *testing.B) {
	version := int16(3)

	priBrokers := make([]byte, 0)
	for i := int32(0); i < 10; i++ {
		priBrokers = append(priBrokers, makeBrokerBytesWithRack(i, "broker.kafka", 9092, "rack")...)
	}
	priBody := makeFullBody(version, priBrokers, 10, makeTopicBytes(5, 3, version), 5)
	priFrame := makeMetaFrame(42, priBody)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := SynthesizeMetadataResponse(priFrame, nil, "proxy.local", 9092, version)
		if err != nil {
			b.Fatal(err)
		}
	}
}
