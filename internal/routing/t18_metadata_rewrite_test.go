package routing

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestMetadataRewrite is the end-to-end test required by T18.
// It builds a MetadataResponse with multiple brokers, rewrites their
// host:port addresses, and verifies the frame size header is correct.
func TestMetadataRewrite(t *testing.T) {
	// ── Build a MetadataResponse v3 with 3 brokers + topic metadata ──
	var body bytes.Buffer

	// ThrottleTimeMs (v3+)
	binary.Write(&body, binary.BigEndian, int32(42))

	// 3 brokers with different rack info
	binary.Write(&body, binary.BigEndian, int32(3))
	body.Write(makeBrokerBytesWithRack(0, "b0.us-east-1.aws.confluent.cloud:9092", 9092, "us-east-1a"))
	body.Write(makeBrokerBytesWithRack(1, "b1.us-east-1.aws.confluent.cloud:9092", 9092, "us-east-1b"))
	body.Write(makeBrokerBytesWithRack(2, "b2.us-east-1.aws.confluent.cloud:9092", 9092, "us-east-1c"))

	// ClusterId
	cid := []byte("prod-cluster-42")
	binary.Write(&body, binary.BigEndian, int16(len(cid)))
	body.Write(cid)

	// ControllerId
	binary.Write(&body, binary.BigEndian, int32(0))

	// 1 topic with 1 partition
	binary.Write(&body, binary.BigEndian, int32(1))
	topicName := []byte("orders")
	binary.Write(&body, binary.BigEndian, int16(0))  // error code
	binary.Write(&body, binary.BigEndian, int16(len(topicName)))
	body.Write(topicName)
	binary.Write(&body, binary.BigEndian, byte(0))   // is_internal (bool, 1 byte)
	binary.Write(&body, binary.BigEndian, int32(1))  // 1 partition
	binary.Write(&body, binary.BigEndian, int16(0))  // error code
	binary.Write(&body, binary.BigEndian, int32(0))  // partition index
	binary.Write(&body, binary.BigEndian, int32(0))  // leader
	binary.Write(&body, binary.BigEndian, int32(1))  // replicas: [0]
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(1))  // isr: [0]
	binary.Write(&body, binary.BigEndian, int32(0))

	correlationID := int32(12345)
	raw := makeResponseFrame(correlationID, body.Bytes())
	originalLen := len(raw)

	// ── Rewrite ──
	proxyHost := ".proxy..com"
	proxyPort := int32(9092)
	version := int16(3)

	result, err := RewriteMetadataResponse(raw, proxyHost, proxyPort, version)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}

	// ── Verify frame size header ─────────────────────────────────────
	newSize := int32(binary.BigEndian.Uint32(result[0:4]))
	actualBodyLen := len(result) - 4
	if newSize != int32(actualBodyLen) {
		t.Errorf("frame size header = %d, but actual body length = %d (frame total=%d)",
			newSize, actualBodyLen, len(result))
	}

	// Frame size should differ from original by the host length delta.
	// With 3 brokers where hosts change from 41 chars to 31 chars,
	// the frame length adjusts accordingly.
	if len(result) == originalLen {
		t.Errorf("rewritten frame length (%d) should differ from original (%d)", len(result), originalLen)
	}

	// ── Verify correlation ID preserved ──────────────────────────────
	gotCorrID := int32(binary.BigEndian.Uint32(result[4:8]))
	if gotCorrID != correlationID {
		t.Errorf("correlation ID = %d, want %d", gotCorrID, correlationID)
	}

	// ── Verify all broker hosts rewritten ────────────────────────────
	if !bytes.Contains(result, []byte(proxyHost)) {
		t.Error("rewritten response does not contain proxy host")
	}
	for _, orig := range []string{"b0.us-east-1", "b1.us-east-1", "b2.us-east-1"} {
		if bytes.Contains(result, []byte(orig)) {
			t.Errorf("original broker host %q still present in rewritten response", orig)
		}
	}

	// ── Verify racks preserved ───────────────────────────────────────
	for _, rack := range []string{"us-east-1a", "us-east-1b", "us-east-1c"} {
		if !bytes.Contains(result, []byte(rack)) {
			t.Errorf("rack %q not preserved", rack)
		}
	}

	// ── Verify topic metadata preserved ──────────────────────────────
	if !bytes.Contains(result, []byte("orders")) {
		t.Error("topic name 'orders' not preserved")
	}
	if !bytes.Contains(result, []byte("prod-cluster-42")) {
		t.Error("cluster ID 'prod-cluster-42' not preserved")
	}

	// ── Re-parse and verify rewritten brokers ────────────────────────
	parsed, err := ParseMetadataResponseBody(result[8:], version)
	if err != nil {
		t.Fatalf("re-parsing rewritten response: %v", err)
	}
	if len(parsed.Brokers) != 3 {
		t.Fatalf("re-parsed %d brokers, want 3", len(parsed.Brokers))
	}
	for i, b := range parsed.Brokers {
		if b.Host != proxyHost {
			t.Errorf("re-parsed broker[%d].Host = %q, want %q", i, b.Host, proxyHost)
		}
		if b.Port != proxyPort {
			t.Errorf("re-parsed broker[%d].Port = %d, want %d", i, b.Port, proxyPort)
		}
	}

	t.Logf("Original: %d bytes, Rewritten: %d bytes, Size header: %d, body: %d ✓",
		originalLen, len(result), newSize, actualBodyLen)
}

// TestMetadataRewrite_FrameSizeRecalculation specifically verifies that
// the 4-byte size prefix is recalculated correctly after broker rewriting.
func TestMetadataRewrite_FrameSizeRecalculation(t *testing.T) {
	tests := []struct {
		name       string
		brokerHost string
		proxyHost  string
	}{
		{"longer proxy host", "b.kafka:9092", ".proxy..com"},
		{"shorter proxy host", "very-long-broker-name.us-east-1.aws.confluent.cloud:9092", "proxy:9092"},
		{"same length hosts", "proxy.local:9092", "proxy.local:9092"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body bytes.Buffer
			binary.Write(&body, binary.BigEndian, int32(0)) // no throttle (v0)

			// 1 broker
			binary.Write(&body, binary.BigEndian, int32(1))
			body.Write(makeBrokerBytes(0, tt.brokerHost, 9092, 0))

			raw := makeResponseFrame(42, body.Bytes())
			rewritten, err := RewriteMetadataResponse(raw, tt.proxyHost, 9092, 0)
			if err != nil {
				t.Fatalf("RewriteMetadataResponse: %v", err)
			}

			// Verify size header
			sizeHeader := int32(binary.BigEndian.Uint32(rewritten[0:4]))
			actualBodyLen := len(rewritten) - 4
			if sizeHeader != int32(actualBodyLen) {
				t.Errorf("size header = %d, body length = %d", sizeHeader, actualBodyLen)
			}
		})
	}
}
