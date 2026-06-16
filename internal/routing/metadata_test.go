package routing

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

// ── Helpers ───────────────────────────────────────────────────────────

func makeResponseFrame(correlationID int32, body []byte) []byte {
	frame := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(body)))
	binary.BigEndian.PutUint32(frame[4:8], uint32(correlationID))
	copy(frame[8:], body)
	return frame
}

func makeBrokerBytes(nodeID int32, host string, port int32, version int16) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, nodeID)
	hostBytes := []byte(host)
	binary.Write(&buf, binary.BigEndian, int16(len(hostBytes)))
	buf.Write(hostBytes)
	binary.Write(&buf, binary.BigEndian, port)
	if version >= 1 {
		binary.Write(&buf, binary.BigEndian, int16(-1))
	}
	return buf.Bytes()
}

func makeBrokerBytesWithRack(nodeID int32, host string, port int32, rack string) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, nodeID)
	hostBytes := []byte(host)
	binary.Write(&buf, binary.BigEndian, int16(len(hostBytes)))
	buf.Write(hostBytes)
	binary.Write(&buf, binary.BigEndian, port)
	rackBytes := []byte(rack)
	binary.Write(&buf, binary.BigEndian, int16(len(rackBytes)))
	buf.Write(rackBytes)
	return buf.Bytes()
}

// makeBody builds a MetadataResponse body. brokerCount is the number of
// broker entries; brokerBytes is the concatenated raw broker bytes.
func makeBody(version int16, brokerCount int, brokerBytes []byte) []byte {
	var buf bytes.Buffer
	if version >= 3 {
		binary.Write(&buf, binary.BigEndian, int32(42))
	}
	binary.Write(&buf, binary.BigEndian, int32(brokerCount))
	buf.Write(brokerBytes)
	if version >= 2 {
		cid := []byte("test-cluster-id")
		binary.Write(&buf, binary.BigEndian, int16(len(cid)))
		buf.Write(cid)
	}
	if version >= 1 {
		binary.Write(&buf, binary.BigEndian, int32(0))
	}
	binary.Write(&buf, binary.BigEndian, int32(0)) // 0 topics
	return buf.Bytes()
}

// ── Tests ─────────────────────────────────────────────────────────────

func TestRewrite_v0_SingleBroker(t *testing.T) {
	b := makeBrokerBytes(1, "broker1.example.com", 9092, 0)
	body := makeBody(0, 1, b)
	raw := makeResponseFrame(42, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}

	size := int32(binary.BigEndian.Uint32(resp[0:4]))
	corrID := int32(binary.BigEndian.Uint32(resp[4:8]))
	if corrID != 42 {
		t.Errorf("CorrelationID = %d, want 42", corrID)
	}
	if size != int32(len(resp)-4) {
		t.Errorf("Size = %d, want %d (frame len=%d)", size, len(resp)-4, len(resp))
	}
}

func TestRewrite_v0_MultipleBrokers(t *testing.T) {
	var all bytes.Buffer
	all.Write(makeBrokerBytes(0, "broker0.kafka.local", 9092, 0))
	all.Write(makeBrokerBytes(1, "broker1.kafka.local", 9093, 0))
	all.Write(makeBrokerBytes(2, "broker2.kafka.local", 9094, 0))

	body := makeBody(0, 3, all.Bytes())
	raw := makeResponseFrame(77, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}

	respBody := resp[8:]
	brokerCount := int32(binary.BigEndian.Uint32(respBody[0:4]))
	if brokerCount != 3 {
		t.Errorf("broker count = %d, want 3", brokerCount)
	}
	if !bytes.Contains(resp, []byte("proxy.local")) {
		t.Error("rewritten response does not contain proxy host")
	}
	if bytes.Contains(resp, []byte("broker0.kafka.local")) {
		t.Error("rewritten response still contains original broker host")
	}
}

func TestRewrite_v1_WithRack(t *testing.T) {
	b := makeBrokerBytesWithRack(5, "broker5.aws.kafka", 9092, "us-east-1a")
	body := makeBody(1, 1, b)
	raw := makeResponseFrame(100, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 1)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse v1: %v", err)
	}
	if !bytes.Contains(resp, []byte("us-east-1a")) {
		t.Error("rack not preserved")
	}
	if !bytes.Contains(resp, []byte("proxy.local")) {
		t.Error("host not rewritten")
	}
}

func TestRewrite_v3_WithThrottleTime(t *testing.T) {
	b := makeBrokerBytesWithRack(10, "broker10.kafka", 9092, "rack-a")
	body := makeBody(3, 1, b)
	raw := makeResponseFrame(200, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.v3.test", 9092, 3)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse v3: %v", err)
	}

	respBody := resp[8:]
	throttleMs := int32(binary.BigEndian.Uint32(respBody[0:4]))
	if throttleMs != 42 {
		t.Errorf("ThrottleTimeMs = %d, want 42", throttleMs)
	}
	if !bytes.Contains(resp, []byte("proxy.v3.test")) {
		t.Error("host not rewritten")
	}
}

func TestRewrite_v12(t *testing.T) {
	b := makeBrokerBytesWithRack(99, "kafka99.internal", 9092, "dc1")
	body := makeBody(12, 1, b)
	raw := makeResponseFrame(999, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.internal", 9092, 12)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse v12: %v", err)
	}
	if !bytes.Contains(resp, []byte("proxy.internal")) {
		t.Error("host not rewritten for v12")
	}
}

func TestRewrite_EmptyBrokerArray(t *testing.T) {
	body := makeBody(3, 0, nil)
	raw := makeResponseFrame(42, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 3)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse empty brokers: %v", err)
	}

	respBody := resp[8:]
	brokerCount := int32(binary.BigEndian.Uint32(respBody[4:8]))
	if brokerCount != 0 {
		t.Errorf("broker count = %d, want 0", brokerCount)
	}
	size := int32(binary.BigEndian.Uint32(resp[0:4]))
	if size != int32(len(resp)-4) {
		t.Errorf("Size = %d, want %d", size, len(resp)-4)
	}
}

func TestRewrite_TruncatedData(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		version int16
	}{
		{"empty", []byte{}, 0},
		{"size only", make([]byte, 4), 0},
		{"no broker array", makeResponseFrame(1, []byte{}), 0},
		{"truncated host", func() []byte {
			b := make([]byte, 12)
			binary.BigEndian.PutUint32(b[0:4], 1)
			binary.BigEndian.PutUint32(b[4:8], 42)
			binary.BigEndian.PutUint16(b[8:10], 100)
			return makeResponseFrame(1, b)
		}(), 0},
		{"truncated rack", func() []byte {
			b := make([]byte, 14)
			binary.BigEndian.PutUint32(b[0:4], 1)
			binary.BigEndian.PutUint32(b[4:8], 1)
			binary.BigEndian.PutUint16(b[8:10], 0)
			binary.BigEndian.PutUint32(b[10:14], 9092)
			return makeResponseFrame(1, b)
		}(), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := RewriteMetadataResponse(tt.data, "proxy.local", 9092, tt.version)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestRewrite_NegativeBrokerCount(t *testing.T) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b[0:4], 0xFFFFFFFF)
	raw := makeResponseFrame(1, b)

	_, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 0)
	if err == nil {
		t.Error("expected error for negative broker count")
	}
}

func TestRewrite_HostLongerThanOriginal(t *testing.T) {
	b := makeBrokerBytes(1, "s", 9092, 0)
	body := makeBody(0, 1, b)
	raw := makeResponseFrame(42, body)

	longHost := "very-long-proxy-hostname.kafka.example.com"
	resp, err := RewriteMetadataResponse(raw, longHost, 9092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}
	if !bytes.Contains(resp, []byte(longHost)) {
		t.Error("long host not found")
	}
	size := int32(binary.BigEndian.Uint32(resp[0:4]))
	if size != int32(len(resp)-4) {
		t.Errorf("Size = %d, want %d", size, len(resp)-4)
	}
}

func TestRewrite_HostShorterThanOriginal(t *testing.T) {
	longHost := "very-long-broker-hostname.region.cloudprovider.example.com"
	b := makeBrokerBytes(1, longHost, 9092, 0)
	body := makeBody(0, 1, b)
	raw := makeResponseFrame(42, body)

	shortHost := "proxy"
	resp, err := RewriteMetadataResponse(raw, shortHost, 9092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}
	if !bytes.Contains(resp, []byte(shortHost)) {
		t.Error("short host not found")
	}
	if bytes.Contains(resp, []byte(longHost)) {
		t.Error("original long host still present")
	}
}

func TestRewrite_NullRackPreserved(t *testing.T) {
	b := makeBrokerBytes(1, "broker1.kafka", 9092, 1)
	body := makeBody(1, 1, b)
	raw := makeResponseFrame(42, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 1)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}

	offset := 4 + 4 + 4 + 4 + 2 + len("proxy.local") + 4
	rackLen := int16(binary.BigEndian.Uint16(resp[offset : offset+2]))
	if rackLen != -1 {
		t.Errorf("rack length = %d, want -1 (null)", rackLen)
	}
}

func TestRewrite_NonNilRackPreserved(t *testing.T) {
	b := makeBrokerBytesWithRack(5, "broker5.kafka", 9092, "us-east-1c")
	body := makeBody(3, 1, b)
	raw := makeResponseFrame(42, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 3)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}
	if !bytes.Contains(resp, []byte("us-east-1c")) {
		t.Error("rack value not preserved")
	}
}

func TestRewrite_ProxyHostAtMaxLength(t *testing.T) {
	b := makeBrokerBytes(1, "short", 9092, 0)
	body := makeBody(0, 1, b)
	raw := makeResponseFrame(42, body)

	maxHost := make([]byte, 32767)
	for i := range maxHost {
		maxHost[i] = 'x'
	}
	resp, err := RewriteMetadataResponse(raw, string(maxHost), 9092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}
	if !bytes.Contains(resp, maxHost) {
		t.Error("max-length host not found")
	}
}

func TestRewrite_ProxyHostExceedsMaxLength(t *testing.T) {
	b := makeBrokerBytes(1, "short", 9092, 0)
	body := makeBody(0, 1, b)
	raw := makeResponseFrame(42, body)

	tooLong := make([]byte, 32768)
	for i := range tooLong {
		tooLong[i] = 'x'
	}
	_, err := RewriteMetadataResponse(raw, string(tooLong), 9092, 0)
	if err == nil {
		t.Error("expected error for host exceeding max length")
	}
}

func TestRewrite_PreservesTopicMetadata(t *testing.T) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(10))
	binary.Write(&body, binary.BigEndian, int32(2))
	body.Write(makeBrokerBytesWithRack(1, "broker1.kafka", 9092, "rack-1"))
	body.Write(makeBrokerBytesWithRack(2, "broker2.kafka", 9093, "rack-2"))

	cid := []byte("my-cluster")
	binary.Write(&body, binary.BigEndian, int16(len(cid)))
	body.Write(cid)
	binary.Write(&body, binary.BigEndian, int32(1))

	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int16(0))
	topicName := []byte("orders")
	binary.Write(&body, binary.BigEndian, int16(len(topicName)))
	body.Write(topicName)
	binary.Write(&body, binary.BigEndian, false)
	binary.Write(&body, binary.BigEndian, int32(2))

	binary.Write(&body, binary.BigEndian, int16(0))
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(1))

	binary.Write(&body, binary.BigEndian, int16(0))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(2))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(2))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(2))

	raw := makeResponseFrame(100, body.Bytes())

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 3)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}

	if !bytes.Contains(resp, topicName) {
		t.Error("topic name not preserved")
	}
	if !bytes.Contains(resp, cid) {
		t.Error("cluster ID not preserved")
	}
	if bytes.Contains(resp, []byte("broker1.kafka")) {
		t.Error("original broker host still present")
	}

	count := bytes.Count(resp, []byte("proxy.local"))
	if count != 2 {
		t.Errorf("'proxy.local' appears %d times, want 2", count)
	}

	size := int32(binary.BigEndian.Uint32(resp[0:4]))
	if size != int32(len(resp)-4) {
		t.Errorf("Size = %d, want %d", size, len(resp)-4)
	}
}

func TestRewrite_PortRewritten(t *testing.T) {
	b := makeBrokerBytes(1, "broker1.kafka", 9093, 0)
	body := makeBody(0, 1, b)
	raw := makeResponseFrame(42, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}

	offset := 4 + 4 + 4 + 4 + 2 + len("proxy.local")
	rewrittenPort := int32(binary.BigEndian.Uint32(resp[offset : offset+4]))
	if rewrittenPort != 9092 {
		t.Errorf("rewritten port = %d, want 9092", rewrittenPort)
	}
}

func TestRewrite_NonStandardProxyPort(t *testing.T) {
	b := makeBrokerBytes(1, "broker1.kafka", 9092, 0)
	body := makeBody(0, 1, b)
	raw := makeResponseFrame(42, body)

	resp, err := RewriteMetadataResponse(raw, "proxy.local", 19092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}

	offset := 4 + 4 + 4 + 4 + 2 + len("proxy.local")
	rewrittenPort := int32(binary.BigEndian.Uint32(resp[offset : offset+4]))
	if rewrittenPort != 19092 {
		t.Errorf("rewritten port = %d, want 19092", rewrittenPort)
	}
}

func TestRewrite_FrameSizeMatchesBody(t *testing.T) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(50))
	binary.Write(&body, binary.BigEndian, int32(3))
	body.Write(makeBrokerBytesWithRack(0, "a.example.com", 9092, "r1"))
	body.Write(makeBrokerBytesWithRack(1, "b.example.com", 9093, "r2"))
	body.Write(makeBrokerBytesWithRack(2, "c.example.com", 9094, "r3"))

	cid := []byte("test")
	binary.Write(&body, binary.BigEndian, int16(len(cid)))
	body.Write(cid)
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(0))

	raw := makeResponseFrame(999, body.Bytes())

	resp, err := RewriteMetadataResponse(raw, "p.local", 9092, 3)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}

	frameSize := int32(binary.BigEndian.Uint32(resp[0:4]))
	if frameSize != int32(len(resp)-4) {
		t.Errorf("frame size %d != body length %d", frameSize, len(resp)-4)
	}
}

func TestParseAndRewrite_RoundTrip(t *testing.T) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(25))
	binary.Write(&body, binary.BigEndian, int32(2))
	body.Write(makeBrokerBytesWithRack(0, "old-broker-0.kafka:9092", 9092, "us-east-1a"))
	body.Write(makeBrokerBytesWithRack(1, "old-broker-1.kafka:9093", 9093, "us-east-1b"))

	cid := []byte("prod-cluster")
	binary.Write(&body, binary.BigEndian, int16(len(cid)))
	body.Write(cid)
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(1))

	topicName := []byte("events")
	binary.Write(&body, binary.BigEndian, int16(0))
	binary.Write(&body, binary.BigEndian, int16(len(topicName)))
	body.Write(topicName)
	binary.Write(&body, binary.BigEndian, false)
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int16(0))
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(0))

	parsed, err := ParseMetadataResponseBody(body.Bytes(), 3)
	if err != nil {
		t.Fatalf("ParseMetadataResponseBody: %v", err)
	}
	parsed.CorrelationID = 1234

	if len(parsed.Brokers) != 2 {
		t.Fatalf("parsed %d brokers, want 2", len(parsed.Brokers))
	}
	if parsed.Brokers[0].Host != "old-broker-0.kafka:9092" {
		t.Errorf("broker[0].Host = %q", parsed.Brokers[0].Host)
	}
	if parsed.Brokers[0].Port != 9092 {
		t.Errorf("broker[0].Port = %d", parsed.Brokers[0].Port)
	}
	if parsed.Brokers[0].Rack != "us-east-1a" {
		t.Errorf("broker[0].Rack = %q", parsed.Brokers[0].Rack)
	}
	if !parsed.Brokers[0].RackIsSet {
		t.Error("broker[0].RackIsSet should be true")
	}

	result, err := RewriteBrokers(parsed, "new-proxy.local", 9092, 3)
	if err != nil {
		t.Fatalf("RewriteBrokers: %v", err)
	}

	if !bytes.Contains(result, []byte("new-proxy.local")) {
		t.Error("new proxy host not present")
	}
	if !bytes.Contains(result, []byte("us-east-1a")) {
		t.Error("rack us-east-1a not preserved")
	}
	if !bytes.Contains(result, []byte("us-east-1b")) {
		t.Error("rack us-east-1b not preserved")
	}
	if !bytes.Contains(result, []byte("events")) {
		t.Error("topic 'events' not preserved")
	}
	if bytes.Contains(result, []byte("old-broker-0")) {
		t.Error("original broker host still present")
	}

	frameSize := int32(binary.BigEndian.Uint32(result[0:4]))
	if frameSize != int32(len(result)-4) {
		t.Errorf("frame size %d != body length %d", frameSize, len(result)-4)
	}
}

func TestRewrite_SingleBrokerNoTopics(t *testing.T) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(1))
	body.Write(makeBrokerBytes(0, "only-broker.kafka", 9092, 0))
	binary.Write(&body, binary.BigEndian, int32(0))

	raw := makeResponseFrame(7, body.Bytes())
	resp, err := RewriteMetadataResponse(raw, "proxy", 9092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}
	if !bytes.Contains(resp, []byte("proxy")) {
		t.Error("proxy host not found")
	}
}

func TestRewrite_BrokerWithEmptyHost(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(1))
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int16(0))
	binary.Write(&buf, binary.BigEndian, int32(9092))

	raw := makeResponseFrame(1, buf.Bytes())
	resp, err := RewriteMetadataResponse(raw, "proxy", 9092, 0)
	if err != nil {
		t.Fatalf("RewriteMetadataResponse: %v", err)
	}
	if !bytes.Contains(resp, []byte("proxy")) {
		t.Error("proxy host not found")
	}
}

func TestParse_MetadataAPIKey(t *testing.T) {
	if protocol.APIKeyMetadata != 3 {
		t.Errorf("APIKeyMetadata = %d, want 3", protocol.APIKeyMetadata)
	}
}

func TestParse_RackSetCorrectly(t *testing.T) {
	// v0: no rack
	var buf0 bytes.Buffer
	binary.Write(&buf0, binary.BigEndian, int32(1))
	binary.Write(&buf0, binary.BigEndian, int32(42))
	binary.Write(&buf0, binary.BigEndian, int16(4))
	buf0.WriteString("test")
	binary.Write(&buf0, binary.BigEndian, int32(9092))

	resp0, err := ParseMetadataResponseBody(buf0.Bytes(), 0)
	if err != nil {
		t.Fatalf("ParseMetadataResponseBody v0: %v", err)
	}
	if resp0.Brokers[0].RackIsSet {
		t.Error("v0 broker should have RackIsSet=false")
	}

	// v1: null rack
	var buf1 bytes.Buffer
	binary.Write(&buf1, binary.BigEndian, int32(1))
	binary.Write(&buf1, binary.BigEndian, int32(42))
	binary.Write(&buf1, binary.BigEndian, int16(4))
	buf1.WriteString("test")
	binary.Write(&buf1, binary.BigEndian, int32(9092))
	binary.Write(&buf1, binary.BigEndian, int16(-1))

	resp1, err := ParseMetadataResponseBody(buf1.Bytes(), 1)
	if err != nil {
		t.Fatalf("ParseMetadataResponseBody v1: %v", err)
	}
	if resp1.Brokers[0].RackIsSet {
		t.Error("v1 broker with null rack should have RackIsSet=false")
	}

	// v1: set rack
	var buf2 bytes.Buffer
	binary.Write(&buf2, binary.BigEndian, int32(1))
	binary.Write(&buf2, binary.BigEndian, int32(42))
	binary.Write(&buf2, binary.BigEndian, int16(4))
	buf2.WriteString("test")
	binary.Write(&buf2, binary.BigEndian, int32(9092))
	rackVal := []byte("dc1")
	binary.Write(&buf2, binary.BigEndian, int16(len(rackVal)))
	buf2.Write(rackVal)

	resp2, err := ParseMetadataResponseBody(buf2.Bytes(), 1)
	if err != nil {
		t.Fatalf("ParseMetadataResponseBody v1: %v", err)
	}
	if !resp2.Brokers[0].RackIsSet {
		t.Error("v1 broker with rack should have RackIsSet=true")
	}
	if resp2.Brokers[0].Rack != "dc1" {
		t.Errorf("Rack = %q, want %q", resp2.Brokers[0].Rack, "dc1")
	}
}

// ── Benchmarks ────────────────────────────────────────────────────────

func BenchmarkRewriteMetadataResponse_3Brokers(b *testing.B) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(10))
	binary.Write(&body, binary.BigEndian, int32(3))
	body.Write(makeBrokerBytesWithRack(0, "broker0.kafka.local", 9092, "rack0"))
	body.Write(makeBrokerBytesWithRack(1, "broker1.kafka.local", 9093, "rack1"))
	body.Write(makeBrokerBytesWithRack(2, "broker2.kafka.local", 9094, "rack2"))
	cid := []byte("test-cluster")
	binary.Write(&body, binary.BigEndian, int16(len(cid)))
	body.Write(cid)
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(0))

	raw := makeResponseFrame(42, body.Bytes())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 3)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRewriteMetadataResponse_10Brokers(b *testing.B) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(10))
	binary.Write(&body, binary.BigEndian, int32(10))
	for i := int32(0); i < 10; i++ {
		body.Write(makeBrokerBytesWithRack(i, "broker.kafka.local", int32(9092)+i, "rack"))
	}
	cid := []byte("test-cluster")
	binary.Write(&body, binary.BigEndian, int16(len(cid)))
	body.Write(cid)
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(0))

	raw := makeResponseFrame(42, body.Bytes())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := RewriteMetadataResponse(raw, "proxy.local", 9092, 3)
		if err != nil {
			b.Fatal(err)
		}
	}
}
