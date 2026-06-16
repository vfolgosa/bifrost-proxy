package routing

import (
	"encoding/binary"
	"testing"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

// --- Helpers specific to MetadataTarget tests ---

// buildMetadataRequest builds a valid Kafka Metadata request frame with the
// given correlation ID and client ID. The body is a minimal MetadataRequest v4:
// null topics array + allow_auto_topic_creation=false.
func buildMetadataRequest(correlationID int32, clientID string) []byte {
	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], uint16(protocol.APIKeyMetadata))
	binary.BigEndian.PutUint16(header[2:4], 4) // APIVersion = 4
	binary.BigEndian.PutUint32(header[4:8], uint32(correlationID))

	// Body: null topics (int32=-1) + allow_auto_topic_creation (bool=false)
	body := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00}

	clientIDLen := int16(len(clientID))
	fullLen := 4 + 8 + 2 + len(clientID) + len(body)
	buf := make([]byte, fullLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(fullLen-4))
	copy(buf[4:12], header)
	binary.BigEndian.PutUint16(buf[12:14], uint16(clientIDLen))
	copy(buf[14:], clientID)
	copy(buf[14+len(clientID):], body)
	return buf
}

// buildRequest builds a Kafka request frame with the given API key, version,
// correlation ID, and client ID. Useful for testing non-metadata API key detection.
func buildRequest(apiKey, apiVersion int16, correlationID int32, clientID string) []byte {
	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], uint16(apiKey))
	binary.BigEndian.PutUint16(header[2:4], uint16(apiVersion))
	binary.BigEndian.PutUint32(header[4:8], uint32(correlationID))

	body := []byte{0x00, 0x00, 0x00, 0x00}

	clientIDLen := int16(len(clientID))
	fullLen := 4 + 8 + 2 + len(clientID) + len(body)
	buf := make([]byte, fullLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(fullLen-4))
	copy(buf[4:12], header)
	binary.BigEndian.PutUint16(buf[12:14], uint16(clientIDLen))
	copy(buf[14:], clientID)
	copy(buf[14+len(clientID):], body)
	return buf
}

// activePassiveCfg creates a ClusterConfig in active_passive mode.
func activePassiveCfg(active, primaryBootstrap, secondaryBootstrap string) config.ClusterConfig {
	return config.ClusterConfig{
		Mode:   config.ModeActivePassive,
		Active: active,
		Primary: config.ClusterEndpoint{
			Bootstrap: primaryBootstrap,
		},
		Secondary: config.ClusterEndpoint{
			Bootstrap: secondaryBootstrap,
		},
	}
}

// --- Tests: Valid Metadata requests in active_passive mode ---

func TestMetadataTarget_ActivePrimary(t *testing.T) {
	cfg := activePassiveCfg(
		config.ActivePrimary,
		"pkc-11111.us-east-1.aws.confluent.cloud:9092",
		"pkc-22222.us-east-2.aws.confluent.cloud:9092",
	)
	req := buildMetadataRequest(42, "myclient")

	addr, corrID, err := MetadataTarget(req, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantAddr := "pkc-11111.us-east-1.aws.confluent.cloud:9092"
	if addr != wantAddr {
		t.Errorf("upstreamAddr = %q, want %q", addr, wantAddr)
	}
	if corrID != 42 {
		t.Errorf("correlationID = %d, want 42", corrID)
	}
}

func TestMetadataTarget_ActiveSecondary(t *testing.T) {
	cfg := activePassiveCfg(
		config.ActiveSecondary,
		"pkc-11111.us-east-1.aws.confluent.cloud:9092",
		"pkc-22222.us-east-2.aws.confluent.cloud:9092",
	)
	req := buildMetadataRequest(99, "app-v2")

	addr, corrID, err := MetadataTarget(req, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantAddr := "pkc-22222.us-east-2.aws.confluent.cloud:9092"
	if addr != wantAddr {
		t.Errorf("upstreamAddr = %q, want %q", addr, wantAddr)
	}
	if corrID != 99 {
		t.Errorf("correlationID = %d, want 99", corrID)
	}
}

func TestMetadataTarget_PreservesCorrelationID(t *testing.T) {
	cfg := activePassiveCfg(config.ActivePrimary, "host1:9092", "host2:9092")

	corrIDs := []int32{0, 1, 42, -1, 2147483647, -2147483648}
	for _, want := range corrIDs {
		req := buildMetadataRequest(want, "")
		_, corrID, err := MetadataTarget(req, cfg)
		if err != nil {
			t.Fatalf("corrID=%d: unexpected error: %v", want, err)
		}
		if corrID != want {
			t.Errorf("correlationID = %d, want %d", corrID, want)
		}
	}
}

func TestMetadataTarget_WithClientIDs(t *testing.T) {
	cfg := activePassiveCfg(config.ActivePrimary, "host1:9092", "host2:9092")

	clientIDs := []string{"", "a", "my-producer-v2", "client.with.dots"}
	for _, cid := range clientIDs {
		req := buildMetadataRequest(12345, cid)
		addr, corrID, err := MetadataTarget(req, cfg)
		if err != nil {
			t.Fatalf("clientID=%q: unexpected error: %v", cid, err)
		}
		if addr != "host1:9092" {
			t.Errorf("clientID=%q: upstreamAddr = %q, want host1:9092", cid, addr)
		}
		if corrID != 12345 {
			t.Errorf("clientID=%q: correlationID = %d, want 12345", cid, corrID)
		}
	}
}

// --- Tests: Non-metadata API keys are rejected ---

func TestMetadataTarget_RejectsProduce(t *testing.T) {
	cfg := activePassiveCfg(config.ActivePrimary, "host1:9092", "host2:9092")
	req := buildRequest(protocol.APIKeyProduce, 8, 1, "producer")

	_, _, err := MetadataTarget(req, cfg)
	if err == nil {
		t.Fatal("expected error for Produce request, got nil")
	}
}

func TestMetadataTarget_RejectsFetch(t *testing.T) {
	cfg := activePassiveCfg(config.ActivePrimary, "host1:9092", "host2:9092")
	req := buildRequest(protocol.APIKeyFetch, 12, 1, "consumer")

	_, _, err := MetadataTarget(req, cfg)
	if err == nil {
		t.Fatal("expected error for Fetch request, got nil")
	}
}

func TestMetadataTarget_RejectsSaslHandshake(t *testing.T) {
	cfg := activePassiveCfg(config.ActivePrimary, "host1:9092", "host2:9092")
	req := buildRequest(protocol.APIKeySaslHandshake, 1, 1, "")

	_, _, err := MetadataTarget(req, cfg)
	if err == nil {
		t.Fatal("expected error for SaslHandshake request, got nil")
	}
}

func TestMetadataTarget_RejectsSaslAuthenticate(t *testing.T) {
	cfg := activePassiveCfg(config.ActivePrimary, "host1:9092", "host2:9092")
	req := buildRequest(protocol.APIKeySaslAuthenticate, 2, 1, "")

	_, _, err := MetadataTarget(req, cfg)
	if err == nil {
		t.Fatal("expected error for SaslAuthenticate request, got nil")
	}
}

// --- Tests: Non-active_passive mode is rejected ---

func TestMetadataTarget_RejectsLoadBalance(t *testing.T) {
	lbCfg := config.ClusterConfig{
		Mode: config.ModeLoadBalance,
		Primary: config.ClusterEndpoint{
			Bootstrap: "pkc-33333.us-east-1.aws.confluent.cloud:9092",
			Weight:    70,
		},
		Secondary: config.ClusterEndpoint{
			Bootstrap: "pkc-44444.us-east-2.aws.confluent.cloud:9092",
			Weight:    30,
		},
	}
	req := buildMetadataRequest(1, "")

	_, _, err := MetadataTarget(req, lbCfg)
	if err == nil {
		t.Fatal("expected error for load_balance mode, got nil")
	}
}

// --- Tests: Invalid / corrupted request data ---

func TestMetadataTarget_TruncatedData(t *testing.T) {
	cfg := activePassiveCfg(config.ActivePrimary, "host1:9092", "host2:9092")

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too short (4 bytes)", make([]byte, 4)},
		{"too short (10 bytes)", make([]byte, 10)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := MetadataTarget(tt.data, cfg)
			if err == nil {
				t.Fatal("expected error for truncated data, got nil")
			}
		})
	}
}

// --- Tests: Configuration edge cases ---

func TestMetadataTarget_EmptyBootstrap(t *testing.T) {
	cfg := activePassiveCfg(config.ActivePrimary, "", "host2:9092")
	req := buildMetadataRequest(1, "")

	_, _, err := MetadataTarget(req, cfg)
	if err == nil {
		t.Fatal("expected error for empty bootstrap address, got nil")
	}
}

func TestMetadataTarget_UnknownActive(t *testing.T) {
	cfg := activePassiveCfg("terciario", "host1:9092", "host2:9092")
	req := buildMetadataRequest(1, "")

	_, _, err := MetadataTarget(req, cfg)
	if err == nil {
		t.Fatal("expected error for unknown active cluster, got nil")
	}
}
