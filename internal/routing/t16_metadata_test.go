package routing

import (
	"encoding/binary"
	"testing"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

func buildMetaReq(correlationID int32, clientID string) []byte {
	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], uint16(protocol.APIKeyMetadata))
	binary.BigEndian.PutUint16(header[2:4], 4)
	binary.BigEndian.PutUint32(header[4:8], uint32(correlationID))
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

func buildKafkaReq(apiKey, apiVersion int16, correlationID int32, clientID string) []byte {
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

func apCfg(active, prim, sec string) config.ClusterConfig {
	return config.ClusterConfig{
		Mode:   config.ModeActivePassive,
		Active: active,
		Primary:   config.ClusterEndpoint{Bootstrap: prim},
		Secondary: config.ClusterEndpoint{Bootstrap: sec},
	}
}

func TestT16_MetadataTarget_ActivePrimary(t *testing.T) {
	cfg := apCfg(config.ActivePrimary, "pkc-11111.us-east-1.aws.confluent.cloud:9092", "pkc-22222.us-east-2.aws.confluent.cloud:9092")
	addr, corrID, err := MetadataTarget(buildMetaReq(42, "myclient"), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "pkc-11111.us-east-1.aws.confluent.cloud:9092" {
		t.Errorf("addr = %q", addr)
	}
	if corrID != 42 {
		t.Errorf("corrID = %d, want 42", corrID)
	}
}

func TestT16_MetadataTarget_ActiveSecondary(t *testing.T) {
	cfg := apCfg(config.ActiveSecondary, "pkc-11111.us-east-1.aws.confluent.cloud:9092", "pkc-22222.us-east-2.aws.confluent.cloud:9092")
	addr, corrID, err := MetadataTarget(buildMetaReq(99, "app-v2"), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "pkc-22222.us-east-2.aws.confluent.cloud:9092" {
		t.Errorf("addr = %q", addr)
	}
	if corrID != 99 {
		t.Errorf("corrID = %d, want 99", corrID)
	}
}

func TestT16_CorrelationIDPreserved(t *testing.T) {
	cfg := apCfg(config.ActivePrimary, "h1:9092", "h2:9092")
	for _, want := range []int32{0, 1, 42, -1, 2147483647, -2147483648} {
		_, corrID, err := MetadataTarget(buildMetaReq(want, ""), cfg)
		if err != nil {
			t.Fatalf("corrID=%d: %v", want, err)
		}
		if corrID != want {
			t.Errorf("corrID = %d, want %d", corrID, want)
		}
	}
}

func TestT16_RejectsNonMetadata(t *testing.T) {
	cfg := apCfg(config.ActivePrimary, "h1:9092", "h2:9092")
	tests := []struct {
		name   string
		apiKey int16
	}{
		{"Produce", protocol.APIKeyProduce},
		{"Fetch", protocol.APIKeyFetch},
		{"SaslHandshake", protocol.APIKeySaslHandshake},
		{"SaslAuthenticate", protocol.APIKeySaslAuthenticate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := MetadataTarget(buildKafkaReq(tt.apiKey, 0, 1, ""), cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestT16_RejectsLoadBalance(t *testing.T) {
	lbCfg := config.ClusterConfig{
		Mode: config.ModeLoadBalance,
		Primary:   config.ClusterEndpoint{Bootstrap: "pkc-33333.us-east-1.aws.confluent.cloud:9092", Weight: 70},
		Secondary: config.ClusterEndpoint{Bootstrap: "pkc-44444.us-east-2.aws.confluent.cloud:9092", Weight: 30},
	}
	_, _, err := MetadataTarget(buildMetaReq(1, ""), lbCfg)
	if err == nil {
		t.Fatal("expected error for load_balance mode")
	}
}

func TestT16_TruncatedData(t *testing.T) {
	cfg := apCfg(config.ActivePrimary, "h1:9092", "h2:9092")
	for _, d := range [][]byte{{}, make([]byte, 4), make([]byte, 10)} {
		_, _, err := MetadataTarget(d, cfg)
		if err == nil {
			t.Fatal("expected error for truncated data")
		}
	}
}

func TestT16_EmptyBootstrap(t *testing.T) {
	cfg := apCfg(config.ActivePrimary, "", "h2:9092")
	_, _, err := MetadataTarget(buildMetaReq(1, ""), cfg)
	if err == nil {
		t.Fatal("expected error for empty bootstrap")
	}
}

func TestT16_UnknownActive(t *testing.T) {
	cfg := apCfg("terciario", "h1:9092", "h2:9092")
	_, _, err := MetadataTarget(buildMetaReq(1, ""), cfg)
	if err == nil {
		t.Fatal("expected error for unknown active")
	}
}
