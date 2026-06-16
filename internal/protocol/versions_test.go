package protocol

import (
	"strings"
	"testing"
)

func TestIsSupported(t *testing.T) {
	tests := []struct {
		name      string
		apiKey    int16
		version   int16
		supported bool
	}{
		// Produce: 0-9
		{"Produce v0 supported", APIKeyProduce, 0, true},
		{"Produce v9 supported", APIKeyProduce, 9, true},
		{"Produce v10 unsupported", APIKeyProduce, 10, false},
		{"Produce v-1 unsupported", APIKeyProduce, -1, false},

		// Fetch: 0-12
		{"Fetch v0 supported", APIKeyFetch, 0, true},
		{"Fetch v12 supported", APIKeyFetch, 12, true},
		{"Fetch v13 unsupported", APIKeyFetch, 13, false},

		// Metadata: 0-12
		{"Metadata v0 supported", APIKeyMetadata, 0, true},
		{"Metadata v12 supported", APIKeyMetadata, 12, true},
		{"Metadata v13 unsupported", APIKeyMetadata, 13, false},

		// SaslHandshake: 0-1
		{"SaslHandshake v0 supported", APIKeySaslHandshake, 0, true},
		{"SaslHandshake v1 supported", APIKeySaslHandshake, 1, true},
		{"SaslHandshake v2 unsupported", APIKeySaslHandshake, 2, false},

		// SaslAuthenticate: 0-2
		{"SaslAuthenticate v0 supported", APIKeySaslAuthenticate, 0, true},
		{"SaslAuthenticate v2 supported", APIKeySaslAuthenticate, 2, true},
		{"SaslAuthenticate v3 unsupported", APIKeySaslAuthenticate, 3, false},

		// Unknown key
		{"Unknown key unsupported", 999, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSupported(tt.apiKey, tt.version)
			if got != tt.supported {
				t.Errorf("IsSupported(%d, %d) = %v, want %v", tt.apiKey, tt.version, got, tt.supported)
			}
		})
	}
}

func TestGetMaxVersion(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  int16
		wantMax int16
		wantOk  bool
	}{
		{"Produce max=9", APIKeyProduce, 9, true},
		{"Fetch max=12", APIKeyFetch, 12, true},
		{"Metadata max=12", APIKeyMetadata, 12, true},
		{"SaslHandshake max=1", APIKeySaslHandshake, 1, true},
		{"SaslAuthenticate max=2", APIKeySaslAuthenticate, 2, true},
		{"Unknown returns false", 999, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMax, gotOk := GetMaxVersion(tt.apiKey)
			if gotMax != tt.wantMax {
				t.Errorf("GetMaxVersion(%d) max = %d, want %d", tt.apiKey, gotMax, tt.wantMax)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetMaxVersion(%d) ok = %v, want %v", tt.apiKey, gotOk, tt.wantOk)
			}
		})
	}
}

func TestAllFiveAPIKeysInSupportedAPIs(t *testing.T) {
	expectedKeys := []int16{
		APIKeyProduce,
		APIKeyFetch,
		APIKeyMetadata,
		APIKeySaslHandshake,
		APIKeySaslAuthenticate,
	}

	for _, key := range expectedKeys {
		_, ok := SupportedAPIs[key]
		if !ok {
			t.Errorf("API key %d not found in SupportedAPIs map", key)
		}
	}

	if len(SupportedAPIs) < 5 {
		t.Errorf("SupportedAPIs has %d entries, want at least 5", len(SupportedAPIs))
	}
}

func TestSupportedVersionBoundaries(t *testing.T) {
	// Verify that min/max boundaries are inclusive
	tests := []struct {
		name    string
		apiKey  int16
		version int16
		want    bool
	}{
		{"Produce min boundary", APIKeyProduce, 0, true},
		{"Produce max boundary", APIKeyProduce, 9, true},
		{"Fetch min boundary", APIKeyFetch, 0, true},
		{"Fetch max boundary", APIKeyFetch, 12, true},
		{"Metadata min boundary", APIKeyMetadata, 0, true},
		{"Metadata max boundary", APIKeyMetadata, 12, true},
		{"SaslHandshake min boundary", APIKeySaslHandshake, 0, true},
		{"SaslHandshake max boundary", APIKeySaslHandshake, 1, true},
		{"SaslAuthenticate min boundary", APIKeySaslAuthenticate, 0, true},
		{"SaslAuthenticate max boundary", APIKeySaslAuthenticate, 2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSupported(tt.apiKey, tt.version); got != tt.want {
				t.Errorf("IsSupported(%d, %d) = %v, want %v", tt.apiKey, tt.version, got, tt.want)
			}
		})
	}
}

func TestUnsupportedVersionError(t *testing.T) {
	// An unsupported version should return false from IsSupported
	if IsSupported(APIKeyProduce, 99) {
		t.Error("APIKeyProduce v99 should not be supported")
	}
	if IsSupported(APIKeyFetch, -1) {
		t.Error("APIKeyFetch v-1 should not be supported")
	}
	if IsSupported(9999, 0) {
		t.Error("unknown API key 9999 should not be supported")
	}
}

// helper for substring check without importing strings in test (stays minimal)
func containsSubstr(s, substr string) bool {
	return strings.Contains(s, substr)
}
