package protocol

// Kafka API key constants.
const (
	APIKeyProduce          int16 = 0
	APIKeyFetch            int16 = 1
	APIKeyMetadata         int16 = 3
	APIKeySaslHandshake    int16 = 17
	APIKeySaslAuthenticate int16 = 36
)

// VersionRange describes the inclusive min and max API versions
// supported by the proxy for a given Kafka API key.
type VersionRange struct {
	MinVersion int16
	MaxVersion int16
}

// SupportedAPIs maps each API key the proxy can parse to its supported
// version range.  Keys not present in this map are treated as passthrough.
// Version ranges per spec Section 4.3.
var SupportedAPIs = map[int16]VersionRange{
	APIKeyProduce:          {MinVersion: 0, MaxVersion: 9},
	APIKeyFetch:            {MinVersion: 0, MaxVersion: 12},
	APIKeyMetadata:         {MinVersion: 0, MaxVersion: 12},
	APIKeySaslHandshake:    {MinVersion: 0, MaxVersion: 1},
	APIKeySaslAuthenticate: {MinVersion: 0, MaxVersion: 2},
}

// IsSupported reports whether (apiKey, version) falls within a declared
// range.  An API key not present in SupportedAPIs returns false (passthrough).
func IsSupported(apiKey, version int16) bool {
	r, ok := SupportedAPIs[apiKey]
	if !ok {
		return false
	}
	return version >= r.MinVersion && version <= r.MaxVersion
}

// GetMaxVersion returns the maximum supported version for the given API key.
// The bool is false when the API key is not in the SupportedAPIs map.
func GetMaxVersion(apiKey int16) (int16, bool) {
	r, ok := SupportedAPIs[apiKey]
	if !ok {
		return 0, false
	}
	return r.MaxVersion, true
}
