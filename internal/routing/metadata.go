// Package routing implements protocol-aware Kafka message routing.
// This file handles MetadataResponse parsing and broker host:port rewriting
// per the Kafka protocol specification Section 5.2.
package routing

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

// Errors returned by MetadataResponse parsing and rewriting.
var (
	ErrMetadataTooShort        = errors.New("metadata response too short to parse")
	ErrInvalidBrokerArray      = errors.New("invalid broker array in metadata response")
	ErrBrokerHostTooLong       = errors.New("broker host exceeds maximum length (32767 bytes)")
	ErrMetadataResponseTooLarge = errors.New("metadata response exceeds maximum size (100MB)")
)

// MaxMetadataResponseSize caps the total size of a MetadataResponse we'll process,
// protecting against memory exhaustion from a malformed or malicious response.
const MaxMetadataResponseSize = 100 << 20 // 100 MB

// MaxStringLen is the maximum length of a Kafka string (int16 max).
const MaxStringLen = 32767

// Broker represents a single broker entry parsed from a MetadataResponse.
type Broker struct {
	NodeID int32
	Host   string
	Port   int32
	// Rack is the broker rack (nullable string, v1+).
	// Empty string means either absent (v0) or null (v1+ with length=-1).
	Rack      string
	RackIsSet bool // true if the rack field had a non-null value
}

// MetadataResponse holds the parsed components of a MetadataResponse frame.
type MetadataResponse struct {
	CorrelationID  int32
	ThrottleTimeMs int32 // v3+
	Brokers        []Broker
	// bodyBeforeBrokers is the raw body bytes before the broker array length field.
	bodyBeforeBrokers []byte
	// bodyAfterBrokers is the raw body bytes after the broker array.
	bodyAfterBrokers []byte
}

// ParseMetadataResponseBody parses a Kafka MetadataResponse body (excluding the
// frame size prefix and correlation_id) into a MetadataResponse struct.
//
// The data parameter should be the body bytes starting immediately after the
// correlation_id field. The version parameter determines which fields are present.
//
// This function also captures the raw bytes before and after the broker array
// so that RewriteBrokers can reconstruct the response with modified broker entries.
func ParseMetadataResponseBody(data []byte, version int16) (*MetadataResponse, error) {
	if len(data) > MaxMetadataResponseSize {
		return nil, ErrMetadataResponseTooLarge
	}

	r := &MetadataResponse{}
	pos := 0

	// ── ThrottleTimeMs (v3+) ──────────────────────────────────────────
	beforeBrokersStart := 0
	if version >= 3 {
		if len(data) < pos+4 {
			return nil, fmt.Errorf("%w: missing ThrottleTimeMs", ErrMetadataTooShort)
		}
		r.ThrottleTimeMs = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
	}
	r.bodyBeforeBrokers = make([]byte, pos-beforeBrokersStart)
	copy(r.bodyBeforeBrokers, data[beforeBrokersStart:pos])

	// ── Brokers Array ─────────────────────────────────────────────────
	if len(data) < pos+4 {
		return nil, fmt.Errorf("%w: missing broker array length", ErrMetadataTooShort)
	}

	brokerArrayStart := pos
	brokerCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if brokerCount < 0 {
		return nil, fmt.Errorf("%w: negative broker count %d", ErrInvalidBrokerArray, brokerCount)
	}

	// An empty broker array is unusual but valid — capture body segments
	// consistently: bodyBeforeBrokers stops before the array length field,
	// so RewriteBrokers always appends the (zero-length) array.
	if brokerCount == 0 {
		r.bodyBeforeBrokers = make([]byte, brokerArrayStart)
		copy(r.bodyBeforeBrokers, data[:brokerArrayStart])
		r.bodyAfterBrokers = make([]byte, len(data)-pos)
		copy(r.bodyAfterBrokers, data[pos:])
		return r, nil
	}

	r.Brokers = make([]Broker, brokerCount)
	for i := 0; i < brokerCount; i++ {
		b, bytesRead, err := parseBroker(data[pos:], version)
		if err != nil {
			return nil, fmt.Errorf("broker[%d]: %w", i, err)
		}
		r.Brokers[i] = b
		pos += bytesRead
	}

	// ── Capture remaining body ────────────────────────────────────────
	r.bodyBeforeBrokers = make([]byte, brokerArrayStart)
	copy(r.bodyBeforeBrokers, data[:brokerArrayStart])

	r.bodyAfterBrokers = make([]byte, len(data)-pos)
	copy(r.bodyAfterBrokers, data[pos:])

	return r, nil
}

// parseBroker parses a single broker entry from raw bytes.
// Returns the parsed Broker, the number of bytes consumed, and any error.
//
// Wire format:
//
//	v0: NodeId(int32) Host(string) Port(int32)
//	v1+: NodeId(int32) Host(string) Port(int32) Rack(nullable_string)
func parseBroker(data []byte, version int16) (Broker, int, error) {
	var b Broker
	pos := 0

	// NodeId (int32)
	if len(data) < pos+4 {
		return b, 0, fmt.Errorf("%w: missing NodeId", ErrMetadataTooShort)
	}
	b.NodeID = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	// Host (string: int16 length + UTF-8)
	if len(data) < pos+2 {
		return b, 0, fmt.Errorf("%w: missing Host length", ErrMetadataTooShort)
	}
	hostLen := int(int16(binary.BigEndian.Uint16(data[pos : pos+2])))
	pos += 2

	if hostLen < 0 {
		return b, 0, fmt.Errorf("invalid broker Host length: %d (must be >= 0)", hostLen)
	}
	if hostLen > MaxStringLen {
		return b, 0, ErrBrokerHostTooLong
	}
	if len(data) < pos+hostLen {
		return b, 0, fmt.Errorf("%w: truncated Host (need %d, have %d)",
			ErrMetadataTooShort, hostLen, len(data)-pos)
	}
	b.Host = string(data[pos : pos+hostLen])
	pos += hostLen

	// Port (int32)
	if len(data) < pos+4 {
		return b, 0, fmt.Errorf("%w: missing Port", ErrMetadataTooShort)
	}
	b.Port = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	// Rack (nullable_string, v1+)
	if version >= 1 {
		if len(data) < pos+2 {
			return b, 0, fmt.Errorf("%w: missing Rack length", ErrMetadataTooShort)
		}
		rackLen := int(int16(binary.BigEndian.Uint16(data[pos : pos+2])))
		pos += 2

		switch {
		case rackLen == -1:
			// null rack — no bytes follow
			b.RackIsSet = false
		case rackLen == 0:
			// empty string rack
			b.Rack = ""
			b.RackIsSet = true
		case rackLen > 0:
			if rackLen > MaxStringLen {
				return b, 0, fmt.Errorf("broker rack exceeds maximum length: %d", rackLen)
			}
			if len(data) < pos+rackLen {
				return b, 0, fmt.Errorf("%w: truncated Rack (need %d, have %d)",
					ErrMetadataTooShort, rackLen, len(data)-pos)
			}
			b.Rack = string(data[pos : pos+rackLen])
			b.RackIsSet = true
			pos += rackLen
		default:
			return b, 0, fmt.Errorf("invalid Rack length: %d", rackLen)
		}
	}

	return b, pos, nil
}

// RewriteBrokers creates a new MetadataResponse body with all broker host:port
// entries rewritten to proxyHost and proxyPort. All other fields (topic metadata,
// controller ID, cluster ID, cluster authorized operations, etc.) are preserved
// byte-for-byte from the original response.
//
// The returned slice is a complete Kafka response frame: size prefix (4 bytes) +
// correlation_id (4 bytes) + modified body. The size prefix is recalculated to
// reflect the new body length.
//
// proxyHost is the SNI hostname the client used to connect (e.g. "kafka.example.com").
// proxyPort is the port the proxy listens on (typically 9092).
// version is the Metadata API version the client requested.
func RewriteBrokers(resp *MetadataResponse, proxyHost string, proxyPort int32, version int16) ([]byte, error) {
	// Validate proxy host length
	if len(proxyHost) > MaxStringLen {
		return nil, fmt.Errorf("%w: proxy host is %d bytes", ErrBrokerHostTooLong, len(proxyHost))
	}

	// ── Build new broker array section ────────────────────────────────
	var brokerBuf bytes.Buffer

	// Broker array length
	if err := binary.Write(&brokerBuf, binary.BigEndian, int32(len(resp.Brokers))); err != nil {
		return nil, fmt.Errorf("writing broker array length: %w", err)
	}

	for _, b := range resp.Brokers {
		// NodeId (unchanged)
		if err := binary.Write(&brokerBuf, binary.BigEndian, b.NodeID); err != nil {
			return nil, fmt.Errorf("writing NodeId: %w", err)
		}

		// Host (rewritten to proxy host)
		hostBytes := []byte(proxyHost)
		if err := binary.Write(&brokerBuf, binary.BigEndian, int16(len(hostBytes))); err != nil {
			return nil, fmt.Errorf("writing Host length: %w", err)
		}
		brokerBuf.Write(hostBytes)

		// Port (rewritten to proxy port)
		if err := binary.Write(&brokerBuf, binary.BigEndian, proxyPort); err != nil {
			return nil, fmt.Errorf("writing Port: %w", err)
		}

		// Rack (preserved, v1+)
		if version >= 1 {
			if b.RackIsSet {
				rackBytes := []byte(b.Rack)
				if err := binary.Write(&brokerBuf, binary.BigEndian, int16(len(rackBytes))); err != nil {
					return nil, fmt.Errorf("writing Rack length: %w", err)
				}
				brokerBuf.Write(rackBytes)
			} else {
				// Null rack
				if err := binary.Write(&brokerBuf, binary.BigEndian, int16(-1)); err != nil {
					return nil, fmt.Errorf("writing null Rack: %w", err)
				}
			}
		}
	}

	// ── Assemble the new response ─────────────────────────────────────
	//
	// Frame layout:
	//   [4] MessageLength (excludes this field)
	//   [4] CorrelationID
	//   [bodyBeforeBrokers] [brokerArray] [bodyAfterBrokers]
	//
	// brokerBuf already includes the array length + all modified broker entries.

	// New body size = everything before the array length + new broker array + everything after.
	// Frame layout: [4:size(excludes self)] [4:correlationID] [bodyParts]
	newBodyLen := len(resp.bodyBeforeBrokers) + brokerBuf.Len() + len(resp.bodyAfterBrokers)
	// Size field value = correlationID(4) + bodyParts
	sizeFieldValue := 4 + newBodyLen
	// Total frame = sizePrefix(4) + sizeFieldValue
	newFrameLen := 4 + sizeFieldValue

	if newFrameLen > MaxMetadataResponseSize {
		return nil, ErrMetadataResponseTooLarge
	}

	result := make([]byte, newFrameLen)

	// Size prefix (excludes itself; reports correlationID + remaining body)
	binary.BigEndian.PutUint32(result[0:4], uint32(sizeFieldValue))

	// CorrelationID
	binary.BigEndian.PutUint32(result[4:8], uint32(resp.CorrelationID))

	// Body before broker array (includes ThrottleTimeMs if v3+)
	offset := 8
	copy(result[offset:], resp.bodyBeforeBrokers)
	offset += len(resp.bodyBeforeBrokers)

	// New broker array
	copy(result[offset:], brokerBuf.Bytes())
	offset += brokerBuf.Len()

	// Body after broker array (ClusterId, ControllerId, Topics, etc.)
	copy(result[offset:], resp.bodyAfterBrokers)

	return result, nil
}

// RewriteMetadataResponse is a convenience function that parses a raw
// MetadataResponse frame and rewrites broker host:port entries in one step.
//
// rawResponse contains the complete upstream response frame (size prefix +
// correlation_id + body). proxyHost replaces the broker host, proxyPort replaces
// the broker port. version is the Metadata API version.
//
// Returns a new byte slice containing the complete rewritten response frame
// with the frame size header recalculated.
func RewriteMetadataResponse(rawResponse []byte, proxyHost string, proxyPort int32, version int16) ([]byte, error) {
	if len(rawResponse) > MaxMetadataResponseSize {
		return nil, ErrMetadataResponseTooLarge
	}

	// Parse the response frame header: size(4) + correlation_id(4)
	if len(rawResponse) < 8 {
		return nil, fmt.Errorf("%w: response frame too short (%d bytes)", ErrMetadataTooShort, len(rawResponse))
	}

	frameSize := int32(binary.BigEndian.Uint32(rawResponse[0:4]))
	correlationID := int32(binary.BigEndian.Uint32(rawResponse[4:8]))

	// Sanity check: frame size should match remaining data
	if frameSize < 4 {
		return nil, fmt.Errorf("invalid frame size %d (min 4 for correlation_id)", frameSize)
	}
	expectedLen := 4 + int(frameSize) // size prefix not counted in frameSize
	if len(rawResponse) < expectedLen {
		return nil, fmt.Errorf("%w: frame size=%d but only %d bytes available",
			ErrMetadataTooShort, frameSize, len(rawResponse))
	}

	// Parse the body (after size prefix + correlation_id)
	body := rawResponse[8:expectedLen]
	resp, err := ParseMetadataResponseBody(body, version)
	if err != nil {
		return nil, fmt.Errorf("parsing metadata body: %w", err)
	}
	resp.CorrelationID = correlationID

	return RewriteBrokers(resp, proxyHost, proxyPort, version)
}

// MetadataTarget resolves the upstream bootstrap address for a Metadata
// request (API Key 3) in active_passive mode. It extracts the Correlation ID
// from the request header so the caller can preserve it when forwarding the
// response back to the client.
//
// Returns:
//   - upstreamAddr: the bootstrap address of the active cluster (primary or secondary)
//   - correlationID: the client-supplied correlation ID from the request header
//   - err: non-nil if the data is not a valid Metadata request or the mode is not active_passive
func MetadataTarget(requestData []byte, clusterCfg config.ClusterConfig) (upstreamAddr string, correlationID int32, err error) {
	// Parse the Kafka request header to detect the API key and extract
	// the correlation ID.
	hdr, err := protocol.ParseRequestHeader(requestData)
	if err != nil {
		return "", 0, fmt.Errorf("parsing metadata request header: %w", err)
	}

	// Guard: this function handles API Key 3 (Metadata) only.
	if hdr.APIKey != protocol.APIKeyMetadata {
		return "", 0, fmt.Errorf("not a metadata request: API key %d (expected %d)", hdr.APIKey, protocol.APIKeyMetadata)
	}

	// T16 covers active_passive mode only. Load-balance metadata
	// (synthetic merging of two responses) is handled in T28.
	if clusterCfg.Mode != config.ModeActivePassive && clusterCfg.Mode != config.ModeSingle {
		return "", 0, fmt.Errorf(
			"metadata forwarding for mode %q is not implemented",
			clusterCfg.Mode,
		)
	}

	// Resolve the active cluster's bootstrap address.
	var targetAddr string
	switch clusterCfg.Active {
	case config.ActivePrimary:
		targetAddr = clusterCfg.Primary.Bootstrap
	case config.ActiveSecondary:
		targetAddr = clusterCfg.Secondary.Bootstrap
	default:
		return "", 0, fmt.Errorf("unknown active cluster: %q (expected %q or %q)",
			clusterCfg.Active, config.ActivePrimary, config.ActiveSecondary)
	}

	if targetAddr == "" {
		return "", 0, fmt.Errorf("active cluster %q has empty bootstrap address", clusterCfg.Active)
	}

	return targetAddr, hdr.CorrelationID, nil
}
