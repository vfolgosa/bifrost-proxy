// Package routing implements request routing with protocol awareness.
// This file handles SASL Handshake (API Key 17) and SASL Authenticate
// (API Key 36) passthrough.
//
// SASL frames are forwarded byte-for-byte between client and upstream,
// including any multi-step exchange (e.g. SCRAM). After the upstream
// responds with error_code=0 on a SASL Authenticate, the connection is
// marked as authenticated.
package routing

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/vfolgosa/bifrost-proxy/internal/logger"
	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

// SASLHandler tracks the SASL authentication state of a single connection.
// It detects SASL frames and forwards them unchanged (blind passthrough).
// On a successful SASL Authenticate response (error_code == 0) the handler
// marks the connection as authenticated.
type SASLHandler struct {
	authenticated bool
}

// Authenticated reports whether SASL authentication has completed
// successfully on this connection.
func (h *SASLHandler) Authenticated() bool {
	return h.authenticated
}

// IsSASLAuthenticateRequest checks whether the given raw Kafka frame is a
// SASL Authenticate request by peeking at the API key in the header.
//
// Frame layout (request):
//
//	Offset  Bytes  Field
//	  0       4    Size          (int32)
//	  4       2    APIKey        (int16)
//	  6       2    APIVersion    (int16)
//	  8       4    CorrelationID (int32)
//	 12       2    ClientIDLen   (int16)
//	 14      N     ClientID      (string)
//
// Returns false for truncated or malformed frames.
func IsSASLAuthenticateRequest(frame []byte) bool {
	if len(frame) < 6 {
		return false
	}
	apiKey := int16(binary.BigEndian.Uint16(frame[4:6]))
	return apiKey == protocol.APIKeySaslAuthenticate
}

// IsSASLRequest returns true if the 6-byte header (size:4 + api_key:2)
// indicates a SASL Handshake (17) or SASL Authenticate (36) request.
func IsSASLRequest(header []byte) bool {
	if len(header) < 6 {
		return false
	}
	apiKey := int16(binary.BigEndian.Uint16(header[4:6]))
	return apiKey == protocol.APIKeySaslHandshake || apiKey == protocol.APIKeySaslAuthenticate
}

// PassthroughRequest returns the request frame unchanged. It is a no-op
// passthrough for SASL Authenticate — the proxy forwards the raw bytes
// without modification.
func (h *SASLHandler) PassthroughRequest(frame []byte) []byte {
	logger.Default().Debug("forwarding SASL Authenticate request",
		"size_bytes", len(frame))
	return frame
}

// PassthroughResponse returns the response frame unchanged and updates
// the authentication state. If the response error_code is 0, the
// connection is marked as authenticated.
//
// SASL Authenticate response body layout (v0+):
//
//	Offset  Bytes  Field
//	  0       2    ErrorCode          (int16)
//	  2       2    ErrorMessageLen    (int16) — -1 for null
//	  4      N     ErrorMessage       (string)
//	 ...           AuthBytes          (bytes) — prefixed with int32 length
//	 ...           SessionLifetimeMs  (int64) — added in v1
//
// The error_code is at a fixed offset of 8 from the start of the Kafka
// response frame (after the 4-byte Size and 4-byte CorrelationID).
func (h *SASLHandler) PassthroughResponse(frame []byte) []byte {
	if len(frame) >= 10 {
		errorCode := int16(binary.BigEndian.Uint16(frame[8:10]))
		if errorCode == 0 {
			if !h.authenticated {
				h.authenticated = true
				logger.Default().Info("SASL authentication successful")
			}
		}
	}
	return frame
}

// handleSASLResponse checks a raw SASL Authenticate response frame for success.
func handleSASLResponse(frame []byte) {
	if len(frame) >= 10 {
		errorCode := int16(binary.BigEndian.Uint16(frame[8:10]))
		if errorCode == 0 {
			logger.Default().Info("SASL authentication successful")
		}
	}
}

// HandleSASLPassthrough reads a Kafka request frame from clientConn,
// checks if it is a SASL Authenticate request (API Key 36), and if so
// forwards it byte-for-byte to upstreamConn and returns the response.
//
// If the request is not SASL Authenticate, the function returns
// (false, nil) immediately — the caller is responsible for handling
// the frame through other routing paths.
//
// For SASL Authenticate: the full request frame is read from clientConn,
// forwarded to upstreamConn, the response is read from upstreamConn, and
// forwarded back to clientConn.
func HandleSASLPassthrough(clientConn, upstreamConn net.Conn) (bool, error) {
	// Read the first 6 bytes to determine the API key.
	// Kafka frame:  [size:4] [api_key:2] [api_version:2] ...
	header := make([]byte, 6)
	if _, err := io.ReadFull(clientConn, header); err != nil {
		return false, err
	}

	apiKey := int16(binary.BigEndian.Uint16(header[4:6]))
	if apiKey != protocol.APIKeySaslAuthenticate {
		// Not a SASL Authenticate request — caller should handle
		// this frame through other routing paths.
		return false, nil
	}

	// Parse frame size to determine the remaining payload length.
	frameSize := int32(binary.BigEndian.Uint32(header[0:4]))
	remaining := int(frameSize) - 2 // subtract the 2-byte API key we already read

	if remaining < 0 {
		// Frame size claims to be smaller than what we already read: malformed.
		return false, nil
	}

	// Read the rest of the request frame (remaining header + body).
	requestPayload := make([]byte, remaining)
	if _, err := io.ReadFull(clientConn, requestPayload); err != nil {
		return false, err
	}

	// Reconstruct the full request frame: size prefix + api_key + rest.
	fullRequest := make([]byte, 0, 4+2+remaining)
	fullRequest = append(fullRequest, header...)
	fullRequest = append(fullRequest, requestPayload...)

	logger.L().Debug("forwarding SASL Authenticate request",
		"size_bytes", len(fullRequest))

	// Forward entire request frame byte-for-byte to upstream.
	if _, err := upstreamConn.Write(fullRequest); err != nil {
		return true, err
	}

	// Read the response frame: size(4) + correlation_id(4) + body.
	respHeader := make([]byte, 8)
	if _, err := io.ReadFull(upstreamConn, respHeader); err != nil {
		return true, err
	}

	respSize := int32(binary.BigEndian.Uint32(respHeader[0:4]))
	respBody := make([]byte, int(respSize)-4)
	if _, err := io.ReadFull(upstreamConn, respBody); err != nil {
		return true, err
	}

	// Reconstruct the full response frame.
	fullResponse := make([]byte, 0, 8+len(respBody))
	fullResponse = append(fullResponse, respHeader...)
	fullResponse = append(fullResponse, respBody...)

	// Check authentication result from the response.
	handleSASLResponse(fullResponse)

	// Forward response back to client.
	if _, err := clientConn.Write(fullResponse); err != nil {
		return true, err
	}

	return true, nil
}

// ── Multi-step SASL Exchange ──────────────────────────────────────────
//
// Kafka SASL authentication may involve multiple round-trips:
//
//  1. SaslHandshake (API Key 17) — negotiated once per connection.
//  2. SaslAuthenticate (API Key 36) — may be repeated for multi-step
//     mechanisms like SCRAM (client-first, server-first, client-final,
//     server-final). Each exchange is a single request/response pair.
//
// HandleSASLExchange loops, reading frames from clientConn and forwarding
// every SASL Handshake and SASL Authenticate request/response pair
// byte-for-byte between client and upstream.
//
// The loop ends when either:
//   - A non-SASL frame arrives: returns that frame's 6-byte header so the
//     caller can route it (Metadata interception, Produce/Fetch routing, etc.).
//   - An I/O error occurs: returns the error.
//
// After a successful SASL Authenticate response (error_code == 0), the
// handler is marked as authenticated. The loop continues so subsequent
// non-SASL frames (e.g. Metadata) are read and returned to the caller.

// SASLExchangeResult holds the result of HandleSASLExchange, including
// the correlation_id and client_id extracted from the first non-SASL frame.
type SASLExchangeResult struct {
	Header        []byte // raw bytes consumed from clientConn (full Kafka header)
	CorrelationID int32
	ClientID      string
}

// HandleSASLExchange loops, reading frames from clientConn and forwarding
// every SASL Handshake (API Key 17) and SASL Authenticate (API Key 36)
// frame byte-for-byte to upstreamConn. Responses are forwarded back to
// the client unchanged.
//
// Parameters:
//   - handler: tracks authentication state (updated on SASL Authenticate
//     responses with error_code == 0).
//   - clientConn, upstreamConn: the established TLS/TCP connections.
//
// Returns a SASLExchangeResult containing the raw Kafka header bytes
// (including size prefix) of the first non-SASL frame, its correlation_id,
// and client_id. Returns nil + error on I/O failure.
func HandleSASLExchange(handler *SASLHandler, clientConn, upstreamConn net.Conn) (*SASLExchangeResult, error) {
	for {
		// Read the first 6 bytes to peek at the API key.
		// Kafka frame: [size:4] [api_key:2] [api_version:2] ...
		peek := make([]byte, 6)
		if _, err := io.ReadFull(clientConn, peek); err != nil {
			return nil, fmt.Errorf("reading SASL frame header: %w", err)
		}

		apiKey := int16(binary.BigEndian.Uint16(peek[4:6]))

		if apiKey != protocol.APIKeySaslHandshake && apiKey != protocol.APIKeySaslAuthenticate {
			// Not a SASL frame — read the remaining fixed header to
			// extract correlation_id and client_id, then return all
			// consumed bytes (including peek) for the caller to prepend.

			// Read the rest of the fixed header: api_version(2) + correlation_id(4) + client_id_len(2) = 8 bytes
			fixedRest := make([]byte, 8)
			if _, err := io.ReadFull(clientConn, fixedRest); err != nil {
				return nil, fmt.Errorf("reading non-SASL frame fixed header: %w", err)
			}

			correlationID := int32(binary.BigEndian.Uint32(fixedRest[2:6]))
			clientIDLen := int16(binary.BigEndian.Uint16(fixedRest[6:8]))

			var clientID string
			headerEnd := 14 // peek(6) + fixedRest(8)
			if clientIDLen > 0 {
				clientIDBytes := make([]byte, clientIDLen)
				if _, err := io.ReadFull(clientConn, clientIDBytes); err != nil {
					return nil, fmt.Errorf("reading client_id from non-SASL frame: %w", err)
				}
				clientID = string(clientIDBytes)
				headerEnd += int(clientIDLen)
			}

			// Build the full header slice: peek + fixed rest + client ID
			header := make([]byte, headerEnd)
			copy(header[0:6], peek)
			copy(header[6:14], fixedRest)
			if clientIDLen > 0 {
				copy(header[14:], []byte(clientID))
			}

			return &SASLExchangeResult{
				Header:        header,
				CorrelationID: correlationID,
				ClientID:      clientID,
			}, nil
		}

		// Parse frame size and read the remaining payload.
		frameSize := int32(binary.BigEndian.Uint32(peek[0:4]))
		remaining := int(frameSize) - 2 // subtract the 2-byte API key already read

		if remaining < 0 {
			return nil, fmt.Errorf("malformed SASL frame: size %d too small", frameSize)
		}

		payload := make([]byte, remaining)
		if _, err := io.ReadFull(clientConn, payload); err != nil {
			return nil, fmt.Errorf("reading SASL frame payload (%d bytes): %w", remaining, err)
		}

		// Reconstruct the full request frame.
		fullRequest := make([]byte, 0, 6+remaining)
		fullRequest = append(fullRequest, peek...)
		fullRequest = append(fullRequest, payload...)

		apiName := "Handshake"
		if apiKey == protocol.APIKeySaslAuthenticate {
			apiName = "Authenticate"
		}
		logger.L().Debug("forwarding SASL request",
			"api", apiName, "size_bytes", len(fullRequest))

		// Forward entire request frame byte-for-byte to upstream.
		if _, err := upstreamConn.Write(fullRequest); err != nil {
			return nil, fmt.Errorf("forwarding SASL %s to upstream: %w", apiName, err)
		}

		// Read the response frame: size(4) + correlation_id(4) + body.
		respHeader := make([]byte, 8)
		if _, err := io.ReadFull(upstreamConn, respHeader); err != nil {
			return nil, fmt.Errorf("reading SASL %s response header: %w", apiName, err)
		}

		respSize := int32(binary.BigEndian.Uint32(respHeader[0:4]))
		if respSize < 4 {
			return nil, fmt.Errorf("invalid SASL %s response size %d", apiName, respSize)
		}

		respBody := make([]byte, respSize-4)
		if _, err := io.ReadFull(upstreamConn, respBody); err != nil {
			return nil, fmt.Errorf("reading SASL %s response body (%d bytes): %w", apiName, respSize-4, err)
		}

		// Reconstruct the full response frame.
		fullResponse := make([]byte, 0, 8+len(respBody))
		fullResponse = append(fullResponse, respHeader...)
		fullResponse = append(fullResponse, respBody...)

		// Update authentication state for SASL Authenticate responses.
		if apiKey == protocol.APIKeySaslAuthenticate {
			handler.PassthroughResponse(fullResponse)
		}

		// Forward response back to client.
		if _, err := clientConn.Write(fullResponse); err != nil {
			return nil, fmt.Errorf("forwarding SASL %s response to client: %w", apiName, err)
		}
	}
}
