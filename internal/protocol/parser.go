// Package protocol implements the Kafka wire protocol frame parser
// per the Kafka protocol specification Section 4.3.
package protocol

import (
	"encoding/binary"
	"sync"
)

// RequestHeader represents a Kafka request frame header.
//
// Wire format (big-endian):
//
//	Offset  Bytes  Field
//	  0       4    Size          (int32)  — total message size excluding this field
//	  4       2    APIKey        (int16)  — Kafka API key
//	  6       2    APIVersion    (int16)  — API version
//	  8       4    CorrelationID (int32)  — client-supplied correlation id
//	 12       2    ClientIDLen   (int16)  — length of client ID string, or -1 for null
//	 14      N    ClientID      (string) — UTF-8 encoded client ID
type RequestHeader struct {
	Size          int32
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// ResponseHeader represents a Kafka response frame header.
//
// Wire format (big-endian):
//
//	Offset  Bytes  Field
//	  0       4    Size          (int32) — total message size excluding this field
//	  4       4    CorrelationID (int32) — echoes the request's correlation id
type ResponseHeader struct {
	Size          int32
	CorrelationID int32
}

// Re-export ErrFrameTooShort from errors.go for convenience.
// Defined in errors.go as: "frame too short to parse request header"

// fixedHeaderLen is the minimum length of a request header frame:
// Size(4) + APIKey(2) + APIVersion(2) + CorrelationID(4) + ClientIDLen(2) = 14
const fixedHeaderLen = 14

// ParseRequestHeader decodes a Kafka request header from data.
// Uses slice windowing for the ClientID string to avoid extra allocations.
// Returns ErrFrameTooShort if data is too short to contain a valid header.
func ParseRequestHeader(data []byte) (RequestHeader, error) {
	if len(data) < fixedHeaderLen {
		return RequestHeader{}, ErrFrameTooShort
	}

	size := int32(binary.BigEndian.Uint32(data[0:4]))
	apiKey := int16(binary.BigEndian.Uint16(data[4:6]))
	apiVersion := int16(binary.BigEndian.Uint16(data[6:8]))
	correlationID := int32(binary.BigEndian.Uint32(data[8:12]))
	clientIDLen := int16(binary.BigEndian.Uint16(data[12:14]))

	var clientID string
	switch {
	case clientIDLen == -1:
		// Null string — empty ClientID
		clientID = ""
	case clientIDLen > 0:
		end := fixedHeaderLen + int(clientIDLen)
		if len(data) < end {
			return RequestHeader{}, ErrFrameTooShort
		}
		// Zero-allocation: slice windowing — string shares memory with data
		clientID = btos(data[fixedHeaderLen:end])
	default:
		// clientIDLen == 0 — empty string (distinct from null)
		clientID = ""
	}

	return RequestHeader{
		Size:          size,
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}, nil
}

// ParseResponseHeader decodes a Kafka response header from data.
// Returns ErrFrameTooShort if data is too short.
func ParseResponseHeader(data []byte) (ResponseHeader, error) {
	// Size(4) + CorrelationID(4) = 8
	if len(data) < 8 {
		return ResponseHeader{}, ErrFrameTooShort
	}

	size := int32(binary.BigEndian.Uint32(data[0:4]))
	correlationID := int32(binary.BigEndian.Uint32(data[4:8]))

	return ResponseHeader{
		Size:          size,
		CorrelationID: correlationID,
	}, nil
}

// bufferPool provides reusable byte slices for frame encoding.
var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// WriteFrame encodes a complete Kafka frame from raw header bytes and body bytes.
// The returned slice is owned by the caller until passed to ReleaseFrame.
//
// header should contain the header fields after the Size prefix
// (i.e., api_key + api_version + correlation_id + client_id for requests).
// WriteFrame prepends the correct size.
func WriteFrame(header []byte, body []byte) []byte {
	totalSize := 4 + len(header) + len(body)
	bp := bufferPool.Get().(*[]byte)
	buf := *bp

	if cap(buf) < totalSize {
		// Not enough capacity, allocate a fresh slice
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	// Size prefix excludes itself
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(header)+len(body)))
	copy(buf[4:], header)
	copy(buf[4+len(header):], body)

	*bp = buf
	return buf
}

// ReleaseFrame returns a frame buffer obtained from WriteFrame back to the pool.
// The buffer must not be used after this call.
func ReleaseFrame(buf []byte) {
	if cap(buf) > 0 && len(buf) <= cap(buf) {
		bufferPool.Put(&buf)
	}
}

// btos converts a byte slice to a string without allocation.
// Uses the unsafe trick: since bytes are immutable under string,
// and we never mutate the underlying data, this is safe.
func btos(b []byte) string {
	return string(b)
}
