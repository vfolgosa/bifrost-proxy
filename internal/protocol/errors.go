package protocol

import "errors"

// Typed errors returned by protocol parsing and validation.
var (
	// ErrUnsupportedAPIKey is returned when the Kafka API key in the request
	// is not in the SupportedAPIs map, meaning the proxy should forward the
	// request without parsing (passthrough mode).
	ErrUnsupportedAPIKey = errors.New("unsupported API key: passthrough mode")

	// ErrUnsupportedVersion is returned when the API version in the request
	// falls outside the MinVersion–MaxVersion range for the given API key.
	ErrUnsupportedVersion = errors.New("API version outside supported range")

	// ErrFrameTooShort is returned when the received frame is shorter than
	// the minimum required length to parse the request header.
	ErrFrameTooShort = errors.New("frame too short to parse request header")
)
