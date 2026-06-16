package protocol

import (
	"encoding/binary"
	"testing"
)

// --- ParseRequestHeader Tests ---

func TestParseRequestHeaderFull(t *testing.T) {
	clientID := "myclient"
	clientIDLen := int16(len(clientID))

	var buf [1024]byte
	binary.BigEndian.PutUint32(buf[0:4], 50)
	binary.BigEndian.PutUint16(buf[4:6], 3)
	binary.BigEndian.PutUint16(buf[6:8], 4)
	binary.BigEndian.PutUint32(buf[8:12], 12345)
	binary.BigEndian.PutUint16(buf[12:14], uint16(clientIDLen))
	copy(buf[14:], clientID)

	h, err := ParseRequestHeader(buf[:14+len(clientID)])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.Size != 50 {
		t.Errorf("Size = %d, want 50", h.Size)
	}
	if h.APIKey != 3 {
		t.Errorf("APIKey = %d, want 3", h.APIKey)
	}
	if h.APIVersion != 4 {
		t.Errorf("APIVersion = %d, want 4", h.APIVersion)
	}
	if h.CorrelationID != 12345 {
		t.Errorf("CorrelationID = %d, want 12345", h.CorrelationID)
	}
	if h.ClientID != clientID {
		t.Errorf("ClientID = %q, want %q", h.ClientID, clientID)
	}
}

func TestParseRequestHeaderNullClientID(t *testing.T) {
	var buf [14]byte
	binary.BigEndian.PutUint32(buf[0:4], 100)
	binary.BigEndian.PutUint16(buf[4:6], 1)
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], 42)
	binary.BigEndian.PutUint16(buf[12:14], 0xFFFF)

	h, err := ParseRequestHeader(buf[:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.ClientID != "" {
		t.Errorf("ClientID = %q, want empty string (null)", h.ClientID)
	}
	if h.CorrelationID != 42 {
		t.Errorf("CorrelationID = %d, want 42", h.CorrelationID)
	}
	if h.Size != 100 {
		t.Errorf("Size = %d, want 100", h.Size)
	}
}

func TestParseRequestHeaderEmptyClientID(t *testing.T) {
	var buf [14]byte
	binary.BigEndian.PutUint32(buf[0:4], 200)
	binary.BigEndian.PutUint16(buf[4:6], 18)
	binary.BigEndian.PutUint16(buf[6:8], 2)
	binary.BigEndian.PutUint32(buf[8:12], 99)
	binary.BigEndian.PutUint16(buf[12:14], 0)

	h, err := ParseRequestHeader(buf[:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.ClientID != "" {
		t.Errorf("ClientID = %q, want empty string", h.ClientID)
	}
	if h.APIKey != 18 {
		t.Errorf("APIKey = %d, want 18", h.APIKey)
	}
}

func TestParseRequestHeaderTruncated(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too short (4 bytes)", make([]byte, 4)},
		{"too short (10 bytes)", make([]byte, 10)},
		{"exactly 13 bytes (one short)", make([]byte, 13)},
		{"header ok but clientID truncated", func() []byte {
			buf := make([]byte, 16)
			binary.BigEndian.PutUint32(buf[0:4], 100)
			binary.BigEndian.PutUint16(buf[12:14], 100)
			return buf
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseRequestHeader(tt.data)
			if err != ErrFrameTooShort {
				t.Errorf("expected ErrFrameTooShort, got %v", err)
			}
		})
	}
}

func TestParseRequestHeaderLongClientID(t *testing.T) {
	clientID := make([]byte, 256)
	for i := range clientID {
		clientID[i] = byte('a' + (i % 26))
	}

	buf := make([]byte, 14+256)
	binary.BigEndian.PutUint32(buf[0:4], 1000)
	binary.BigEndian.PutUint16(buf[4:6], 1)
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], 1)
	binary.BigEndian.PutUint16(buf[12:14], 256)
	copy(buf[14:], clientID)

	h, err := ParseRequestHeader(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.ClientID != string(clientID) {
		t.Errorf("ClientID mismatch")
	}
}

// --- ParseResponseHeader Tests ---

func TestParseResponseHeader(t *testing.T) {
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[0:4], 42)
	binary.BigEndian.PutUint32(buf[4:8], 99)

	h, err := ParseResponseHeader(buf[:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.Size != 42 {
		t.Errorf("Size = %d, want 42", h.Size)
	}
	if h.CorrelationID != 99 {
		t.Errorf("CorrelationID = %d, want 99", h.CorrelationID)
	}
}

func TestParseResponseHeaderTruncated(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"2 bytes", make([]byte, 2)},
		{"4 bytes", make([]byte, 4)},
		{"7 bytes", make([]byte, 7)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseResponseHeader(tt.data)
			if err != ErrFrameTooShort {
				t.Errorf("expected ErrFrameTooShort, got %v", err)
			}
		})
	}
}

// --- WriteFrame Tests ---

func TestWriteFrame(t *testing.T) {
	header := []byte{0x00, 0x01, 0x00, 0x02}
	body := []byte("hello")

	frame := WriteFrame(header, body)
	defer ReleaseFrame(frame)

	if len(frame) != 13 {
		t.Fatalf("frame length = %d, want 13", len(frame))
	}

	size := int32(binary.BigEndian.Uint32(frame[0:4]))
	if size != 9 {
		t.Errorf("Size = %d, want 9", size)
	}

	for i, b := range header {
		if frame[4+i] != b {
			t.Errorf("header byte %d = %d, want %d", i, frame[4+i], b)
		}
	}

	for i, b := range body {
		if frame[8+i] != b {
			t.Errorf("body byte %d = %d, want %d", i, frame[8+i], b)
		}
	}
}

func TestWriteFrameEmptyBody(t *testing.T) {
	header := []byte{0x00, 0x01}
	frame := WriteFrame(header, nil)
	defer ReleaseFrame(frame)

	if len(frame) != 6 {
		t.Fatalf("frame length = %d, want 6", len(frame))
	}
	size := int32(binary.BigEndian.Uint32(frame[0:4]))
	if size != 2 {
		t.Errorf("Size = %d, want 2", size)
	}
}

func TestWriteFrameEmptyHeader(t *testing.T) {
	body := []byte("data")
	frame := WriteFrame(nil, body)
	defer ReleaseFrame(frame)

	if len(frame) != 8 {
		t.Fatalf("frame length = %d, want 8", len(frame))
	}
	size := int32(binary.BigEndian.Uint32(frame[0:4]))
	if size != 4 {
		t.Errorf("Size = %d, want 4", size)
	}
}

func TestWriteFrameLarge(t *testing.T) {
	header := make([]byte, 100)
	body := make([]byte, 500)

	frame := WriteFrame(header, body)
	defer ReleaseFrame(frame)

	if len(frame) != 604 {
		t.Fatalf("frame length = %d, want 604", len(frame))
	}
	size := int32(binary.BigEndian.Uint32(frame[0:4]))
	if size != 600 {
		t.Errorf("Size = %d, want 600", size)
	}
}

func TestWriteFrameBeyondPoolCap(t *testing.T) {
	body := make([]byte, 10000)
	frame := WriteFrame(nil, body)
	defer ReleaseFrame(frame)

	if len(frame) != 10004 {
		t.Fatalf("frame length = %d, want 10004", len(frame))
	}
	size := int32(binary.BigEndian.Uint32(frame[0:4]))
	if size != 10000 {
		t.Errorf("Size = %d, want 10000", size)
	}
}

// --- Round-trip Tests ---

func TestRoundTripRequest(t *testing.T) {
	header := make([]byte, 10)
	binary.BigEndian.PutUint16(header[0:2], 3)
	binary.BigEndian.PutUint16(header[2:4], 5)
	binary.BigEndian.PutUint32(header[4:8], 777)
	binary.BigEndian.PutUint16(header[8:10], 0)

	body := []byte("request-body")

	frame := WriteFrame(header, body)
	defer ReleaseFrame(frame)

	h, err := ParseRequestHeader(frame)
	if err != nil {
		t.Fatalf("ParseRequestHeader: %v", err)
	}

	if h.Size != int32(len(header)+len(body)) {
		t.Errorf("Size = %d, want %d", h.Size, len(header)+len(body))
	}
	if h.APIKey != 3 {
		t.Errorf("APIKey = %d, want 3", h.APIKey)
	}
	if h.APIVersion != 5 {
		t.Errorf("APIVersion = %d, want 5", h.APIVersion)
	}
	if h.CorrelationID != 777 {
		t.Errorf("CorrelationID = %d, want 777", h.CorrelationID)
	}
	if h.ClientID != "" {
		t.Errorf("ClientID = %q, want empty", h.ClientID)
	}

	bodyStart := 14 + len([]byte(h.ClientID))
	if bodyStart > len(frame) {
		t.Fatal("frame too short for body offset")
	}
	parsedBody := frame[bodyStart:]
	if string(parsedBody) != string(body) {
		t.Errorf("body = %q, want %q", string(parsedBody), string(body))
	}
}

func TestRoundTripResponse(t *testing.T) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header[0:4], 999)

	frame := WriteFrame(header, nil)
	defer ReleaseFrame(frame)

	h, err := ParseResponseHeader(frame)
	if err != nil {
		t.Fatalf("ParseResponseHeader: %v", err)
	}
	if h.CorrelationID != 999 {
		t.Errorf("CorrelationID = %d, want 999", h.CorrelationID)
	}
}

// --- Pool Reuse Tests ---

func TestBufferPoolReuse(t *testing.T) {
	frame1 := WriteFrame([]byte{1, 2, 3, 4}, []byte("abc"))
	ReleaseFrame(frame1)

	frame2 := WriteFrame([]byte{5, 6}, []byte("xyz"))
	defer ReleaseFrame(frame2)

	if len(frame2) != 9 {
		t.Errorf("frame2 length = %d, want 9", len(frame2))
	}
	size := int32(binary.BigEndian.Uint32(frame2[0:4]))
	if size != 5 {
		t.Errorf("Size = %d, want 5", size)
	}
}

func TestReleaseFrameSafety(t *testing.T) {
	ReleaseFrame(nil)
	empty := []byte{}
	ReleaseFrame(empty)
}

// --- Specific API Key / Version Tests ---

func TestParseProduceRequestHeaderV9(t *testing.T) {
	clientID := "producer-1"
	var buf [1024]byte
	binary.BigEndian.PutUint32(buf[0:4], 14+uint32(len(clientID)))
	binary.BigEndian.PutUint16(buf[4:6], uint16(APIKeyProduce))
	binary.BigEndian.PutUint16(buf[6:8], 9)
	binary.BigEndian.PutUint32(buf[8:12], 1)
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:], clientID)

	h, err := ParseRequestHeader(buf[:14+len(clientID)])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.APIKey != APIKeyProduce {
		t.Errorf("APIKey = %d, want %d (Produce)", h.APIKey, APIKeyProduce)
	}
	if h.APIVersion != 9 {
		t.Errorf("APIVersion = %d, want 9", h.APIVersion)
	}
	if h.CorrelationID != 1 {
		t.Errorf("CorrelationID = %d, want 1", h.CorrelationID)
	}
	if h.ClientID != "producer-1" {
		t.Errorf("ClientID = %q, want %q", h.ClientID, "producer-1")
	}
}

func TestParseFetchRequestHeaderV12(t *testing.T) {
	clientID := "consumer-1"
	var buf [1024]byte
	binary.BigEndian.PutUint32(buf[0:4], 14+uint32(len(clientID)))
	binary.BigEndian.PutUint16(buf[4:6], uint16(APIKeyFetch))
	binary.BigEndian.PutUint16(buf[6:8], 12)
	binary.BigEndian.PutUint32(buf[8:12], 100)
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:], clientID)

	h, err := ParseRequestHeader(buf[:14+len(clientID)])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.APIKey != APIKeyFetch {
		t.Errorf("APIKey = %d, want %d (Fetch)", h.APIKey, APIKeyFetch)
	}
	if h.APIVersion != 12 {
		t.Errorf("APIVersion = %d, want 12", h.APIVersion)
	}
	if h.CorrelationID != 100 {
		t.Errorf("CorrelationID = %d, want 100", h.CorrelationID)
	}
	if h.ClientID != "consumer-1" {
		t.Errorf("ClientID = %q, want %q", h.ClientID, "consumer-1")
	}
}

func TestParseMetadataRequestHeaderV0(t *testing.T) {
	clientID := "admin"
	var buf [1024]byte
	binary.BigEndian.PutUint32(buf[0:4], 14+uint32(len(clientID)))
	binary.BigEndian.PutUint16(buf[4:6], uint16(APIKeyMetadata))
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], 7)
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:], clientID)

	h, err := ParseRequestHeader(buf[:14+len(clientID)])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.APIKey != APIKeyMetadata {
		t.Errorf("APIKey = %d, want %d (Metadata)", h.APIKey, APIKeyMetadata)
	}
	if h.APIVersion != 0 {
		t.Errorf("APIVersion = %d, want 0", h.APIVersion)
	}
	if h.CorrelationID != 7 {
		t.Errorf("CorrelationID = %d, want 7", h.CorrelationID)
	}
	if h.ClientID != "admin" {
		t.Errorf("ClientID = %q, want %q", h.ClientID, "admin")
	}
}

func TestParseSASLHandshakeHeader(t *testing.T) {
	clientID := "sasl-client"
	var buf [1024]byte
	binary.BigEndian.PutUint32(buf[0:4], 14+uint32(len(clientID)))
	binary.BigEndian.PutUint16(buf[4:6], uint16(APIKeySaslHandshake))
	binary.BigEndian.PutUint16(buf[6:8], 1)
	binary.BigEndian.PutUint32(buf[8:12], 99)
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:], clientID)

	h, err := ParseRequestHeader(buf[:14+len(clientID)])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.APIKey != APIKeySaslHandshake {
		t.Errorf("APIKey = %d, want %d (SASLHandshake)", h.APIKey, APIKeySaslHandshake)
	}
	if h.APIVersion != 1 {
		t.Errorf("APIVersion = %d, want 1", h.APIVersion)
	}
	if h.CorrelationID != 99 {
		t.Errorf("CorrelationID = %d, want 99", h.CorrelationID)
	}
	if h.ClientID != "sasl-client" {
		t.Errorf("ClientID = %q, want %q", h.ClientID, "sasl-client")
	}
}

// --- Truncated Frame (size field bigger than actual data) ---

func TestParseRequestHeaderSizeFieldTooBig(t *testing.T) {
	var buf [14]byte
	binary.BigEndian.PutUint32(buf[0:4], 1000)
	binary.BigEndian.PutUint16(buf[4:6], uint16(APIKeyProduce))
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], 42)
	binary.BigEndian.PutUint16(buf[12:14], 0xFFFF)

	h, err := ParseRequestHeader(buf[:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.Size != 1000 {
		t.Errorf("Size = %d, want 1000", h.Size)
	}
}

// --- All 5 API Keys Recognized ---

func TestAllFiveAPIKeysRecognized(t *testing.T) {
	keys := []struct {
		key  int16
		name string
	}{
		{APIKeyProduce, "Produce"},
		{APIKeyFetch, "Fetch"},
		{APIKeyMetadata, "Metadata"},
		{APIKeySaslHandshake, "SASLHandshake"},
		{APIKeySaslAuthenticate, "SASLAuthenticate"},
	}

	for _, k := range keys {
		t.Run(k.name, func(t *testing.T) {
			if _, ok := SupportedAPIs[k.key]; !ok {
				t.Errorf("API key %d (%s) not found in SupportedAPIs", k.key, k.name)
			}
		})
	}

	if len(keys) != 5 {
		t.Errorf("expected 5 API keys, got %d", len(keys))
	}
}
