package routing

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/protocol"
)

// buildKafkaRequest constructs a full Kafka request frame: size prefix + header + body.
func buildKafkaRequest(apiKey int16, apiVersion int16, correlationID int32, clientID string, body []byte) []byte {
	clientIDLen := int16(len(clientID))
	headerLen := 2 + 2 + 4 + 2 + len(clientID)
	totalBodyLen := headerLen + len(body)

	buf := make([]byte, 4+totalBodyLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalBodyLen))
	binary.BigEndian.PutUint16(buf[4:6], uint16(apiKey))
	binary.BigEndian.PutUint16(buf[6:8], uint16(apiVersion))
	binary.BigEndian.PutUint32(buf[8:12], uint32(correlationID))
	binary.BigEndian.PutUint16(buf[12:14], uint16(clientIDLen))
	copy(buf[14:14+len(clientID)], clientID)
	copy(buf[14+len(clientID):], body)
	return buf
}

// buildKafkaResponse constructs a Kafka response frame: size prefix + correlationID + body.
func buildKafkaResponse(correlationID int32, body []byte) []byte {
	buf := make([]byte, 4+4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(4+len(body)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(correlationID))
	copy(buf[8:], body)
	return buf
}

// minimalRequest builds the smallest valid frame with just a header (6 bytes + clientIDLen + clientID).
// Size field covers APIKey(2) + APIVersion(2) + CorrelationID(4) + ClientIDLen(2) + ClientID.
func minimalRequest(apiKey int16) []byte {
	// size = 2 + 2 + 4 + 2 = 10 (no client ID, no body)
	buf := make([]byte, 4+10)
	binary.BigEndian.PutUint32(buf[0:4], 10) // frame size (excl size prefix)
	binary.BigEndian.PutUint16(buf[4:6], uint16(apiKey))
	binary.BigEndian.PutUint16(buf[6:8], 0) // apiVersion=0
	binary.BigEndian.PutUint32(buf[8:12], 1) // correlationID=1
	binary.BigEndian.PutUint16(buf[12:14], 0) // clientIDLen=0
	return buf
}


// drainNonSASLFrame reads the remaining bytes of a non-SASL frame from conn
// after HandleSASLExchange has consumed the first bytes of the header.
// This prevents the net.Pipe write from blocking on the other end.
func drainNonSASLFrame(conn net.Conn, result *SASLExchangeResult) {
	if result == nil || result.Header == nil || len(result.Header) < 6 {
		return
	}
	frameSize := int(binary.BigEndian.Uint32(result.Header[0:4]))
	remaining := frameSize - (len(result.Header) - 4) // subtract already-consumed header bytes
	if remaining > 0 {
		drain := make([]byte, remaining)
		io.ReadFull(conn, drain)
	}
}
func TestHandleSASLPassthrough_MatchesAndForwards(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	reqPayload := []byte{0x00, 0x01, 0x00, 0x05, 'P', 'L', 'A', 'I', 'N'}
	request := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 42, "testclient", reqPayload)
	response := buildKafkaResponse(42, []byte{0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	// Upstream: read forwarded request, verify bytes, send response.
	go func() {
		defer wg.Done()
		buf := make([]byte, len(request))
		n, err := upstreamProxy.Read(buf)
		if err != nil {
			t.Errorf("upstreamProxy.Read: %v", err)
			return
		}
		if n != len(request) {
			t.Errorf("upstream received %d bytes, want %d", n, len(request))
		}
		if !bytes.Equal(buf[:n], request) {
			t.Error("upstream received different bytes than client sent")
		}
		if _, err := upstreamProxy.Write(response); err != nil {
			t.Errorf("upstreamProxy.Write: %v", err)
		}
	}()

	// SASL handler.
	go func() {
		defer wg.Done()
		handled, err := HandleSASLPassthrough(proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("HandleSASLPassthrough: %v", err)
		}
		if !handled {
			t.Error("expected handled=true for API Key 36")
		}
	}()

	// Write request from client side.
	if _, err := clientConn.Write(request); err != nil {
		t.Fatalf("clientConn.Write: %v", err)
	}

	// Read response from client side.
	respBuf := make([]byte, len(response))
	n, err := clientConn.Read(respBuf)
	if err != nil {
		t.Fatalf("clientConn.Read response: %v", err)
	}
	if n != len(response) {
		t.Fatalf("client received %d response bytes, want %d", n, len(response))
	}
	if !bytes.Equal(respBuf, response) {
		t.Errorf("client received different response bytes than upstream sent")
	}

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()
}

func TestHandleSASLPassthrough_DoesNotMatchOtherAPIKeys(t *testing.T) {
	tests := []struct {
		name   string
		apiKey int16
	}{
		{"Produce", protocol.APIKeyProduce},
		{"Fetch", protocol.APIKeyFetch},
		{"Metadata", protocol.APIKeyMetadata},
		{"SaslHandshake", protocol.APIKeySaslHandshake},
		{"Unknown", int16(99)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientConn, proxyConn := net.Pipe()
			_, upstreamConn := net.Pipe()

			request := minimalRequest(tt.apiKey)

			// Write the request in a goroutine so HandleSASLPassthrough can
			// read the first 6 bytes and return (false, nil) without blocking.
			go func() {
				clientConn.Write(request)
				clientConn.Close()
			}()

			handled, err := HandleSASLPassthrough(proxyConn, upstreamConn)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if handled {
				t.Errorf("expected handled=false for API key %d (%s)", tt.apiKey, tt.name)
			}

			upstreamConn.Close()
			proxyConn.Close()
		})
	}
}

func TestHandleSASLPassthrough_PreservesPayloadBytesExactly(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	payload := []byte{
		0xFF, 0xFF, 0xFF, 0xFF,
		0x00, 0x00, 0x00, 0x00,
		'S', 'C', 'R', 'A', 'M', '-', 'S', 'H', 'A', '-', '2', '5', '6',
	}
	request := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 9999, "sasltest", payload)
	response := buildKafkaResponse(9999, []byte{0x00, 0x01, 0x00, 0x08, 'S', 'C', 'R', 'A', 'M', '-', '2', '5', '6'})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, len(request))
		n, _ := upstreamProxy.Read(buf)
		if !bytes.Equal(buf[:n], request) {
			t.Errorf("upstream received corrupted bytes")
			for i := 0; i < n && i < len(request); i++ {
				if buf[i] != request[i] {
					t.Logf("byte %d: got 0x%02x, want 0x%02x", i, buf[i], request[i])
					break
				}
			}
		}
		upstreamProxy.Write(response)
	}()

	go func() {
		defer wg.Done()
		HandleSASLPassthrough(proxyConn, upstreamConn)
	}()

	clientConn.Write(request)
	respBuf := make([]byte, len(response))
	n, _ := clientConn.Read(respBuf)
	if !bytes.Equal(respBuf[:n], response) {
		t.Error("client received corrupted response bytes")
	}

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()
}

func TestHandleSASLPassthrough_EmptyBody(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	request := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 0, 7, "", nil)
	response := buildKafkaResponse(7, nil)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, len(request))
		n, _ := upstreamProxy.Read(buf)
		if n != len(request) {
			t.Errorf("upstream got %d bytes, want %d", n, len(request))
		}
		upstreamProxy.Write(response)
	}()

	go func() {
		defer wg.Done()
		handled, err := HandleSASLPassthrough(proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !handled {
			t.Error("expected handled=true for empty body")
		}
	}()

	clientConn.Write(request)
	respBuf := make([]byte, len(response))
	n, _ := clientConn.Read(respBuf)
	if n != len(response) {
		t.Errorf("client got %d response bytes, want %d", n, len(response))
	}

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()
}

func TestHandleSASLPassthrough_LargePayload(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	largePayload := make([]byte, 10000)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	request := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 1, "large-client", largePayload)
	largeResponse := make([]byte, 8000)
	for i := range largeResponse {
		largeResponse[i] = byte((i * 3) % 256)
	}
	response := buildKafkaResponse(1, largeResponse)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, len(request))
		total := 0
		for total < len(request) {
			n, err := upstreamProxy.Read(buf[total:])
			if err != nil {
				t.Errorf("upstream read error at offset %d: %v", total, err)
				return
			}
			total += n
		}
		if !bytes.Equal(buf, request) {
			t.Error("large payload corrupted in transit to upstream")
		}
		upstreamProxy.Write(response)
	}()

	go func() {
		defer wg.Done()
		HandleSASLPassthrough(proxyConn, upstreamConn)
	}()

	// Write in chunks.
	for offset := 0; offset < len(request); {
		chunk := 1024
		if offset+chunk > len(request) {
			chunk = len(request) - offset
		}
		clientConn.Write(request[offset : offset+chunk])
		offset += chunk
	}

	respBuf := make([]byte, len(response))
	total := 0
	for total < len(response) {
		n, err := clientConn.Read(respBuf[total:])
		if err != nil {
			t.Fatalf("client read error at offset %d: %v", total, err)
		}
		total += n
	}
	if !bytes.Equal(respBuf, response) {
		t.Error("large response corrupted in transit to client")
	}

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()
}

// ── HandleSASLExchange Tests ────────────────────────────────────────

// TestHandleSASLExchange_SingleHandshakeSingleAuthenticate tests the complete
// SASL flow: a single SaslHandshake request/response followed by a single
// SaslAuthenticate request/response, then a non-SASL frame.
func TestHandleSASLExchange_SingleHandshakeSingleAuthenticate(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	// Build SaslHandshake request (API 17, v1, no body)
	handshakeReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 100, "testclient", nil)
	// Build SaslHandshake response with SCRAM-SHA-256 mechanism
	handshakeResp := buildKafkaResponse(100, []byte{
		0x00, 0x00, // error_code=0
		0x00, 0x01, // mechanism count=1
		0x00, 0x0A, 'S', 'C', 'R', 'A', 'M', '-', 'S', 'H', 'A', '-', '2', '5', '6',
	})

	// Build SaslAuthenticate request (API 36, v1 with SCRAM client-first message)
	authReq := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 101, "testclient",
		[]byte{0x00, 0x00, 0x00, 0x14, 'n', ',', ',', 'n', '=', 'u', '=', 'r', '=', '1', '2', '3', '4', '5'})
	// Build SaslAuthenticate response (error_code=0 success)
	authResp := buildKafkaResponse(101, []byte{
		0x00, 0x00, // error_code=0 (success)
		0xFF, 0xFF, // error_message null
		0x00, 0x00, 0x00, 0x18, // auth_bytes length=24
		'r', '=', '1', '2', '3', '4', '5', 's', '=', 'A', '=', ',', 'i', '=', '4', '0', '9', '6',
	})

	// Non-SASL frame that follows (Metadata request)
	metadataReq := buildKafkaRequest(protocol.APIKeyMetadata, 0, 200, "testclient", []byte{0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	// Upstream goroutine: responds to SASL frames in order
	go func() {
		defer wg.Done()

		// 1. Read and verify SaslHandshake request
		buf := make([]byte, len(handshakeReq))
		n, err := upstreamProxy.Read(buf)
		if err != nil {
			t.Errorf("upstream read handshake: %v", err)
			return
		}
		if !bytes.Equal(buf[:n], handshakeReq) {
			t.Errorf("handshake request corrupted: got %d bytes, want %d", n, len(handshakeReq))
		}
		if _, err := upstreamProxy.Write(handshakeResp); err != nil {
			t.Errorf("upstream write handshake resp: %v", err)
			return
		}

		// 2. Read and verify SaslAuthenticate request
		buf2 := make([]byte, len(authReq))
		n2, err := upstreamProxy.Read(buf2)
		if err != nil {
			t.Errorf("upstream read authenticate: %v", err)
			return
		}
		if !bytes.Equal(buf2[:n2], authReq) {
			t.Errorf("authenticate request corrupted: got %d bytes, want %d", n2, len(authReq))
		}
		if _, err := upstreamProxy.Write(authResp); err != nil {
			t.Errorf("upstream write auth resp: %v", err)
			return
		}
	}()

	// Proxy goroutine: runs HandleSASLExchange
	go func() {
		defer wg.Done()
		result, err := HandleSASLExchange(&handler, proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("HandleSASLExchange: %v", err)
			return
		}
		// firstBytes should be the 6-byte header of the non-SASL frame (Metadata)
		if result == nil || result.Header == nil {
			t.Error("expected non-nil firstBytes for non-SASL frame")
		} else {
			// Verify it's Metadata (API Key 3)
			if len(result.Header) >= 6 {
				apiKey := int16(binary.BigEndian.Uint16(result.Header[4:6]))
				if apiKey != protocol.APIKeyMetadata {
					t.Errorf("expected API Key 3 (Metadata), got %d", apiKey)
				}
			}
		}
		drainNonSASLFrame(proxyConn, result)
	}()

	// Write SASL frames from client side
	if _, err := clientConn.Write(handshakeReq); err != nil {
		t.Fatalf("client write handshake: %v", err)
	}
	// Read handshake response
	hsRespBuf := make([]byte, len(handshakeResp))
	if _, err := clientConn.Read(hsRespBuf); err != nil {
		t.Fatalf("client read handshake resp: %v", err)
	}
	if !bytes.Equal(hsRespBuf, handshakeResp) {
		t.Error("handshake response corrupted")
	}

	if _, err := clientConn.Write(authReq); err != nil {
		t.Fatalf("client write authenticate: %v", err)
	}
	// Read authenticate response
	authRespBuf := make([]byte, len(authResp))
	if _, err := clientConn.Read(authRespBuf); err != nil {
		t.Fatalf("client read auth resp: %v", err)
	}
	if !bytes.Equal(authRespBuf, authResp) {
		t.Error("authenticate response corrupted")
	}

	// Write non-SASL frame to unblock HandleSASLExchange
	if _, err := clientConn.Write(metadataReq); err != nil {
		t.Fatalf("client write metadata: %v", err)
	}

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()

	// Verify authentication state
	if !handler.Authenticated() {
		t.Error("expected handler to be authenticated after successful SASL")
	}
}

// TestHandleSASLExchange_MultiStepSCRAM tests a multi-step SCRAM SASL exchange
// with multiple SaslAuthenticate round-trips (client-first, server-first, client-final).
func TestHandleSASLExchange_MultiStepSCRAM(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	// SaslHandshake
	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 1, "scram", nil)
	hsResp := buildKafkaResponse(1, []byte{
		0x00, 0x00, // error_code=0
		0x00, 0x01, // mechanism count=1
		0x00, 0x0A, 'S', 'C', 'R', 'A', 'M', '-', 'S', 'H', 'A', '-', '2', '5', '6',
	})

	// Step 1: client-first message
	auth1Req := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 2, "scram",
		[]byte{0x00, 0x00, 0x00, 0x0E, 'n', ',', ',', 'n', '=', 'u', 's', 'e', 'r', ',', 'r', '=', 'c', 'l', 'n', 'o', 'n', 'c', 'e'})
	// Step 1 response: error_code=0 with server-first message
	auth1Resp := buildKafkaResponse(2, []byte{
		0x00, 0x00, // error_code=0
		0xFF, 0xFF, // error_message null
		0x00, 0x00, 0x00, 0x10, // auth_bytes length=16
		'r', '=', 'c', 'l', 'n', 'o', 'n', 'c', 'e', 's', 'r', 'v', 'n', 'o', 'n', 'c',
	})

	// Step 2: client-final message
	auth2Req := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 3, "scram",
		[]byte{0x00, 0x00, 0x00, 0x22, 'c', '=', 'b', 'i', 'd', 'x', ',', 'r', '=', 'c', 'l', 'n', 'o', 'n', 'c', 'e', 's', 'r', 'v', 'n', 'o', 'n', 'c', ',', 'p', '=', 'A', 'B', 'C', 'D'})

	// Step 2 response: error_code=0 with server-final message (success!)
	auth2Resp := buildKafkaResponse(3, []byte{
		0x00, 0x00, // error_code=0 (success!)
		0xFF, 0xFF, // error_message null
		0x00, 0x00, 0x00, 0x06, // auth_bytes length=6
		'v', '=', 's', 'e', 'r', 'v', '=', 's', 'i', 'g',
	})

	// Non-SASL frame after authentication
	nonSASL := buildKafkaRequest(protocol.APIKeyMetadata, 0, 99, "scram", []byte{0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		// 1. Handshake

		buf := make([]byte, len(hsReq))
		upstreamProxy.Read(buf)
		upstreamProxy.Write(hsResp)
		// 2. Auth step 1
		buf1 := make([]byte, len(auth1Req))
		upstreamProxy.Read(buf1)
		upstreamProxy.Write(auth1Resp)
		// 3. Auth step 2
		buf2 := make([]byte, len(auth2Req))
		upstreamProxy.Read(buf2)
		upstreamProxy.Write(auth2Resp)
	}()

	go func() {
		defer wg.Done()
		result, err := HandleSASLExchange(&handler, proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("HandleSASLExchange: %v", err)
			return
		}
		if result == nil || result.Header == nil {
			t.Error("expected non-nil firstBytes for non-SASL frame")
		}
		drainNonSASLFrame(proxyConn, result)
	}()

	// Client sends handshake
	clientConn.Write(hsReq)
	resp := make([]byte, len(hsResp))
	clientConn.Read(resp)

	// Client sends auth step 1
	clientConn.Write(auth1Req)
	resp1 := make([]byte, len(auth1Resp))
	clientConn.Read(resp1)

	// Client sends auth step 2
	clientConn.Write(auth2Req)
	resp2 := make([]byte, len(auth2Resp))
	clientConn.Read(resp2)
	if !bytes.Equal(resp2, auth2Resp) {
		t.Error("auth step 2 response corrupted")
	}

	// Send non-SASL to unblock
	clientConn.Write(nonSASL)

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()

	if !handler.Authenticated() {
		t.Error("expected authenticated after multi-step SCRAM")
	}
}

// TestHandleSASLExchange_AuthenticationState verifies that the SASLHandler.Authenticated()
// flag is correctly set based on error_code in SaslAuthenticate responses.
func TestHandleSASLExchange_AuthenticationState(t *testing.T) {
	// Case 1: error_code=0 → authenticated
	t.Run("success", func(t *testing.T) {
		clientConn, proxyConn := net.Pipe()
		upstreamProxy, upstreamConn := net.Pipe()

		handler := SASLHandler{}

		hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 1, "", nil)
		hsResp := buildKafkaResponse(1, []byte{0x00, 0x00, 0x00, 0x00})

		authReq := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 2, "", []byte{0x00, 0x00, 0x00, 0x04, 't', 'e', 's', 't'})
		authResp := buildKafkaResponse(2, []byte{
			0x00, 0x00, // error_code=0
			0xFF, 0xFF, // null error message
			0x00, 0x00, 0x00, 0x04, 'o', 'k', 'a', 'y',
		})

		nonSASL := buildKafkaRequest(protocol.APIKeyProduce, 0, 99, "", []byte{0x00, 0x00})

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			buf := make([]byte, len(hsReq))
			upstreamProxy.Read(buf)
			upstreamProxy.Write(hsResp)
			buf2 := make([]byte, len(authReq))
			upstreamProxy.Read(buf2)
			upstreamProxy.Write(authResp)
		}()

		go func() {
			defer wg.Done()
			result, _ := HandleSASLExchange(&handler, proxyConn, upstreamConn)
			drainNonSASLFrame(proxyConn, result)
		}()

		clientConn.Write(hsReq)
		r := make([]byte, len(hsResp))
		clientConn.Read(r)
		clientConn.Write(authReq)
		r2 := make([]byte, len(authResp))
		clientConn.Read(r2)
		clientConn.Write(nonSASL)

		wg.Wait()
		clientConn.Close()
		upstreamProxy.Close()

		if !handler.Authenticated() {
			t.Error("expected authenticated=true after error_code=0")
		}
	})

	// Case 2: error_code != 0 → NOT authenticated
	t.Run("failure", func(t *testing.T) {
		clientConn, proxyConn := net.Pipe()
		upstreamProxy, upstreamConn := net.Pipe()

		handler := SASLHandler{}

		hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 1, "", nil)
		hsResp := buildKafkaResponse(1, []byte{0x00, 0x00, 0x00, 0x00})

		authReq := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 2, "", []byte{0x00, 0x00, 0x00, 0x04, 'b', 'a', 'd', '!'})
		authResp := buildKafkaResponse(2, []byte{
			0x00, 0x33, // error_code=51 (SASL_AUTHENTICATION_FAILED)
			0x00, 0x0F, 'A', 'u', 't', 'h', ' ', 'f', 'a', 'i', 'l', 'e', 'd', '!', '!', '!', // error message
			0xFF, 0xFF, 0xFF, 0xFF, // auth_bytes = -1 (null)
		})

		nonSASL := buildKafkaRequest(protocol.APIKeyProduce, 0, 99, "", []byte{0x00, 0x00})

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			buf := make([]byte, len(hsReq))
			upstreamProxy.Read(buf)
			upstreamProxy.Write(hsResp)
			buf2 := make([]byte, len(authReq))
			upstreamProxy.Read(buf2)
			upstreamProxy.Write(authResp)
		}()

		go func() {
			defer wg.Done()
			result, _ := HandleSASLExchange(&handler, proxyConn, upstreamConn)
			drainNonSASLFrame(proxyConn, result)
		}()

		clientConn.Write(hsReq)
		r := make([]byte, len(hsResp))
		clientConn.Read(r)
		clientConn.Write(authReq)
		r2 := make([]byte, len(authResp))
		clientConn.Read(r2)
		clientConn.Write(nonSASL)

		wg.Wait()
		clientConn.Close()
		upstreamProxy.Close()

		if handler.Authenticated() {
			t.Error("expected authenticated=false after error_code=51 (SASL_AUTHENTICATION_FAILED)")
		}
	})
}

// TestHandleSASLExchange_NonSASLFrameHeader tests that after SASL completes,
// the first non-SASL frame's 6-byte header is correctly returned.
func TestHandleSASLExchange_NonSASLFrameHeader(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	// Complete handshake
	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 1, "", nil)
	hsResp := buildKafkaResponse(1, []byte{0x00, 0x00, 0x00, 0x00})

	// Non-SASL produce request with known payload
	produceBody := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 't', 'o', 'p', 'i', 'c'}
	produceReq := buildKafkaRequest(protocol.APIKeyProduce, 2, 42, "cli", produceBody)

	var wg sync.WaitGroup
	wg.Add(2)

	var saslResult *SASLExchangeResult

	go func() {
		defer wg.Done()
		buf := make([]byte, len(hsReq))
		upstreamProxy.Read(buf)
		upstreamProxy.Write(hsResp)
	}()

	go func() {
		defer wg.Done()
		var err error
		saslResult, err = HandleSASLExchange(&handler, proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("HandleSASLExchange: %v", err)
		}
		drainNonSASLFrame(proxyConn, saslResult)
	}()

	clientConn.Write(hsReq)
	r := make([]byte, len(hsResp))
	clientConn.Read(r)

	// Now send non-SASL frame
	clientConn.Write(produceReq)

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()

	if saslResult == nil || saslResult.Header == nil {
		t.Fatal("expected non-nil firstBytes")
	}
	if len(saslResult.Header) < 14 {
		t.Fatalf("expected at least 14-byte header, got %d bytes", len(saslResult.Header))
	}

	apiKey := int16(binary.BigEndian.Uint16(saslResult.Header[4:6]))
	if apiKey != protocol.APIKeyProduce {
		t.Errorf("expected APIKeyProduce (0) in returned header, got %d", apiKey)
	}
	size := int32(binary.BigEndian.Uint32(saslResult.Header[0:4]))
	expectedSize := int32(2 + 2 + 4 + 2 + 3 + len(produceBody)) // apiKey+apiVersion+corrID+clientIDLen+"cli"+body
	if size != expectedSize {
		t.Errorf("expected frame size %d, got %d", expectedSize, size)
	}
}

// TestHandleSASLExchange_OnlyHandshake_NoAuthenticate tests that when only
// a SaslHandshake is sent (no Authenticate), followed by a non-SASL frame,
// the header is correctly returned.
func TestHandleSASLExchange_OnlyHandshake_NoAuthenticate(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 100, "", nil)
	hsResp := buildKafkaResponse(100, []byte{0x00, 0x00, 0x00, 0x00})
	nonSASL := buildKafkaRequest(protocol.APIKeyFetch, 0, 200, "", []byte{0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	var saslResult *SASLExchangeResult

	go func() {
		defer wg.Done()
		buf := make([]byte, len(hsReq))
		upstreamProxy.Read(buf)
		upstreamProxy.Write(hsResp)
	}()

	go func() {
		defer wg.Done()
		var err error
		saslResult, err = HandleSASLExchange(&handler, proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("HandleSASLExchange: %v", err)
		}
		drainNonSASLFrame(proxyConn, saslResult)
	}()

	clientConn.Write(hsReq)
	r := make([]byte, len(hsResp))
	clientConn.Read(r)
	clientConn.Write(nonSASL)

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()

	if saslResult == nil || saslResult.Header == nil {
		t.Fatal("expected non-nil firstBytes")
	}
	apiKey := int16(binary.BigEndian.Uint16(saslResult.Header[4:6]))
	if apiKey != protocol.APIKeyFetch {
		t.Errorf("expected APIKeyFetch (1), got %d", apiKey)
	}
	// Not authenticated since no SaslAuthenticate happened
	if handler.Authenticated() {
		t.Error("should not be authenticated without SaslAuthenticate")
	}
}

// TestHandleSASLExchange_NoSASLAtAll verifies that when the first frame is
// not SASL, its header is returned immediately without any forwarding.
func TestHandleSASLExchange_NoSASLAtAll(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}
	produceReq := buildKafkaRequest(protocol.APIKeyProduce, 0, 1, "", []byte{0xAA, 0xBB})

	var wg sync.WaitGroup
	wg.Add(1)

	var saslResult *SASLExchangeResult

	go func() {
		defer wg.Done()
		var err error
		saslResult, err = HandleSASLExchange(&handler, proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("HandleSASLExchange: %v", err)
		}
		drainNonSASLFrame(proxyConn, saslResult)
	}()

	// Send non-SASL frame
	clientConn.Write(produceReq)

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()
	upstreamConn.Close()

	if saslResult == nil || saslResult.Header == nil {
		t.Fatal("expected non-nil firstBytes")
	}
	apiKey := int16(binary.BigEndian.Uint16(saslResult.Header[4:6]))
	if apiKey != protocol.APIKeyProduce {
		t.Errorf("expected APIKeyProduce (0), got %d", apiKey)
	}
	if handler.Authenticated() {
		t.Error("should not be authenticated with no SASL frames")
	}
}

// TestHandleSASLExchange_LargePayload verifies that large SASL payloads
// are forwarded byte-for-byte without corruption.
func TestHandleSASLExchange_LargePayload(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 1, "large", nil)
	hsResp := buildKafkaResponse(1, []byte{0x00, 0x00, 0x00, 0x00})

	// Large SCRAM payload
	largePayload := make([]byte, 50000)
	for i := range largePayload {
		largePayload[i] = byte((i * 7) % 256)
	}
	authReq := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 2, "large", largePayload)

	largeRespPayload := make([]byte, 40000)
	for i := range largeRespPayload {
		largeRespPayload[i] = byte((i * 13) % 256)
	}
	authResp := buildKafkaResponse(2, largeRespPayload)

	nonSASL := buildKafkaRequest(protocol.APIKeyMetadata, 0, 99, "large", []byte{0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	var saslResult *SASLExchangeResult

	go func() {
		defer wg.Done()
		// Handshake
		buf := make([]byte, len(hsReq))
		upstreamProxy.Read(buf)
		upstreamProxy.Write(hsResp)
		// Authenticate — read in chunks
		buf2 := make([]byte, len(authReq))
		total := 0
		for total < len(buf2) {
			n, err := upstreamProxy.Read(buf2[total:])
			if err != nil {
				t.Errorf("upstream read large auth at %d: %v", total, err)
				return
			}
			total += n
		}
		if !bytes.Equal(buf2, authReq) {
			t.Error("large auth request corrupted")
		}
		upstreamProxy.Write(authResp)
	}()

	go func() {
		defer wg.Done()
		var err error
		saslResult, err = HandleSASLExchange(&handler, proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("HandleSASLExchange: %v", err)
		}
		drainNonSASLFrame(proxyConn, saslResult)
	}()

	// Write handshake
	clientConn.Write(hsReq)
	resp := make([]byte, len(hsResp))
	clientConn.Read(resp)

	// Write large auth request in chunks
	for offset := 0; offset < len(authReq); {
		chunk := 2048
		if offset+chunk > len(authReq) {
			chunk = len(authReq) - offset
		}
		clientConn.Write(authReq[offset : offset+chunk])
		offset += chunk
	}

	// Read large response in chunks
	respBuf := make([]byte, len(authResp))
	total := 0
	for total < len(respBuf) {
		n, err := clientConn.Read(respBuf[total:])
		if err != nil {
			t.Fatalf("client read large resp at %d: %v", total, err)
		}
		total += n
	}
	if !bytes.Equal(respBuf, authResp) {
		t.Error("large auth response corrupted")
	}

	clientConn.Write(nonSASL)

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()

	if saslResult == nil || saslResult.Header == nil {
		t.Error("expected non-nil firstBytes")
	}
}

// TestHandleSASLExchange_BinaryPayload verifies that SASL payloads containing
// null bytes and arbitrary binary data are preserved exactly.
func TestHandleSASLExchange_BinaryPayload(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 1, "", nil)
	hsResp := buildKafkaResponse(1, []byte{0x00, 0x00, 0x00, 0x00})

	// Binary payload with null bytes, high bytes, and all byte values
	binaryPayload := []byte{
		0x00, 0x00, 0x00, 0x00,
		0xFF, 0xFF, 0xFF, 0xFF,
		0x80, 0x81, 0xFE, 0x7F,
		0x00, 0x01, 0x02, 0x03,
		'A', 'B', 'C', 'D',
		0x00, // embedded null
		'E', 'F', 'G',
	}
	authReq := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 42, "bin", binaryPayload)

	binaryRespPayload := []byte{
		0x00, 0x00, // error_code
		0xFF, 0xFF, // null error_message
		0x00, 0x00, 0x00, 0x08,
		0xDE, 0xAD, 0xBE, 0xEF,
		0xCA, 0xFE, 0xBA, 0xBE,
	}
	authResp := buildKafkaResponse(42, binaryRespPayload)

	nonSASL := buildKafkaRequest(protocol.APIKeyMetadata, 0, 99, "bin", []byte{0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, len(hsReq))
		upstreamProxy.Read(buf)
		upstreamProxy.Write(hsResp)
		buf2 := make([]byte, len(authReq))
		n, _ := upstreamProxy.Read(buf2)

		if !bytes.Equal(buf2[:n], authReq) {
			t.Error("binary auth request corrupted")
		}
		upstreamProxy.Write(authResp)
	}()

	go func() {
		defer wg.Done()
		result, _ := HandleSASLExchange(&handler, proxyConn, upstreamConn)
		drainNonSASLFrame(proxyConn, result)
	}()

	clientConn.Write(hsReq)
	r := make([]byte, len(hsResp))
	clientConn.Read(r)

	clientConn.Write(authReq)
	r2 := make([]byte, len(authResp))
	n, _ := clientConn.Read(r2)
	if !bytes.Equal(r2[:n], authResp) {
		t.Error("binary auth response corrupted")
	}

	clientConn.Write(nonSASL)

	wg.Wait()

	clientConn.Close()
	upstreamProxy.Close()
}

// TestHandleSASLExchange_ClientClosesMidExchange verifies error handling when
// the client closes the connection during a SASL exchange.
func TestHandleSASLExchange_ClientClosesMidExchange(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	_, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	// Send partial SASL data then close — must do this in a goroutine
	// because net.Pipe.Write blocks until the reader consumes data.
	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 1, "", nil)
	go func() {
		clientConn.Write(hsReq[:2]) // partial header only
		clientConn.Close()
	}()

	_, err := HandleSASLExchange(&handler, proxyConn, upstreamConn)
	if err == nil {
		t.Error("expected error when client closes mid-exchange")
	}

	upstreamConn.Close()
	proxyConn.Close()
}

// TestHandleSASLExchange_UpstreamClosesMidExchange verifies error handling when
// the upstream closes during a SASL exchange.
func TestHandleSASLExchange_UpstreamClosesMidExchange(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 1, "", nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		// Read the request but close upstream instead of responding
		buf := make([]byte, len(hsReq))
		upstreamProxy.Read(buf)
		upstreamProxy.Close() // kill upstream mid-exchange
	}()

	var exchangeErr error
	go func() {
		_, exchangeErr = HandleSASLExchange(&handler, proxyConn, upstreamConn)
	}()

	clientConn.Write(hsReq)

	wg.Wait()
	time.Sleep(50 * time.Millisecond) // let HandleSASLExchange observe the closed upstream

	if exchangeErr == nil {
		t.Error("expected error when upstream closes mid-exchange")
	}
	clientConn.Close()
}

// TestHandleSASLExchange_EmptyBody verifies that SASL frames with empty body
// (only header, no payload) are handled correctly.
func TestHandleSASLExchange_EmptyBody(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	// Empty body SaslHandshake
	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 0, 1, "", nil)
	hsResp := buildKafkaResponse(1, []byte{0x00, 0x00, 0x00, 0x00})

	// Empty body SaslAuthenticate (no auth_bytes)
	authReq := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 0, 2, "", nil)
	authResp := buildKafkaResponse(2, []byte{
		0x00, 0x00, // error_code=0
		0xFF, 0xFF, // null error message
		0xFF, 0xFF, 0xFF, 0xFF, // auth_bytes = -1 (null)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // session_lifetime_ms = 0
	})

	nonSASL := buildKafkaRequest(protocol.APIKeyMetadata, 0, 99, "", []byte{0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, len(hsReq))
		upstreamProxy.Read(buf)
		upstreamProxy.Write(hsResp)
		buf2 := make([]byte, len(authReq))
		upstreamProxy.Read(buf2)
		upstreamProxy.Write(authResp)
	}()

	go func() {
		defer wg.Done()
		result, err := HandleSASLExchange(&handler, proxyConn, upstreamConn)
		if err != nil {
			t.Errorf("HandleSASLExchange: %v", err)
		}
		drainNonSASLFrame(proxyConn, result)
	}()

	clientConn.Write(hsReq)
	r := make([]byte, len(hsResp))
	clientConn.Read(r)
	clientConn.Write(authReq)
	r2 := make([]byte, len(authResp))
	clientConn.Read(r2)
	clientConn.Write(nonSASL)

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()

	if !handler.Authenticated() {
		t.Error("expected authenticated after successful empty-body exchange")
	}
}

// TestHandleSASLExchange_PreservesBytesExactly verifies exact byte-for-byte
// preservation through the SASL exchange for both Handshake and Authenticate.
func TestHandleSASLExchange_PreservesBytesExactly(t *testing.T) {
	clientConn, proxyConn := net.Pipe()
	upstreamProxy, upstreamConn := net.Pipe()

	handler := SASLHandler{}

	// Handshake with specific binary patterns
	hsBody := []byte{0x01, 0x02, 0x03, 0x04, 0xFF, 0xFE, 0xFD, 0xFC}
	hsReq := buildKafkaRequest(protocol.APIKeySaslHandshake, 1, 42, "exact", hsBody)
	hsResp := buildKafkaResponse(42, []byte{
		0x00, 0x00,
		0x00, 0x01,
		0x00, 0x0C, 'S', 'C', 'R', 'A', 'M', '-', 'S', 'H', 'A', '-', '5', '1', '2',
	})

	// Authenticate with specific byte pattern
	authBody := []byte{0xAA, 0x55, 0xAA, 0x55, 0x00, 0xFF, 0x00, 0xFF}
	authReq := buildKafkaRequest(protocol.APIKeySaslAuthenticate, 1, 42, "exact", authBody)
	authResp := buildKafkaResponse(42, []byte{
		0x00, 0x00,
		0xFF, 0xFF,
		0x00, 0x00, 0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF,
	})

	nonSASL := buildKafkaRequest(protocol.APIKeyProduce, 0, 99, "exact", []byte{0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		// Verify handshake bytes exactly
		buf := make([]byte, len(hsReq))
		n, _ := upstreamProxy.Read(buf)
		if !bytes.Equal(buf[:n], hsReq) {
			t.Error("handshake bytes corrupted")
			for i := 0; i < n && i < len(hsReq); i++ {
				if buf[i] != hsReq[i] {
					t.Logf("handshake byte %d: got 0x%02x, want 0x%02x", i, buf[i], hsReq[i])
					break
				}
			}
		}
		upstreamProxy.Write(hsResp)

		// Verify authenticate bytes exactly
		buf2 := make([]byte, len(authReq))
		n2, _ := upstreamProxy.Read(buf2)
		if !bytes.Equal(buf2[:n2], authReq) {
			t.Error("authenticate bytes corrupted")
			for i := 0; i < n2 && i < len(authReq); i++ {
				if buf2[i] != authReq[i] {
					t.Logf("auth byte %d: got 0x%02x, want 0x%02x", i, buf2[i], authReq[i])
					break
				}
			}
		}
		upstreamProxy.Write(authResp)
	}()

	go func() {
		defer wg.Done()
		result, _ := HandleSASLExchange(&handler, proxyConn, upstreamConn)
		drainNonSASLFrame(proxyConn, result)
	}()

	// Send handshake
	clientConn.Write(hsReq)
	r := make([]byte, len(hsResp))
	n, _ := clientConn.Read(r)
	if !bytes.Equal(r[:n], hsResp) {
		t.Error("handshake response corrupted")
	}

	// Send authenticate
	clientConn.Write(authReq)
	r2 := make([]byte, len(authResp))
	n2, _ := clientConn.Read(r2)
	if !bytes.Equal(r2[:n2], authResp) {
		t.Error("authenticate response corrupted")
	}

	clientConn.Write(nonSASL)

	wg.Wait()
	clientConn.Close()
	upstreamProxy.Close()
}

