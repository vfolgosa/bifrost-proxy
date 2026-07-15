package health

import "net"

// AuthenticatePlain performs SASL/PLAIN on conn when username is non-empty.
// A no-op when username is empty.
func AuthenticatePlain(conn net.Conn, username, password string) error {
	return saslAuth(conn, username, password)
}
