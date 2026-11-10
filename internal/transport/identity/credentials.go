// Package identity is the workload identity of the outbound internal
// clients (P0.1, security.md SEC-01): rotating TLS 1.3 gRPC client
// credentials and the HTTP transport of the Model Proxy client, loaded from
// mounted certificate files, verifying each server against the current CA
// bundle and its reviewed server name. A workload certificate identifies a
// service; it never establishes a user, tenant, role or approver. The
// Workflow worker serves no internal gRPC, so it holds no server
// credentials or caller authorization; the tests' peer servers use the
// fixture credentials of identitytest.
package identity

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"

	"google.golang.org/grpc/credentials"
)

// clientCreds dials with the Reloader's current certificate and CA bundle
// and verifies the server through standard hostname verification against
// ServerName (a DNS SAN); no session cache, so nothing resumes under
// retired trust.
type clientCreds struct {
	reloader   *Reloader
	serverName string
}

// NewClientCredentials returns rotating client credentials; serverName is
// the DNS SAN the server certificate must carry.
func NewClientCredentials(r *Reloader, serverName string) (credentials.TransportCredentials, error) {
	if serverName == "" {
		return nil, errors.New("identity: server_name is required for hostname verification")
	}
	return &clientCreds{reloader: r, serverName: serverName}, nil
}

func (c *clientCreds) config() *tls.Config {
	m := c.reloader.Current()
	return &tls.Config{
		Certificates: []tls.Certificate{m.Certificate},
		RootCAs:      m.Pool,
		ServerName:   c.serverName,
		MinVersion:   tls.VersionTLS13,
	}
}

func (c *clientCreds) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return credentials.NewTLS(c.config()).ClientHandshake(ctx, authority, raw)
}

func (c *clientCreds) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("identity: client credentials cannot serve")
}

func (c *clientCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", SecurityVersion: "1.3", ServerName: c.serverName}
}

func (c *clientCreds) Clone() credentials.TransportCredentials {
	return &clientCreds{reloader: c.reloader, serverName: c.serverName}
}

// OverrideServerName keeps the configured name: the reviewed server_name is
// the verification target, never the dialed authority.
func (c *clientCreds) OverrideServerName(string) error { return nil }

// ClientTLSConfig is a standard tls.Config snapshot for non-gRPC clients:
// the certificate follows the Reloader on every handshake, the roots are
// the bundle at the time of the call. Connections that outlive a CA
// retirement are rebuilt by their owner from a fresh snapshot (Reloader
// OnChange); HTTPTransport does that per connection.
func ClientTLSConfig(r *Reloader, serverName string) *tls.Config {
	m := r.Current()
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: serverName,
		RootCAs:    m.Pool,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert := r.Current().Certificate
			return &cert, nil
		},
	}
}

// HTTPTransport is an http.Transport whose every new TLS connection uses the
// Reloader's current material with standard verification; a CA retirement
// closes the idle pool so the next request handshakes under current trust.
func HTTPTransport(r *Reloader, serverName string) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = nil
	t.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		raw, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		conn := tls.Client(raw, ClientTLSConfig(r, serverName))
		if err := conn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, err
		}
		return conn, nil
	}
	r.OnChange(func(previous, current *Material) {
		if len(RetiredRoots(previous, current)) > 0 {
			t.CloseIdleConnections()
		}
	})
	return t
}
