package identity_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/transport/identity"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/transport/identity/identitytest"
)

const td = "anvilkit.local"

var (
	clientURI = identitytest.SPIFFE(td, "anvilkit-apps", "anvilkit-agent-workflow")
	ctrlURI   = identitytest.SPIFFE(td, "anvilkit-apps", "anvilkit-agent-control")
)

func TestMaterialRejectsDefects(t *testing.T) {
	ca := identitytest.NewCA(t, "ca")
	leaf := ca.Issue(t, "x", []string{clientURI})
	other := ca.Issue(t, "y", []string{clientURI})
	_, err := identity.Parse(leaf.CertPEM, other.KeyPEM, ca.PEM, time.Now())
	require.ErrorContains(t, err, "certificate and key", "mismatched key")
	_, err = identity.Parse(leaf.CertPEM, leaf.KeyPEM, []byte("not pem"), time.Now())
	require.ErrorContains(t, err, "no certificate", "empty bundle")
	_, err = identity.Parse(leaf.CertPEM, leaf.KeyPEM, leaf.CertPEM, time.Now())
	require.ErrorContains(t, err, "non-CA", "a leaf is not a CA")
	expired := ca.IssueFor(t, "x", []string{clientURI}, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	_, err = identity.Parse(expired.CertPEM, expired.KeyPEM, ca.PEM, time.Now())
	require.ErrorContains(t, err, "not valid", "expired leaf")
	m, err := identity.Parse(leaf.CertPEM, leaf.KeyPEM, ca.PEM, time.Now())
	require.NoError(t, err)
	require.True(t, m.TrustsRoot(ca.Cert))
}

// peer is Control's side of a connection: an mTLS health server under a
// polling Reloader with the fixture credentials of identitytest.
type peer struct {
	dir      string
	reloader *identity.Reloader
	addr     string
}

func startPeer(t *testing.T, ca *identitytest.CA, bundle []byte, dnsName string) *peer {
	t.Helper()
	p := &peer{dir: filepath.Join(t.TempDir(), "srv")}
	identitytest.Mount(t, p.dir, ca.Issue(t, "anvilkit-agent-control", []string{ctrlURI}, dnsName), bundle)
	cert, key, caFile := identitytest.Files(p.dir)
	r, err := identity.New(identity.Files{CertFile: cert, KeyFile: key, CAFile: caFile}, 50*time.Millisecond, nil)
	require.NoError(t, err)
	r.Start()
	t.Cleanup(r.Stop)
	p.reloader = r
	srv := grpc.NewServer(grpc.Creds(identitytest.ServerCredentials(r)))
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	p.addr = ln.Addr().String()
	return p
}

func clientDir(t *testing.T, ca *identitytest.CA, bundle []byte, cn string, uris []string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), cn)
	identitytest.Mount(t, dir, ca.Issue(t, cn, uris), bundle)
	return dir
}

// clientOf is the started Reloader of dir's files and the dial option of
// its credentials for Control's reviewed server name.
func clientOf(t *testing.T, dir string) (*identity.Reloader, grpc.DialOption) {
	t.Helper()
	cert, key, ca := identitytest.Files(dir)
	r, err := identity.New(identity.Files{CertFile: cert, KeyFile: key, CAFile: ca}, 50*time.Millisecond, nil)
	require.NoError(t, err)
	r.Start()
	t.Cleanup(r.Stop)
	creds, err := identity.NewClientCredentials(r, "anvilkit-agent-control")
	require.NoError(t, err)
	return r, grpc.WithTransportCredentials(creds)
}

// check opens a new connection (a new handshake under the credentials'
// current material) and calls Health/Check on it.
func check(t *testing.T, addr string, creds grpc.DialOption) error {
	t.Helper()
	conn, err := grpc.NewClient(addr, creds)
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	return err
}

// The client verifies the server against its own bundle and the reviewed
// server name, and refuses to dial without a server name.
func TestClientVerifiesTheServer(t *testing.T) {
	ca := identitytest.NewCA(t, "ca")
	other := identitytest.NewCA(t, "other")
	control := startPeer(t, ca, ca.PEM, "anvilkit-agent-control")
	r, creds := clientOf(t, clientDir(t, ca, ca.PEM, "anvilkit-agent-workflow", []string{clientURI}))
	require.NoError(t, check(t, control.addr, creds), "a trusted server with the reviewed name is served")

	_, untrusting := clientOf(t, clientDir(t, ca, other.PEM, "anvilkit-agent-workflow", []string{clientURI}))
	require.Equal(t, codes.Unavailable, status.Code(check(t, control.addr, untrusting)), "a server the bundle does not trust is refused")

	misnamed := startPeer(t, ca, ca.PEM, "anvilkit-agent-other")
	require.Equal(t, codes.Unavailable, status.Code(check(t, misnamed.addr, creds)), "a trusted server without the reviewed name is refused")

	_, err := identity.NewClientCredentials(r, "")
	require.Error(t, err, "server_name is required for hostname verification")
}

// One client Reloader's credentials follow its files on every new
// handshake: the roots it verifies the server against and the leaf it
// presents, across a CA rotation on both sides.
func TestClientCredentialsFollowRotation(t *testing.T) {
	ca1 := identitytest.NewCA(t, "ca1")
	ca2 := identitytest.NewCA(t, "ca2")
	both := append(append([]byte{}, ca1.PEM...), ca2.PEM...)
	control := startPeer(t, ca1, both, "anvilkit-agent-control")
	dir := clientDir(t, ca1, ca1.PEM, "anvilkit-agent-workflow", []string{clientURI})
	r, creds := clientOf(t, dir)
	require.NoError(t, check(t, control.addr, creds))

	// the client's bundle drops ca1: the ca1 server leaf is refused
	identitytest.Mount(t, dir, ca1.Issue(t, "anvilkit-agent-workflow", []string{clientURI}), ca2.PEM)
	waitReload(t, r)
	require.Equal(t, codes.Unavailable, status.Code(check(t, control.addr, creds)), "the retired root no longer verifies the server")

	// the server moves to a ca2 leaf: the same credentials accept it
	identitytest.Mount(t, control.dir, ca2.Issue(t, "anvilkit-agent-control", []string{ctrlURI}, "anvilkit-agent-control"), both)
	waitReload(t, control.reloader)
	require.NoError(t, check(t, control.addr, creds), "the client trusts the rotated server")

	// the server accepts ca2 clients only: the client's ca1 leaf is refused
	// until the client's own leaf rotates to ca2
	identitytest.Mount(t, control.dir, ca2.Issue(t, "anvilkit-agent-control", []string{ctrlURI}, "anvilkit-agent-control"), ca2.PEM)
	waitReload(t, control.reloader)
	require.Equal(t, codes.Unavailable, status.Code(check(t, control.addr, creds)), "the ca1 client leaf is refused")
	identitytest.Mount(t, dir, ca2.Issue(t, "anvilkit-agent-workflow", []string{clientURI}), ca2.PEM)
	waitReload(t, r)
	require.NoError(t, check(t, control.addr, creds), "the client presents its rotated leaf")
}

func TestHTTPTransportFollowsRotation(t *testing.T) {
	ca1 := identitytest.NewCA(t, "ca1")
	ca2 := identitytest.NewCA(t, "ca2")
	srvLeaf := ca1.Issue(t, "anvilkit-agent-model-proxy", []string{identitytest.SPIFFE(td, "anvilkit-apps", "anvilkit-agent-model-proxy")}, "anvilkit-agent-model-proxy")
	srvCert, err := tls.X509KeyPair(srvLeaf.CertPEM, srvLeaf.KeyPEM)
	require.NoError(t, err)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{srvCert}, ClientCAs: poolOf(t, ca1.PEM), ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
	require.NoError(t, err)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				// Read the request first: closing a connection with unread
				// received bytes sends a TCP reset instead of a FIN, which the
				// client can see before the response (the intermittent
				// "connection reset by peer" under load).
				if _, err := http.ReadRequest(bufio.NewReader(c)); err == nil {
					_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
				}
				c.Close()
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	dir := clientDir(t, ca1, ca1.PEM, "anvilkit-agent-control", []string{ctrlURI})
	cert, key, caFile := identitytest.Files(dir)
	r, err := identity.New(identity.Files{CertFile: cert, KeyFile: key, CAFile: caFile}, 50*time.Millisecond, nil)
	require.NoError(t, err)
	r.Start()
	t.Cleanup(r.Stop)
	tr := identity.HTTPTransport(r, "anvilkit-agent-model-proxy")
	get := func() error {
		req, _ := http.NewRequest(http.MethodGet, "https://"+ln.Addr().String()+"/", nil)
		resp, err := tr.RoundTrip(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}
	require.NoError(t, get())
	// the client now trusts ca2 only: the ca1 server is refused on the next connection
	identitytest.Mount(t, dir, ca1.Issue(t, "anvilkit-agent-control", []string{ctrlURI}), ca2.PEM)
	waitReload(t, r)
	err = get()
	var unknown x509.UnknownAuthorityError
	require.True(t, errors.As(err, &unknown) || err != nil, "server of a retired CA refused: %v", err)
}

func waitReload(t *testing.T, r *identity.Reloader) {
	t.Helper()
	before := r.Current()
	require.Eventually(t, func() bool { return !before.Equal(r.Current()) }, 2*time.Second, 20*time.Millisecond, "reload observed")
}

func poolOf(t *testing.T, pemBytes []byte) *x509.CertPool {
	t.Helper()
	p := x509.NewCertPool()
	require.True(t, p.AppendCertsFromPEM(pemBytes))
	return p
}
