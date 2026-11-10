package identitytest

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"net"
	"sync"

	"google.golang.org/grpc/credentials"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/transport/identity"
)

// ServerCredentials are the rotating mTLS credentials of a test's peer
// server (this service serves no internal gRPC itself): every handshake
// uses r's current material, TLS 1.3 only, client certificates required
// and verified against the current bundle, no session tickets, and a
// reload that retires a root closes the connections that root verified.
func ServerCredentials(r *identity.Reloader) credentials.TransportCredentials {
	s := &serverCreds{reloader: r, conns: map[[32]byte]map[net.Conn]struct{}{}}
	r.OnChange(func(_, current *identity.Material) { s.closeRetired(current) })
	return s
}

type serverCreds struct {
	reloader *identity.Reloader
	mu       sync.Mutex
	conns    map[[32]byte]map[net.Conn]struct{}
}

func (s *serverCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	m := s.reloader.Current()
	cfg := &tls.Config{Certificates: []tls.Certificate{m.Certificate}, ClientCAs: m.Pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13, SessionTicketsDisabled: true}
	conn, info, err := credentials.NewTLS(cfg).ServerHandshake(raw)
	if err != nil {
		return nil, nil, err
	}
	chains := info.(credentials.TLSInfo).State.VerifiedChains
	if len(chains) == 0 || len(chains[0]) == 0 {
		_ = conn.Close()
		return nil, nil, errors.New("identitytest: peer chain not verified")
	}
	fp := sha256.Sum256(chains[0][len(chains[0])-1].Raw)
	s.mu.Lock()
	if s.conns[fp] == nil {
		s.conns[fp] = map[net.Conn]struct{}{}
	}
	s.conns[fp][conn] = struct{}{}
	s.mu.Unlock()
	return conn, info, nil
}

// closeRetired closes the connections whose verifying root current no
// longer trusts.
func (s *serverCreds) closeRetired(current *identity.Material) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for fp, set := range s.conns {
		if _, ok := current.Roots[fp]; ok {
			continue
		}
		for c := range set {
			_ = c.Close()
		}
		delete(s.conns, fp)
	}
}

func (s *serverCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("identitytest: server credentials cannot dial")
}

func (s *serverCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", SecurityVersion: "1.3"}
}

func (s *serverCreds) Clone() credentials.TransportCredentials { return s }

func (s *serverCreds) OverrideServerName(string) error { return nil }
