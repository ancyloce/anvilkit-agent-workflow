// Package preparation implements the owner-confirmed fixed PreparationWorkflow
// (DD-01 #preparation-workflow; development plan S2, 2026-09-13): bounded
// requirements analysis on the local fixture route, grouped clarification
// through Control-recorded question sets, one tracked Temporal Update per
// accepted answer set, and the frozen brief. Every read and write happens in an
// Activity against Control's private RPC; Workflow code stays deterministic.
package preparation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts/controlv1/controlv1connect"
)

// ServiceIdentity is the canonical unit name this Worker presents to Control.
const ServiceIdentity = "anvilkit-agent-workflow"

// ControlClientConfig comes from ANVILKIT_WORKFLOW_CONTROL_*: the mTLS
// identity the Control listener requires and the Workflow service credential
// Control maps to the preparation round methods only.
type ControlClientConfig struct {
	Endpoint, CAFile, CertificateFile, KeyFile, Token string
	DialTimeout                                       time.Duration
}

// ControlClientFromEnv reads the six settings; all absent means the preparation
// Worker is not enabled, any other combination is a configuration error.
func ControlClientFromEnv(getenv func(string) string) (ControlClientConfig, bool, error) {
	cfg := ControlClientConfig{Endpoint: getenv("ANVILKIT_WORKFLOW_CONTROL_ENDPOINT"), CAFile: getenv("ANVILKIT_WORKFLOW_CONTROL_CA"), CertificateFile: getenv("ANVILKIT_WORKFLOW_CONTROL_CERT"), KeyFile: getenv("ANVILKIT_WORKFLOW_CONTROL_KEY"), Token: getenv("ANVILKIT_WORKFLOW_CONTROL_TOKEN"), DialTimeout: 5 * time.Second}
	if cfg.Endpoint == "" && cfg.CAFile == "" && cfg.CertificateFile == "" && cfg.KeyFile == "" && cfg.Token == "" {
		return cfg, false, nil
	}
	if cfg.Endpoint == "" || cfg.CAFile == "" || cfg.CertificateFile == "" || cfg.KeyFile == "" || cfg.Token == "" {
		return cfg, false, errors.New("incomplete Control client configuration")
	}
	return cfg, true, nil
}

// NewControlClient builds the Protobuf gRPC client over HTTP/2 with mTLS,
// presenting the Workflow service credential on every call.
func NewControlClient(cfg ControlClientConfig) (controlv1connect.ControlServiceClient, error) {
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("the Control endpoint must be an https base URL")
	}
	if len(cfg.Token) < 32 || len(cfg.Token) > 256 || strings.ContainsAny(cfg.Token, " \t\r\n") {
		return nil, errors.New("Control requires a 32-256 character Workflow credential without whitespace")
	}
	certificate, err := tls.LoadX509KeyPair(cfg.CertificateFile, cfg.KeyFile)
	if err != nil {
		return nil, errors.New("cannot load the Control client certificate and key")
	}
	ca, err := os.ReadFile(cfg.CAFile)
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("cannot load the Control CA")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = true
	transport.DialContext = (&net.Dialer{Timeout: cfg.DialTimeout}).DialContext
	transport.TLSHandshakeTimeout = cfg.DialTimeout
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate}}
	httpClient := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	credential := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			request.Header().Set("Authorization", "Bearer "+cfg.Token)
			return next(ctx, request)
		}
	})
	return controlv1connect.NewControlServiceClient(httpClient, cfg.Endpoint, connect.WithGRPC(), connect.WithInterceptors(credential)), nil
}
