package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/config"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/transport/identity"
)

// identities holds the process's workload identity (P0.1): one Reloader
// of the mounted files, shared by every mtls client whose block names no
// files of its own; a client with its own files gets its own Reloader.
type identities struct {
	cfg     config.Config
	log     *slog.Logger
	own     *identity.Reloader
	started []*identity.Reloader
}

func newIdentities(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*identities, error) {
	ids := &identities{cfg: cfg, log: log}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			for _, r := range ids.started {
				r.Start()
			}
			return nil
		},
		OnStop: func(context.Context) error {
			for _, r := range ids.started {
				r.Stop()
			}
			return nil
		},
	})
	return ids, nil
}

// reloader returns the Reloader of a resolved file set, sharing the
// process's own identity when the files are its own.
func (ids *identities) reloader(files config.MTLSFiles) (*identity.Reloader, error) {
	own := config.MTLSFiles{CertFile: ids.cfg.Identity.CertFile, KeyFile: ids.cfg.Identity.KeyFile, CAFile: ids.cfg.Identity.CAFile}
	if files.CertFile == own.CertFile && files.KeyFile == own.KeyFile && files.CAFile == own.CAFile && ids.own != nil {
		return ids.own, nil
	}
	r, err := identity.New(identity.Files{CertFile: files.CertFile, KeyFile: files.KeyFile, CAFile: files.CAFile}, ids.cfg.Identity.ReloadInterval, ids.log)
	if err != nil {
		return nil, err
	}
	ids.started = append(ids.started, r)
	if files.CertFile == own.CertFile && files.KeyFile == own.KeyFile && files.CAFile == own.CAFile {
		ids.own = r
	}
	return r, nil
}

// client is the transport credential of one gRPC client: the rotating
// workload identity verified by serverName (a DNS SAN of the target), or
// plaintext when the connection and the top-level guard both say so.
func (ids *identities) client(name string, ci config.ClientIdentity, defaultServerName string) (grpc.DialOption, error) {
	switch ci.Mode {
	case "development":
		if !ids.cfg.Development.Enabled {
			return nil, fmt.Errorf("%s.identity: development mode without development.enabled", name)
		}
		ids.log.Warn("DEVELOPMENT_ONLY plaintext gRPC client; qualifies no production identity", "connection", name)
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	case "mtls":
		files := ids.cfg.Resolved(ci)
		r, err := ids.reloader(files)
		if err != nil {
			return nil, fmt.Errorf("%s.identity: %w", name, err)
		}
		serverName := files.ServerName
		if serverName == "" {
			serverName = defaultServerName
		}
		creds, err := identity.NewClientCredentials(r, serverName)
		if err != nil {
			return nil, fmt.Errorf("%s.identity: %w", name, err)
		}
		return grpc.WithTransportCredentials(creds), nil
	}
	return nil, fmt.Errorf("%s.identity: unknown mode %q", name, ci.Mode)
}

// collector is the transport credential of an outbound gRPC connection to
// a non-AnvilKit server (the Temporal frontend, the OTLP exporter) under a
// ClientTLS section (tls verifies against the bundle read once; mtls
// rotates with the workload's files).
func (ids *identities) collectorCredentials(name string, t config.ClientTLS) (credentials.TransportCredentials, error) {
	switch t.Mode {
	case "development":
		if !ids.cfg.Development.Enabled {
			return nil, fmt.Errorf("%s: development mode without development.enabled", name)
		}
		ids.log.Warn("DEVELOPMENT_ONLY plaintext transport", "connection", name)
		return insecure.NewCredentials(), nil
	case "tls":
		pool, err := caPool(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		return credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: t.ServerName, MinVersion: tls.VersionTLS13}), nil
	case "mtls":
		t = ids.cfg.ResolvedTLS(t)
		r, err := ids.reloader(config.MTLSFiles{CertFile: t.CertFile, KeyFile: t.KeyFile, CAFile: t.CAFile})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		creds, err := identity.NewClientCredentials(r, t.ServerName)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		return creds, nil
	}
	return nil, fmt.Errorf("%s: unknown mode %q", name, t.Mode)
}

// collector is collectorCredentials as a dial option (Temporal).
func (ids *identities) collector(name string, t config.ClientTLS) (grpc.DialOption, error) {
	c, err := ids.collectorCredentials(name, t)
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(c), nil
}

func caPool(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("ca bundle holds no certificate")
	}
	return pool, nil
}

// proxyTLS is the Model Proxy client's identity material under mtls: the
// resolved files and the shared Reloader of the process identity.
func (ids *identities) proxyTLS(ci config.ClientIdentity) (*identity.Reloader, config.MTLSFiles, error) {
	files := ids.cfg.Resolved(ci)
	r, err := ids.reloader(files)
	return r, files, err
}
