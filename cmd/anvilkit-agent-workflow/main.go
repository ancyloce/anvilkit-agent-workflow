package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/localcheck"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/logging"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Set with -ldflags '-X main.version=<retained build identity>'.
var version = "development"

func main() {
	hostname, _ := os.Hostname()
	logger := logging.New(os.Stdout, version, hostname+":"+strconv.Itoa(os.Getpid()), "local")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger); err != nil {
		// Config, TLS and SDK errors can embed remote strings or paths. Report
		// failure with a stable code; never print their raw error chain.
		logger.Error("health.transition", "outcome", "error")
		os.Exit(1)
	}
}

func connectionOptions(getenv func(string) string) (client.Options, error) {
	settings := make(map[string]string)
	for _, name := range []string{"ENDPOINT", "NAMESPACE", "TLS_SERVER_NAME", "TLS_CA", "TLS_CERT", "TLS_KEY"} {
		value := getenv("ANVILKIT_WORKFLOW_TEMPORAL_" + name)
		if value == "" {
			return client.Options{}, fmt.Errorf("missing Temporal setting %s", name)
		}
		settings[name] = value
	}
	host, port, err := net.SplitHostPort(settings["ENDPOINT"])
	if err != nil || host == "" || port == "" {
		return client.Options{}, errors.New("Temporal endpoint must be host:port")
	}
	ca, err := os.ReadFile(settings["TLS_CA"])
	if err != nil {
		return client.Options{}, errors.New("Temporal CA cannot be read")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return client.Options{}, errors.New("Temporal CA is invalid")
	}
	certificate, err := tls.LoadX509KeyPair(settings["TLS_CERT"], settings["TLS_KEY"])
	if err != nil {
		return client.Options{}, errors.New("Temporal client certificate is invalid")
	}
	return client.Options{
		HostPort: settings["ENDPOINT"], Namespace: settings["NAMESPACE"],
		ConnectionOptions: client.ConnectionOptions{TLS: &tls.Config{
			MinVersion: tls.VersionTLS13, ServerName: settings["TLS_SERVER_NAME"],
			RootCAs: roots, Certificates: []tls.Certificate{certificate},
		}},
	}, nil
}

func run(ctx context.Context, logger *logging.Logger) error {
	if err := contracts.Ready(); err != nil {
		return err
	}
	options, err := connectionOptions(os.Getenv)
	if err != nil {
		return err
	}
	options.Logger = logger
	dialContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	c, err := client.DialContext(dialContext, options)
	cancel()
	if err != nil {
		return err
	}
	defer c.Close()
	w := worker.New(c, contracts.LocalCheckTaskQueue, worker.Options{WorkerStopTimeout: 10 * time.Second})
	w.RegisterWorkflowWithOptions(localcheck.LocalCheckWorkflow, workflow.RegisterOptions{Name: contracts.LocalCheckWorkflowType})
	w.RegisterActivityWithOptions(localcheck.ComputeLocalCheck, activity.RegisterOptions{Name: contracts.LocalCheckActivityType})
	if err := w.Start(); err != nil {
		return err
	}
	logger.Info("service.started")
	<-ctx.Done()
	logger.Info("service.stopping")
	w.Stop()
	logger.Info("service.drained")
	return nil
}
