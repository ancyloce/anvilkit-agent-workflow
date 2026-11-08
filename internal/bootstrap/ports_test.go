package bootstrap

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxtest"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/config"
)

// TestNoPagixPortsOutsideDevelopmentRefusesStart (RB-A02): a worker with
// neither the declared Pagix adapter nor the development double would take
// generations and releases that all fail at their first lease or source
// call. Outside development it refuses to start; in development it starts
// with the ports answering DEPENDENCY_UNAVAILABLE, as before.
func TestNoPagixPortsOutsideDevelopmentRefusesStart(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var cfg config.Config
	_, err := newLifecyclePorts(fxtest.NewLifecycle(t), cfg, nil, lifecycleDouble{}, log)
	require.True(t, errors.Is(err, errNoPagixPorts), "%v", err)
	require.ErrorContains(t, err, "lifecycle.pagix.declaration_file (ENV-07)")

	cfg.Development.Enabled = true
	ports, err := newLifecyclePorts(fxtest.NewLifecycle(t), cfg, nil, lifecycleDouble{}, log)
	require.NoError(t, err)
	_, isUnavailable := ports.lease.(unavailablePorts)
	require.True(t, isUnavailable, "development keeps the refusing ports")
}
