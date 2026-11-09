package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, g.Write(&m))
	return m.GetGauge().GetValue()
}

// RB-A25: a drained stop records its drain time and no forced stop; a stop
// that abandons in-flight Activities at the drain bound is forced. The one
// configuration generation of the process is active from the start.
func TestLifecycleMetricsRecordDrainAndAbandonment(t *testing.T) {
	reg := prometheus.NewRegistry()
	business, control := newFakeWorker(), newFakeWorker()
	w, _ := newTestLifecycle(t, time.Second, business, control)
	w.metrics = newLifecycleMetrics(reg)
	families, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]float64{}
	for _, f := range families {
		names[f.GetName()] = f.GetMetric()[0].GetGauge().GetValue()
	}
	require.Equal(t, 1.0, names["anvilkit_workflow_config_generation"])
	_, err = w.start()
	require.NoError(t, err)
	require.NoError(t, w.stop(context.Background()))
	require.Zero(t, gaugeValue(t, w.metrics.forced), "a drained stop is not forced")
	require.Less(t, gaugeValue(t, w.metrics.drain), 1.0)

	reg2 := prometheus.NewRegistry()
	business2, control2 := newFakeWorker(), newFakeWorker()
	w2, _ := newTestLifecycle(t, 200*time.Millisecond, business2, control2)
	w2.metrics = newLifecycleMetrics(reg2)
	business2.hold = w2.activities
	_, err = w2.start()
	require.NoError(t, err)
	require.NoError(t, w2.stop(context.Background()))
	<-business2.done
	require.Equal(t, 1.0, gaugeValue(t, w2.metrics.forced), "abandoned Activities make the stop forced")
	require.GreaterOrEqual(t, gaugeValue(t, w2.metrics.drain), 0.2)
}
