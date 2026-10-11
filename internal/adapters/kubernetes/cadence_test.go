package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// TestTwoHundredObservedJobsKeepTheirCadence is P1.6 AC8: one replica
// observes 200 Jobs at once through a real client-go REST client against a
// fake API server that throttles nothing but fails every 20th request with
// 503. With the configured rate limit every observation keeps its 1 s
// cadence within 20 % and none fails (no spurious OBSERVER_FAILED): a
// failed read is repeated on the next tick.
func TestTwoHundredObservedJobsKeepTheirCadence(t *testing.T) {
	if testing.Short() {
		t.Skip("observes 200 Jobs for several seconds")
	}
	const jobs = 200
	const runFor = 5 * time.Second
	start := time.Now()
	var requests, failed atomic.Int64
	var mu sync.Mutex
	polls := map[string][]time.Time{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if name, ok := strings.CutPrefix(r.URL.Path, "/apis/batch/v1/namespaces/ns/jobs/"); ok {
			// Every poll attempt counts toward the cadence, also one the
			// server then fails.
			mu.Lock()
			polls[name] = append(polls[name], time.Now())
			mu.Unlock()
		}
		if n%20 == 0 {
			failed.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"ServiceUnavailable","code":503}`))
			return
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/apis/batch/v1/namespaces/ns/jobs/"):
			name := strings.TrimPrefix(r.URL.Path, "/apis/batch/v1/namespaces/ns/jobs/")
			job := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"name": name, "namespace": "ns", "uid": "uid-" + name}, "status": map[string]any{}}
			if time.Since(start) > runFor {
				job["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Complete", "status": "True", "reason": "Completed"}}}
			}
			_ = json.NewEncoder(w).Encode(job)
		case r.URL.Path == "/api/v1/namespaces/ns/pods":
			name := strings.TrimPrefix(r.URL.Query().Get("labelSelector"), labelLaunchKey+"=")
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{}, "items": []any{map[string]any{
				"metadata": map[string]any{"name": "pod-" + name, "uid": "pod-" + name, "ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": name, "uid": "uid-" + name, "controller": true}}},
				"status":   map[string]any{"phase": "Running"},
			}}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	l, err := NewForConfig(&rest.Config{Host: srv.URL, QPS: 500, Burst: 1000}, "ns", Options{ObserveInterval: time.Second})
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, jobs)
	for i := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = l.ObserveJob(context.Background(), activities.ObserveJobInput{Launch: activities.LaunchRef{LaunchKey: fmt.Sprintf("job-%03d", i)}, Deadline: time.Now().Add(time.Minute)}, func() {})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "observation %d", i)
	}
	var gaps []time.Duration
	mu.Lock()
	for _, ts := range polls {
		for j := 1; j < len(ts); j++ {
			gaps = append(gaps, ts[j].Sub(ts[j-1]))
		}
	}
	mu.Unlock()
	slices.Sort(gaps)
	var sum time.Duration
	for _, g := range gaps {
		sum += g
	}
	mean := sum / time.Duration(len(gaps))
	p95 := gaps[len(gaps)*95/100]
	t.Logf("P1.6 AC8: %d Jobs, %d requests (%d failed with 503 and were repeated), %d poll intervals: mean %s, p95 %s, max %s; 0 observation failures",
		jobs, requests.Load(), failed.Load(), len(gaps), mean.Round(time.Millisecond), p95.Round(time.Millisecond), gaps[len(gaps)-1].Round(time.Millisecond))
	require.InDelta(t, float64(time.Second), float64(mean), float64(200*time.Millisecond), "mean cadence within 20 %%")
	require.LessOrEqual(t, p95, 1200*time.Millisecond, "p95 cadence within 20 %%")
}
