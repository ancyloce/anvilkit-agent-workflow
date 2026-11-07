package control

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// registerStub answers RegisterInstance and keeps the request; every other
// method of the embedded interface is unused here.
type registerStub struct {
	controlv1.ExecutionServiceClient
	got *controlv1.RegisterInstanceRequest
}

func (s *registerStub) RegisterInstance(_ context.Context, in *controlv1.RegisterInstanceRequest, _ ...grpc.CallOption) (*controlv1.RegisterInstanceResponse, error) {
	s.got = in
	return &controlv1.RegisterInstanceResponse{Instance: &controlv1.PhysicalInstance{InstanceId: "inst-1", Current: true}}, nil
}

// RB-S07: the registration sends Control the image digest the backend observed
// the Pod running, never the launched one; Control compares the two.
func TestRegisterInstanceSendsTheObservedDigest(t *testing.T) {
	stub := &registerStub{}
	c := &Client{exec: stub, backend: "rke2-anvilkit-jobs", worker: "anvilkit-agent-workflow"}
	observed := "sha256:" + strings.Repeat("f", 64)
	ref, err := c.RegisterInstance(context.Background(), activities.RegisterInstanceInput{
		Attempt:   activities.AttemptRef{AttemptID: "att_1", TenantID: "tenant_a"},
		Launch:    activities.LaunchRef{LaunchKey: "lc-1", ImageDigest: "sha256:" + strings.Repeat("a", 64)},
		CommandID: "att_1:register:pod-1", JobUID: "job-1", PodUID: "pod-1", ImageDigest: observed,
	})
	require.NoError(t, err)
	require.Equal(t, "inst-1", ref.InstanceID)
	require.Equal(t, observed, stub.got.GetImageDigest())
	require.Equal(t, "pod-1", stub.got.GetPodUid())
}
