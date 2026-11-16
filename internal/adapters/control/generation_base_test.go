package control

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// generationStub answers GetGeneration with the scripted subject and the
// lease record Control always states.
type generationStub struct {
	controlv1.UnimplementedGenerationServiceServer
	subject *controlv1.OperationSubject
}

func (s *generationStub) GetGeneration(context.Context, *controlv1.GetGenerationRequest) (*controlv1.GetGenerationResponse, error) {
	return &controlv1.GetGenerationResponse{Generation: &controlv1.Generation{Subject: s.subject, Lease: &controlv1.LeaseRecord{}}}, nil
}

// artifactStub answers ReadArtifact with one transfer and keeps the requests.
type artifactStub struct {
	controlv1.UnimplementedArtifactServiceServer
	got []*controlv1.ReadArtifactRequest
}

func (s *artifactStub) ReadArtifact(_ context.Context, in *controlv1.ReadArtifactRequest) (*controlv1.ReadArtifactResponse, error) {
	s.got = append(s.got, in)
	version := "v7"
	return &controlv1.ReadArtifactResponse{Transfer: &controlv1.Transfer{
		TransferId: "xfer_base", Handle: in.GetHandle(), ExpectedDigest: "sha256:base", ExpectedSize: "512", ObjectVersion: &version,
	}}, nil
}

// DEC-062: a refinement's view carries the base source Control bound, read
// from Control's record of that artifact under the refinement's own
// operation; a Generation's view asks for no artifact and carries none.
func TestGetGenerationResolvesARefinementsBase(t *testing.T) {
	gen, artifacts := &generationStub{}, &artifactStub{}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	controlv1.RegisterGenerationServiceServer(server, gen)
	controlv1.RegisterArtifactServiceServer(server, artifacts)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///control", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := &Client{conn: conn, worker: "anvilkit-agent-workflow"}
	in := activities.GetGenerationInput{TenantID: "tenant_a", OperationID: "op_1"}

	revision := "7"
	gen.subject = &controlv1.OperationSubject{ProfileId: "generation-v1", SubjectDigest: "sha256:lineage"}
	view, err := c.GetGeneration(context.Background(), in)
	require.NoError(t, err)
	require.Nil(t, view.Base)
	require.Empty(t, artifacts.got, "a Generation has no base to read")

	handle, base := "hdl_base", "op_base"
	gen.subject = &controlv1.OperationSubject{ProfileId: "refinement-v1", SubjectDigest: "sha256:lineage", SourceRevision: &revision, SourceHandle: &handle, SourceOperationId: &base}
	view, err = c.GetGeneration(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, &activities.StageArtifact{Handle: "hdl_base", Class: "source", Digest: "sha256:base", SizeBytes: "512", TransferID: "xfer_base", ObjectVersion: "v7"}, view.Base)
	require.Equal(t, "7", view.SourceRevision, "the base revision the registration expects")
	require.Len(t, artifacts.got, 1)
	require.Equal(t, "op_1", artifacts.got[0].GetOperationId(), "read as an input of the refinement itself")
	require.Equal(t, "tenant_a", artifacts.got[0].GetTenantId())
	require.Empty(t, artifacts.got[0].GetInstanceId(), "the Workflow reads the record, not as a Job instance")
}
