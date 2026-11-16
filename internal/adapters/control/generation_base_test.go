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

// DEC-062, DEC-064: a refinement's view carries the base its subject names
// as refinement_base: the source Control bound, read from Control's record
// of that artifact under the refinement's own operation, and the base's
// revision. A Generation's view asks for no artifact and carries none, and
// a subject that states only one of the two is refused.
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

	handle := "hdl_base"
	named := &controlv1.RefinementBase{SourceOperationId: "op_base", SourceRevision: revision}
	gen.subject = &controlv1.OperationSubject{ProfileId: "refinement-v1", SubjectDigest: "sha256:lineage", SourceHandle: &handle, RefinementBase: named}
	view, err = c.GetGeneration(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, &activities.StageArtifact{Handle: "hdl_base", Class: "source", Digest: "sha256:base", SizeBytes: "512", TransferID: "xfer_base", ObjectVersion: "v7"}, view.Base)
	require.Equal(t, "7", view.BaseRevision, "the revision the run starts from and the registration expects")
	require.Empty(t, view.SourceRevision, "the base revision is not the candidate's")
	require.Len(t, artifacts.got, 1)
	require.Equal(t, "op_1", artifacts.got[0].GetOperationId(), "read as an input of the refinement itself")
	require.Equal(t, "tenant_a", artifacts.got[0].GetTenantId())
	require.Empty(t, artifacts.got[0].GetInstanceId(), "the Workflow reads the record, not as a Job instance")

	// Fail closed: a refinement without its bound source would generate
	// from an empty directory, and a bound source that no base names (a
	// release's shape of the same fields) is no input of a Generation.
	for name, subject := range map[string]*controlv1.OperationSubject{
		"a base without a bound source": {ProfileId: "refinement-v1", SubjectDigest: "sha256:lineage", RefinementBase: named},
		"a bound source without a base": {ProfileId: "refinement-v1", SubjectDigest: "sha256:lineage", SourceHandle: &handle, SourceOperationId: &named.SourceOperationId, SourceRevision: &revision},
	} {
		gen.subject = subject
		_, err = c.GetGeneration(context.Background(), in)
		require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err), name)
	}
	require.Len(t, artifacts.got, 1, "nothing is read for a refused view")
}

// preparationStub answers GetPreparation with an empty intake and GetBrief
// with the scripted briefs.
type preparationStub struct {
	controlv1.UnimplementedPreparationServiceServer
	briefs map[string]*controlv1.Brief
}

func (s *preparationStub) GetPreparation(context.Context, *controlv1.GetPreparationRequest) (*controlv1.GetPreparationResponse, error) {
	return &controlv1.GetPreparationResponse{Preparation: &controlv1.Preparation{MaxRounds: "2", MaxQuestions: "3", WaitSeconds: "60"}}, nil
}

func (s *preparationStub) GetBrief(_ context.Context, in *controlv1.GetBriefRequest) (*controlv1.GetBriefResponse, error) {
	return &controlv1.GetBriefResponse{Brief: s.briefs[in.GetBriefId()]}, nil
}

// operationStub answers GetOperation with the scripted subjects.
type operationStub struct {
	controlv1.UnimplementedOperationServiceServer
	subjects map[string]*controlv1.OperationSubject
}

func (s *operationStub) GetOperation(_ context.Context, in *controlv1.GetOperationRequest) (*controlv1.GetOperationResponse, error) {
	return &controlv1.GetOperationResponse{Operation: &controlv1.OperationView{OperationId: in.GetOperationId(), Subject: s.subjects[in.GetOperationId()]}}, nil
}

// DEC-063, DEC-064: a Preparation's view carries the base its subject
// names as refinement_base: the base operation's lineage and brief and the
// identity that brief allocates. A Preparation of a new component carries
// none.
func TestGetPreparationResolvesItsBase(t *testing.T) {
	preparations, operations := &preparationStub{briefs: map[string]*controlv1.Brief{}}, &operationStub{subjects: map[string]*controlv1.OperationSubject{}}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	controlv1.RegisterPreparationServiceServer(server, preparations)
	controlv1.RegisterOperationServiceServer(server, operations)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///control", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := &Client{conn: conn, worker: "anvilkit-agent-workflow"}
	in := activities.GetPreparationInput{TenantID: "tenant_a", OperationID: "op_prep"}

	operations.subjects["op_prep"] = &controlv1.OperationSubject{ProfileId: "preparation-v1", SubjectDigest: "sha256:prompt"}
	view, err := c.GetPreparation(context.Background(), in)
	require.NoError(t, err)
	require.Nil(t, view.Base)
	require.Equal(t, uint64(2), view.MaxRounds)

	brief := "brf_base"
	operations.subjects["op_prep"] = &controlv1.OperationSubject{ProfileId: "preparation-v1", SubjectDigest: "sha256:prompt",
		RefinementBase: &controlv1.RefinementBase{SourceOperationId: "op_base", SourceRevision: "4"}}
	operations.subjects["op_base"] = &controlv1.OperationSubject{ProfileId: "generation-v1", SubjectDigest: "sha256:lineage", BriefId: &brief}
	preparations.briefs["brf_base"] = &controlv1.Brief{
		BriefId: "brf_base", Brief: &controlv1.ArtifactBinding{TransferId: "xfer_brief", Digest: "sha256:brief"}, Handle: "hdl_brief",
		Component: &controlv1.ComponentIdentity{ComponentId: "cmp_hero", PuckType: "Hero", PackageName: "@acme/hero"},
	}
	view, err = c.GetPreparation(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, &activities.PreparationBase{
		OperationID: "op_base", Revision: "4", Lineage: "sha256:lineage",
		Identity: activities.ComponentIdentity{ComponentID: "cmp_hero", PuckType: "Hero", PackageName: "@acme/hero"},
		Brief:    activities.ArtifactBinding{TransferID: "xfer_brief", Digest: "sha256:brief", Handle: "hdl_brief"},
	}, view.Base)

	// A base whose brief allocates no identity (frozen before P0.8) is refused.
	preparations.briefs["brf_base"].Component = nil
	_, err = c.GetPreparation(context.Background(), in)
	require.Equal(t, "BRIEF_INVALID", activities.RefusalCode(err))
}
