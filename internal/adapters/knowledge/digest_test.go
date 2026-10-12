package knowledge_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	knowledgev1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/knowledge/v1"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/knowledge"
)

type sources struct {
	knowledgev1.UnimplementedSourceServiceServer
	seen []*knowledgev1.Scope
}

func (s *sources) GetSource(_ context.Context, req *knowledgev1.GetSourceRequest) (*knowledgev1.GetSourceResponse, error) {
	s.seen = append(s.seen, req.GetScope())
	switch req.GetSourceId() {
	case "src_brand":
		return &knowledgev1.GetSourceResponse{Source: &knowledgev1.Source{SourceId: "src_brand", CurrentRevision: "3", ContentDigest: "sha256:brand"}}, nil
	case "src_deleted":
		return &knowledgev1.GetSourceResponse{Source: &knowledgev1.Source{SourceId: "src_deleted", CurrentRevision: "1", ContentDigest: "sha256:x", Deleted: true}}, nil
	case "src_down":
		return nil, status.Error(codes.Unavailable, "DEPENDENCY_UNAVAILABLE")
	}
	return nil, status.Error(codes.NotFound, "NOT_FOUND")
}

// TestContentDigestUnderTheUserScope (P2.2): the digest of exactly the
// referenced revision, read under the operation's user scope; unreadable,
// moved and deleted references are refused, an outage is retryable.
func TestContentDigestUnderTheUserScope(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	fake := &sources{}
	knowledgev1.RegisterSourceServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	d, err := knowledge.Dial(lis.Addr().String(), 5*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer d.Close()
	ctx := context.Background()
	scope := activities.ReferenceScope{TenantID: "tenant_a", ProjectID: "proj_a", ActorID: "user_a"}

	got, err := d.ContentDigest(ctx, scope, activities.SourceReference{SourceID: "src_brand", Revision: "3"})
	require.NoError(t, err)
	require.Equal(t, activities.ContentDigest{SourceID: "src_brand", Revision: "3", Digest: "sha256:brand"}, got)
	require.Equal(t, "user_a", fake.seen[0].GetActorId(), "readability is decided for the user, not the worker")
	require.Equal(t, "proj_a", fake.seen[0].GetProjectId())

	for name, c := range map[string]struct {
		ref  activities.SourceReference
		code string
	}{
		"another revision is never frozen": {activities.SourceReference{SourceID: "src_brand", Revision: "2"}, "REVISION_CONFLICT"},
		"a deleted source":                 {activities.SourceReference{SourceID: "src_deleted", Revision: "1"}, "REVISION_CONFLICT"},
		"unreadable or missing":            {activities.SourceReference{SourceID: "src_secret", Revision: "1"}, "NOT_FOUND"},
	} {
		_, err := d.ContentDigest(ctx, scope, c.ref)
		require.Equal(t, c.code, activities.RefusalCode(err), name)
	}
	_, err = d.ContentDigest(ctx, scope, activities.SourceReference{SourceID: "src_down", Revision: "1"})
	require.Error(t, err)
	require.Empty(t, activities.RefusalCode(err), "an outage is retryable")
	_, err = d.ContentDigest(ctx, activities.ReferenceScope{TenantID: "tenant_a"}, activities.SourceReference{SourceID: "src_brand", Revision: "3"})
	require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err), "no actor, no read")
}
