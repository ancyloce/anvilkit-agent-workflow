// Package knowledge is the Workflow's client of Knowledge's SourceService
// for the content digest of brief references (P2.2, DD-07): the frozen
// brief binds the exact content of each referenced revision, read under
// the scope of the user who asked for the preparation, so only a source
// that user can read now is ever frozen.
package knowledge

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	knowledgev1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/knowledge/v1"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Digests reads the content digest of a source revision.
type Digests struct {
	conn    *grpc.ClientConn
	client  knowledgev1.SourceServiceClient
	timeout time.Duration
}

// Dial connects without blocking; failures surface per call.
func Dial(address string, timeout time.Duration, transport grpc.DialOption) (*Digests, error) {
	conn, err := grpc.NewClient(address, grpc.WithStatsHandler(otelgrpc.NewClientHandler()), transport)
	if err != nil {
		return nil, fmt.Errorf("knowledge %s: %w", address, err)
	}
	return &Digests{conn: conn, client: knowledgev1.NewSourceServiceClient(conn), timeout: timeout}, nil
}

func (d *Digests) Close() error { return d.conn.Close() }

// ContentDigest answers the digest of exactly the referenced revision: a
// source the actor cannot read (or that does not exist) is NOT_FOUND, a
// deleted source or a current revision other than the reference is a
// REVISION_CONFLICT (a newer revision is not silently frozen), and a
// failure to ask is retryable.
func (d *Digests) ContentDigest(ctx context.Context, scope activities.ReferenceScope, ref activities.SourceReference) (activities.ContentDigest, error) {
	if scope.ActorID == "" {
		return activities.ContentDigest{}, activities.Refused("INVALID_ARGUMENT", errors.New("the reference scope names no actor (an operation accepted before P2.2)"))
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	resp, err := d.client.GetSource(ctx, &knowledgev1.GetSourceRequest{
		Scope:    &knowledgev1.Scope{TenantId: scope.TenantID, ProjectId: scope.ProjectID, ActorId: scope.ActorID},
		SourceId: ref.SourceID,
	})
	switch status.Code(err) {
	case codes.OK:
	case codes.NotFound, codes.PermissionDenied:
		return activities.ContentDigest{}, activities.Refused("NOT_FOUND", fmt.Errorf("source %s is not readable in the operation's scope", ref.SourceID))
	case codes.InvalidArgument:
		return activities.ContentDigest{}, activities.Refused("INVALID_ARGUMENT", err)
	default:
		return activities.ContentDigest{}, fmt.Errorf("knowledge GetSource: %w", err)
	}
	src := resp.GetSource()
	if src.GetDeleted() {
		return activities.ContentDigest{}, activities.Refused("REVISION_CONFLICT", fmt.Errorf("source %s was deleted", ref.SourceID))
	}
	if src.GetCurrentRevision() != ref.Revision {
		return activities.ContentDigest{}, activities.Refused("REVISION_CONFLICT", fmt.Errorf("source %s is at revision %s, the reference names %s", ref.SourceID, src.GetCurrentRevision(), ref.Revision))
	}
	if src.GetContentDigest() == "" {
		return activities.ContentDigest{}, activities.Refused("NOT_FOUND", fmt.Errorf("source %s revision %s has no verified content", ref.SourceID, ref.Revision))
	}
	return activities.ContentDigest{SourceID: ref.SourceID, Revision: ref.Revision, Digest: src.GetContentDigest()}, nil
}
