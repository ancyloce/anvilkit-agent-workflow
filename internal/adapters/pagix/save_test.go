package pagix

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// The conditional save of the DEVELOPMENT_ONLY source double: expected
// revision, conflict with the current one, idempotency by effect id, a
// lost answer that recorded the save, and the original-identity query.
func TestConditionalSave(t *testing.T) {
	ctx := context.Background()
	d, err := New(t.TempDir())
	require.NoError(t, err)
	save := func(effect, base string) activities.SaveSourceInput {
		return activities.SaveSourceInput{Lineage: "sha256:lin", BaseRevision: base, Occurrence: 1, Source: activities.StageArtifact{Digest: "sha256:" + effect}}
	}
	cur, err := d.CurrentRevision(ctx, "source:sha256:lin")
	require.NoError(t, err)
	require.Equal(t, "1", cur, "the registered candidate is revision 1")

	res, err := d.SaveRevision(ctx, "eff_a", save("a", "1"))
	require.NoError(t, err)
	require.Equal(t, activities.SaveSourceResult{EffectID: "eff_a", State: "saved", Revision: "2"}, res)
	again, err := d.SaveRevision(ctx, "eff_a", save("a", "1"))
	require.NoError(t, err)
	require.Equal(t, res, again, "the same effect answers its original outcome")

	conflict, err := d.SaveRevision(ctx, "eff_b", save("b", "1"))
	require.NoError(t, err)
	require.Equal(t, "conflict", conflict.State)
	require.Equal(t, "2", conflict.CurrentRevision)

	require.NoError(t, os.WriteFile(filepath.Join(d.dir, "faults", "save:1"), []byte("unknown"), 0o600))
	_, err = d.SaveRevision(ctx, "eff_c", save("c", "2"))
	require.Error(t, err, "the answer is lost")
	q, known, err := d.QuerySave(ctx, "eff_c")
	require.NoError(t, err)
	require.True(t, known, "the save happened upstream")
	require.Equal(t, "3", q.Revision)
	_, known, err = d.QuerySave(ctx, "eff_none")
	require.NoError(t, err)
	require.False(t, known)
	cur, err = d.CurrentRevision(ctx, "source:sha256:lin")
	require.NoError(t, err)
	require.Equal(t, "3", cur)
}
