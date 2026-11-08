package bootstrap

import (
	"bufio"
	"errors"
	"go/build"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// layerRule is what one layer's packages may not import: an outer layer of
// this module, or a framework the layer must not depend on.
type layerRule struct {
	outer      []string
	frameworks []string
}

// innerFrameworks are the transport, composition and storage frameworks an
// inner layer never imports (AGENTS.md §5: domain code imports no Gin, Fx,
// gRPC or SQL driver; the application layers keep the same boundary).
var innerFrameworks = []string{"github.com/gin-gonic/gin", "go.uber.org/fx", "google.golang.org/grpc", "github.com/jackc/pgx", "github.com/lib/pq", "database/sql", "net/http"}

// directionRules is this module's dependency direction (RB-A29, AGENTS.md
// §5): cmd -> bootstrap (Fx) -> transport -> application -> domain; an
// inner layer never imports an outer one. Adapters implement the inner
// layers' ports and are outside the chain; no inner layer imports them.
var directionRules = map[string]layerRule{
	"internal/workflows":  {outer: []string{"internal/adapters", "internal/bootstrap", "internal/transport", "internal/config", "cmd"}, frameworks: append([]string{"os/exec"}, innerFrameworks...)},
	"internal/activities": {outer: []string{"internal/workflows", "internal/adapters", "internal/bootstrap", "internal/transport", "cmd"}, frameworks: innerFrameworks},
	"internal/transport":  {outer: []string{"internal/bootstrap", "cmd"}, frameworks: nil},
	"internal/bootstrap":  {outer: []string{"cmd"}, frameworks: nil},
}

// TestDependencyDirection reads every package's non-test imports from the
// source tree (go/build, the default build context) and refuses each import
// against its layer's rule. A rule whose layer matches no package fails as
// well, so a rename cannot leave the check passing on nothing.
func TestDependencyDirection(t *testing.T) {
	root, module := moduleRootAndPath(t)
	seen := map[string]int{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if path != root && (d.Name() == "testdata" || d.Name() == "node_modules" || strings.HasPrefix(d.Name(), ".")) {
			return filepath.SkipDir
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		layer := layerOf(filepath.ToSlash(rel))
		if layer == "" {
			return nil
		}
		pkg, err := build.ImportDir(path, 0)
		var noGo *build.NoGoError
		if errors.As(err, &noGo) {
			return nil
		}
		if err != nil {
			return err
		}
		seen[layer]++
		rule := directionRules[layer]
		for _, imp := range pkg.Imports {
			if dep, ok := strings.CutPrefix(imp, module+"/"); ok {
				for _, outer := range rule.outer {
					if dep == outer || strings.HasPrefix(dep, outer+"/") {
						t.Errorf("%s (%s) imports %s, an outer layer", filepath.ToSlash(rel), layer, dep)
					}
				}
				continue
			}
			for _, f := range rule.frameworks {
				if imp == f || strings.HasPrefix(imp, f+"/") {
					t.Errorf("%s (%s) imports %s", filepath.ToSlash(rel), layer, imp)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	layers := make([]string, 0, len(directionRules))
	for layer := range directionRules {
		layers = append(layers, layer)
	}
	sort.Strings(layers)
	for _, layer := range layers {
		if seen[layer] == 0 {
			t.Errorf("the rule for %s matches no package: update directionRules", layer)
		}
	}
}

// layerOf is the longest rule prefix of a module-relative directory.
func layerOf(rel string) string {
	best := ""
	for layer := range directionRules {
		if (rel == layer || strings.HasPrefix(rel, layer+"/")) && len(layer) > len(best) {
			best = layer
		}
	}
	return best
}

// moduleRootAndPath walks up from the test's directory to go.mod.
func moduleRootAndPath(t *testing.T) (string, string) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		f, err := os.Open(filepath.Join(dir, "go.mod"))
		if err == nil {
			defer f.Close()
			s := bufio.NewScanner(f)
			for s.Scan() {
				if m, ok := strings.CutPrefix(strings.TrimSpace(s.Text()), "module "); ok {
					return dir, strings.TrimSpace(m)
				}
			}
			t.Fatalf("%s/go.mod names no module", dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test")
		}
		dir = parent
	}
}
