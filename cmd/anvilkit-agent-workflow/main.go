// anvilkit-agent-workflow is the Temporal Worker Deployment: registered
// business Workflows, bounded Activities and the fixed Job launcher. It has
// no public business RPC and never writes Control tables.
package main

import (
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/bootstrap"
)

func main() {
	fx.New(bootstrap.Module()).Run()
}
