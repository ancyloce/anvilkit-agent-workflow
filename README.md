# anvilkit-agent-workflow

The Workflow worker of the AnvilKit Agent platform: the Temporal Worker Deployment that runs the fixed business Workflows (the LocalCheck fixture and the recovery reconciliation in this stage), their bounded Activities against Control, and the fixed-template Kubernetes Job launcher with its trusted observation. It has no public business RPC, no database and no credential other than its launcher identity; every business decision is Control's and only Temporal advances a run. The architecture that owns this service is the parent repository `anvilkit-services` (`docs/architecture/`: the service catalog, `execution.md` DD-01 for the Workflows, leases and Continue-As-New rules, DD-03 for the launcher, isolation and the trusted Job boundary, `delivery.md` P05–P09 for what is implemented and verified); the parent mounts this repository as the submodule `services/agent/workflow`.

This repository holds the replacement implementation (Temporal Go SDK, Fx, koanf configuration, client-go). The previous implementation that lived here (Connect RPC, the retained contracts) is outside the build closure of the replacement and is not a starting point; its last worktree is preserved by the parent's cleanup record, not here.

## Layout

| Path | Content |
| --- | --- |
| `cmd/anvilkit-agent-workflow` | `main`: `fx.New(bootstrap.Module()).Run()` |
| `internal/bootstrap` | Fx assembly: configuration snapshot, logger, Temporal client, Control client, launcher, the two pollers (`anvilkit-workflow`, `anvilkit-workflow-control`) with stable Workflow/Activity names, the health listener, DEVELOPMENT_ONLY fault interceptors |
| `internal/config`, `config.yaml` | One typed, validated configuration snapshot (defaults < the reviewed file < the allowlisted `ANVILKIT_WORKFLOW_*` overrides; unknown keys, ranges and cross-field rules reject the candidate); the `execution` bounds are frozen into each run's history |
| `internal/workflows` | `LocalCheckWorkflow` (attempt, launch, one create request per ordinal with its ledger, observation, registration, verification, acceptance, protected cleanup and reconciliation of an unconfirmed stop) and `RecoveryWorkflow`; Temporal time and Activities only |
| `internal/activities` | The Activity implementations and their stable names: Control calls (`OpenAttempt`, `PrepareLaunch`, `RegisterInstance`, `ObserveInstance`, `AcceptResult`, `CloseAttempt`, recovery), the launcher calls (`CreateJob`, `ObserveJob`, `ObserveLaunch`, `DeleteJob`) and the trusted result verification |
| `internal/adapters/control` | gRPC client of `anvilkit.control.v1` (Execution, Recovery) |
| `internal/adapters/kubernetes` | The fixed-template launcher (reviewed profiles only, digest-pinned images, no ServiceAccount token on any Job Pod, one HTTP request per create ordinal, UID-bound deletes, the Job's own Pod as physical owner), its policy-fixture renderer and the integration test against a real cluster |
| `deploy/chart` | The service's Helm chart (Deployment, ServiceAccount, the launcher's Role and RoleBinding, ConfigMap, PodDisruptionBudget) |
| `Dockerfile`, `.dockerignore` | Image build from this repository root alone |
| `.github/workflows/ci.yml` | Build, vet, test, race check of the Workflows and Activities, image build, chart lint and render |

Dependency direction: `cmd -> bootstrap(Fx) -> workflows/activities -> adapters`; Workflow code imports Temporal only. The generated contract (`github.com/ancyloce/anvilkit-agent-contracts/go`: `anvilkit/control/v1`, `jobschema`) is an ordinary versioned module requirement, resolved through GOPROXY. There is no replace directive, no workspace file and nothing read from a sibling checkout.

## Configuration

The worker reads one reviewed, secret-free file (`config.yaml`, path from `ANVILKIT_WORKFLOW_CONFIG`, default `./config.yaml`) with the sections `temporal` (address, namespace, the two Task Queues, worker identity, build id), `control.address`, `kubernetes` (namespace, enabled profiles, sidecar placement, the candidate seccomp profile), `execution` (the frozen bounds), `development` (file-only fault injection), `health.listen` and `shutdown_timeout`. The only environment overrides are `ANVILKIT_WORKFLOW_TEMPORAL_ADDRESS`, `ANVILKIT_WORKFLOW_CONTROL_ADDRESS`, `ANVILKIT_WORKFLOW_KUBECONFIG`, `ANVILKIT_WORKFLOW_LAUNCH_BACKEND`, `ANVILKIT_WORKFLOW_BUILD_ID`, `ANVILKIT_WORKFLOW_IMAGE_REGISTRY`, `ANVILKIT_WORKFLOW_SIDECAR_CONTROL_ADDRESS` and `ANVILKIT_WORKFLOW_HEALTH_LISTEN`; any other `ANVILKIT_WORKFLOW_*` variable stops the process.

The launcher identity is client-go's REST configuration from one of two sources: a kubeconfig file (`ANVILKIT_WORKFLOW_KUBECONFIG`, a worker outside the cluster such as the parent's development runs) or, when no kubeconfig is named, the Pod's mounted ServiceAccount token and CA (in-cluster). No token or kubeconfig is baked into the image. The health listener is the worker's only HTTP surface: `/healthz` answers while the process runs, `/readyz` between the start of both pollers and the beginning of the shutdown; a stopping worker reports not ready, drains its pollers for up to `shutdown_timeout`, then closes its clients.

## Build and verify from this repository alone

```sh
export GOWORK=off GOFLAGS=-mod=readonly
go build ./... && go vet ./... && go vet -tags integration ./... && go test -count=1 ./...
go test -race -count=1 ./internal/workflows/... ./internal/activities/...
docker build -t anvilkit-agent-workflow:dev .                   # --build-arg GOPROXY=... GONOSUMDB=... only for a private module proxy
helm lint deploy/chart --set temporal.address=temporal:7233 --set control.address=control:9101 \
  --set launcher.backend=cluster --set launcher.imageRegistry=registry.example --set launcher.sidecarControlAddress=control:9101
```

The unit suites use the Temporal test environment and fake clientsets. The in-cluster scenario (`internal/adapters/kubernetes/harness_integration_test.go`, build tag `integration`) needs a real development cluster with the parent's admission, syscall and network policies and a running Control that a Job Pod's sidecar can reach; it reads them from the environment (`KUBECONFIG`, `ANVILKIT_DEV_KIND_GATEWAY`, `ANVILKIT_DEV_REGISTRY`, `ANVILKIT_INTEGRATION_CONTROL_ADDRESS`, `ANVILKIT_INTEGRATION_SIDECAR_CONTROL_ADDRESS`) and skips when they are absent. The parent repository's verification chain prepares that Control from the Control repository and names it; this repository builds nothing but itself. `TestRenderPolicyFixtures` writes the launcher's rendered Jobs into the directory named by `ANVILKIT_RENDER_POLICY_FIXTURES` (the parent's `deploy/policies/tests/resources`, which owns the policies) and is otherwise a no-op.

The contract module is an ordinary published dependency. This build requires `github.com/ancyloce/anvilkit-agent-contracts/go v0.1.2-0.20260916181159-a397fee37c16`: the pseudo-version of commit `a397fee` on `main` of the `anvilkit-agent-contracts` repository (pushed 2026-09-16; it carries `ExecutionService.GetInstance` with the attempt's `operation` in its answer and `profile.expectedResult.resultSizeBytes`), served by `proxy.golang.org` and verified against the checksum database (`go.sum`: `h1:Z0/y7RKUi3G95MAHVAv4dMyf5hivJ47v1R4tzsFpNug=`). No tag names that commit yet; once the contracts repository tags it (`go/v0.1.2`), the `require` line changes to the tag and nothing else does. No replace directive, workspace or local proxy is involved.

## Deploy

`deploy/chart` carries only what this worker needs. Required values: `temporal.address`, `control.address`, `launcher.backend` (the launch backend identity Control records), `launcher.imageRegistry` and `launcher.sidecarControlAddress` (Control as a Job Pod's access sidecar reaches it). The chart is the single owner of the launcher's RBAC: with `rbac.create` it creates the Role and RoleBinding in `launcher.namespace` (`anvilkit-components`) that grant its ServiceAccount exactly Jobs create/get/list/watch/delete and Pods get/list/watch; the ServiceAccount token is mounted into the worker Pod and into nothing else (every Job Pod the launcher renders has `automountServiceAccountToken: false`). The Worker Build ID is the pinned image digest unless `buildId` names one. `image.digest` pins the exact image; `config` renders the reviewed file into a ConfigMap; `resources`, HTTP probes on the health listener, the security context and a PodDisruptionBudget are declared. Environment values (addresses, the backend, the registry, replica counts, digests) and the pinned deployment combination belong to the deploying repository (`anvilkit-services`, `deploy/dev` for the development foundation), never to this chart. Plaintext gRPC and the development sidecar identity are development inputs; runtime qualification (the parent's G gates) is not claimed by any check here.

## License

MIT, see `LICENSE`.
