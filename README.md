# anvilkit-agent-workflow

A standalone Go Temporal Worker for the approved `local-check-v1` profile.
`LocalCheckWorkflow` schedules one `ComputeLocalCheck` Activity, which computes
the byte length and SHA-256 of the exact UTF-8 text retained in its start input.
It preserves the operation, request, fixture, profile and generation fields in
the typed result. `plain-v1` hashes `AnvilKit` (8 bytes); `newline-v1` hashes
`AnvilKit\n` (9 bytes). It performs no trimming or Unicode normalization.

Control owns durable admission, the original Workflow/Run identity and terminal
result acceptance. This Worker has no Agent database connection, HTTP listener,
Control result RPC, Runner, authored graph, Job or paid/external business effect.
It registers only these two fixed types on
`anvilkit-agent-workflow-local-check`.

## Build and test

Use Go 1.27.0. The service owns its module and lock file, including Temporal Go
SDK 1.48.0. JSON Schema validation reuses `jsonschema/v6` 6.0.3; the SDK test
suite uses its existing Testify dependency. No parent checkout or workspace is
needed for these commands:

```sh
GOWORK=off go build -mod=readonly ./...
GOWORK=off go test -mod=readonly -race ./...
GOWORK=off go build -mod=readonly -o anvilkit-agent-workflow ./cmd/anvilkit-agent-workflow
```

The parent generates `internal/contracts/localcheck.gen.go` from the canonical
local-check schema/profile and retains their exact schemas, fixtures and source
digests beside it. Runtime startup verifies embedded bytes. Temporal JSON
conversion rejects duplicates, unknown fields, nulls, malformed Unicode,
unsupported profiles and overflowing counters. Generations and byte lengths
remain decimal strings; numeric schema version forms such as `1.0` are accepted
without allowing a rounded fraction or a string version.

Do not edit generated files. From the parent checkout, regenerate and check:

```sh
python3 tools/generate-proto-bindings.py --consumer anvilkit-agent-workflow
python3 tools/generate-proto-bindings.py --consumer anvilkit-agent-workflow --check
```

## Run against the controlled Temporal environment

The executable requires all six settings below. TLS verifies the server name
and retained CA, requires a client certificate, and uses TLS 1.3 or newer.
There is no plaintext fallback. Use the dedicated Workflow certificate; the
Worker does not need the Control starter certificate or database credentials.

| Environment variable | Value |
| --- | --- |
| `ANVILKIT_WORKFLOW_TEMPORAL_ENDPOINT` | Temporal `host:port` |
| `ANVILKIT_WORKFLOW_TEMPORAL_NAMESPACE` | Existing controlled namespace, with at least 24-hour retention |
| `ANVILKIT_WORKFLOW_TEMPORAL_TLS_SERVER_NAME` | Certificate server name, `temporal.local` in the retained handoff |
| `ANVILKIT_WORKFLOW_TEMPORAL_TLS_CA` | Path to the Temporal CA certificate |
| `ANVILKIT_WORKFLOW_TEMPORAL_TLS_CERT` | Path to the Workflow client certificate |
| `ANVILKIT_WORKFLOW_TEMPORAL_TLS_KEY` | Path to its private key |

After setting these variables, run `./anvilkit-agent-workflow`. Startup fails
closed if configuration, retained contracts or the Temporal connection is
unavailable. SIGINT/SIGTERM stops polling and gives the Worker a 10-second drain
window. The executable reports service startup, stopping and drain completion.
Build with `-ldflags '-X main.version=<retained-build-identity>'` to identify the
binary in logs; the acceptance driver supplies its source-tree digest.

## Fixed execution contract

The immutable input/result definitions and exact fixture values are in
`internal/contracts/retained/local-check-v1.schema.json`,
`local-check-v1.fixtures.json` and `local-check-v1.json`.

| Bound | Value |
| --- | --- |
| Workflow execution timeout | 5 minutes |
| Activity StartToClose / ScheduleToClose | 10 seconds / 60 seconds |
| Activity maximum attempts | 3 |
| Retry initial interval / coefficient / maximum interval | 1 second / 2 / 5 seconds |
| Workflow ID | `local-check:<operationId>` |
| Workflow retries, children, Continue-as-New, authored steps | Absent |

The starter must use `REJECT_DUPLICATE`, `FAIL` and
`WorkflowExecutionErrorWhenAlreadyStarted=true`. The Workflow checks its input,
identity, queue, timeout and absence of a Workflow retry policy before
scheduling the Activity. Control retains responsibility for the original
15-minute start window, history retention and original-identity reconciliation.
No replacement Workflow is started here. An observed cancellation returns a
canceled execution without a successful result. Activity attempts may repeat;
physical exactly-once execution is not claimed.

Workflow code uses only deterministic SDK operations and its retained input.
It returns the Activity's recorded result. Schema compilation happens once at
process startup; execution consults no file, database or mutable latest record.
Control verifies the fixture/input binding and current generations before
accepting a terminal result; computation alone grants no business authority.

## Real acceptance and replay

Unit tests cover both fixtures, strict conversion, large counters, exact UTF-8,
three-attempt exhaustion and observed cancellation. Real Temporal acceptance
requires the parent-owned driver and the persistent environment handoff:

```sh
ANVILKIT_WORKFLOW_TEST_ENVIRONMENT=/path/to/environment.json \
  python3 tools/run-verification.py --only workflow-local
```

Run that command from the parent checkout. It copies this clone outside the
parent, builds and tests it, then launches the canonical Worker binary with
only Temporal connection settings. The starter uses a separate client
certificate. Against Temporal Server 1.31.2, the proof checks both fixture
results, recorded input and execution bounds, pre-recorded cancellation,
closed-ID duplicate rejection and rejection of a Workflow retry policy. The
SDK unit environment omits that last metadata, so its rejection is tested on
the real server.

The driver retains source/binary digests, four real histories, Worker logs and
an evidence summary under its printed `/tmp/anvilkit-workflow01-proof-*` path.
Both successful histories replay without an Activity execution or execution
log. Replay must supply `ReplayWorkflowHistoryOptions.OriginalExecution` with
the retained Workflow and Run IDs; the SDK's default placeholder ID does not
match this flow's identity binding. The driver stops only its Worker process
and leaves the existing persistent infrastructure running.

Activity records use the approved local logging exception: `operationId`,
actual Temporal Workflow/Run IDs, `activityAttempt` and
`attributes.profileRef=local-check-v1`. They carry no fabricated StepExecution,
Attempt or instance ID. Workflow logging uses the SDK's replay-aware logger;
SDK free-form messages/errors and input/result contents are excluded. Every
real Worker log is checked against the retained log schema.

WORKFLOW-01 establishes this controlled local Worker. The subsequent WORKFLOW-02
acceptance, described below, completes CONTROL-04/05 and API-03's real flow. This proof does not qualify business Workflows, external
effects, production tracing, namespace authorization or restore targets.

### WORKFLOW-02 integrated recovery

The parent `workflow-recovery` verification step now covers API -> Control ->
Temporal/Worker -> Control -> API using the same retained persistent environment.
CONTROL-04/05 and API-03's real acceptance are included. Test-only authenticated
transport boundaries withhold a successful start reply, hold Activity completion
until Worker process loss, or return NotFound for an already bound history read.
Three lost Activity completions exercise the actual server retry ceiling.
No fault API or fault setting is included in the Worker executable.

The driver also covers intake persistence interruptions, reserved cancellation,
Control downtime after Temporal completion, a lost PostgreSQL COMMIT reply,
individual service restarts, and PostgreSQL/Temporal restart. Ten original real
histories replay with their exact Workflow/Run IDs and no execution logs or SQL
result/event writes. The history-read fault preserves upstream history for
replay; it does not qualify retention expiry or disaster recovery.

Run from the parent:

```sh
ANVILKIT_WORKFLOW_TEST_ENVIRONMENT=/path/to/environment.json \
  python3 tools/run-verification.py --only workflow-recovery
```

The 2026-09-10 controlled run passed 84 integrated assertions and the eleven
PostgreSQL transaction cases, with ten successful history replays and all 1,594
service records schema-valid. The parent task acceptance record retains exact
source/binary/contract hashes, commands, process exits and evidence paths.
Business Workflows, paid effects, production authorization and restore targets
remain outside this qualification.
