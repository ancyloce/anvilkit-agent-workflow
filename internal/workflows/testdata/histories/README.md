# Recorded Workflow histories (P1.7 replay evidence)

`TestReplay` (`../../replay_test.go`) replays every `*.json` here on the
current Workflow code with `worker.NewWorkflowReplayer`; a change that is not
guarded by `workflow.GetVersion` fails it before it reaches a worker.

Every file is a history from the development foundation's Temporal
(namespace `anvilkit`, DEVELOPMENT_ONLY data, test tenants only), scanned for
URLs, signing parameters and bearer tokens before it was written.

| Files | Source | Code that recorded it |
|---|---|---|
| `<Type>-before-<operation>.json` | runs of the integration harnesses on the foundation, exported by `TestExportHistories` (some abandoned by a stopped harness worker: they replay as far as they got) | Workflow code before the P1.6 version markers (2026-09-17 to 2026-10-11) |
| `<Type>-after-<operation>.json` | integration runs of 2026-10-11 after P1.6, exported the same way | current code |
| `<Type>-before-<scenario>.json` | `TestRecordHistories` scenarios run by the code of commit `d61b6f2` (the last commit before P1.6, extracted with `git archive`) | pre-P1.6 code |
| `<Type>-after-<scenario>.json` | the same scenarios run by the current code | current code |

The scenarios cross the P1.6 markers: `settle-poll-backoff` (settlement kept
reconciling, continue-as-new), `close-intent-first` (a canceled or failed
run's final close meets a reconciling operation), `hold-after-cleanup` (every
job step), `recovery-batched-rounds` (two recovery rounds; after P1.6 each is
its own run, `-run2`).

Excluded: `Release-before-zadph65k546jssh4gbjrehaq` (2026-09-30T20:16Z) was
recorded by an uncommitted iteration of the Release Workflow (the Workflow was
first committed at 23:13 that day); it fails replay on every committed
revision, and the run recorded ten minutes later replays.

Negative check (recorded in `outputs/p1-verification-20261011/p17-replay-negative.log`):
removing the `close-intent-first` guard fails `Preparation-before-canceled`;
removing the `recovery-batched-rounds` guard fails the three Recovery
`before` histories.

Regenerate (foundation up, `.local/dev/env.sh` sourced):

```sh
# exported runs
ANVILKIT_HISTORY_BEFORE=<rfc3339> ANVILKIT_HISTORY_AFTER=<rfc3339> \
  go test -tags historyexport -run TestExportHistories ./internal/workflows/
# scenarios, current code
ANVILKIT_HISTORY_LABEL=after go test -tags historyrecord -run TestRecordHistories ./internal/workflows/
# scenarios, an earlier revision: git archive <rev> | tar -x -C <dir>, copy
# history_record_test.go there, run with ANVILKIT_HISTORY_LABEL=before and
# copy the files back
```
