# Checked manual deployment

The repository intentionally has no CI/CD integration. A push or merge changes source control
only. An authorized operator deploys a clean, reviewed commit with `scripts/deploy.py`.

The entry point is deliberately conservative:

- every Databricks command receives an explicit profile and bundle target;
- the expected workspace URL and existing access group are verified before mutation;
- dependencies are installed from lock files;
- the full Python and frontend quality gate runs before deployment;
- compiled frontend freshness is checked rather than assumed;
- clean `HEAD` is written to the ignored `.build-info.json` included in the App snapshot;
- a deterministic runtime-only App directory is staged under `.artifacts/app`;
- resources are reconciled in dependency order;
- App-owned Lakebase migrations run as the existing App service principal before data-plane jobs;
- obsolete producer and pipeline-runner jobs are stopped before the Rate RTM job restarts;
- post-deployment platform checks run before human acceptance begins.

The script never handles an OAuth secret. Authentication remains in the selected Databricks CLI
profile, and generated MCP connection credentials remain inside the governance deployment helper.

## Choose the deployment scope

Use the default complete-demo scope whenever a change affects Lakebase schema, Rate RTM code,
Unity Catalog resources, the governed Skill, MCP, or Unity Gateway. It preserves the
dependency-ordered deployment described below. The personal-sandbox redesign is always a full
deployment; it is never eligible for `--app-only`.

Use `--app-only` when every runtime change is contained within the existing App boundary:

- React source and styles under `frontend/`;
- FastAPI routes, operational runtime, and App presentation logic under `backend/`;
- App process configuration in `app.yaml`; or
- App Python dependencies in `pyproject.toml`, `uv.lock`, and `requirements.txt`.

Do not use `--app-only` for `sql/lakebase_schema.sql`, `src/`, `genie/`, `skills/`, governance SQL,
or non-App resource changes in `databricks.yml`. Documentation, tests, and deployment-tooling-only
commits do not require any workspace deployment.

Both scopes run the same clean-commit, identity, locked-dependency, test, build, staging, and
bundle-validation preflight. App-only execution resolves and validates the existing App's narrow
bundle source directory, synchronizes only `.artifacts/app`, and creates a snapshot through the
direct App API. It does not run a bundle resource deployment, Lakebase migration, continuous-job
restart or Skill/MCP/Gateway reconciliation.

## Target inputs

The current development environment is:

| Input | Value |
| --- | --- |
| Workspace | `https://fevm-classic-stable-realtime-demos.cloud.databricks.com` |
| CLI profile | `fe_vm_rtm` |
| Bundle target | `dev` |
| App | `asset-reliability-center` |
| Catalog | `classic_stable_realtime_demos_catalog` |
| Schema | `asset_reliability_command_center` |
| App admission group | Built-in workspace `users` group |
| Governed-history reference group | `asset-reliability-command-center-operator` (`84285700224068`) |
| Lakebase project | `asset-reliability-command-center` |

These are command inputs, not identities embedded in application logic. Use the values for the
actual target environment; do not silently rely on a CLI default profile.

## Prerequisites

- Databricks CLI 1.17 or later with OAuth authentication for the intended workspace.
- `uv`, Node.js, and npm.
- Permission to update the App, jobs, Lakebase project, UC schema, Skill, MCP source
  connection/service, and policy function.
- Membership in the existing group passed to `--access-group`.
- A clean, committed `feat/{feature_name}` branch for pre-merge acceptance, or the reviewed merge
  commit on `main` for final deployment.
- Capacity for one fixed-size classic RTM workload at the configured aggregate Rate.

Do not commit `.build-info.json`, generated credentials, tokens, `.env`, frontend build output, or
Databricks state.

## Preflight only

Run the entry point without `--execute` first:

```bash
uv run python scripts/deploy.py \
  --profile fe_vm_rtm \
  --target dev \
  --expected-host https://fevm-classic-stable-realtime-demos.cloud.databricks.com \
  --access-group users \
  --catalog classic_stable_realtime_demos_catalog \
  --schema asset_reliability_command_center \
  --app-name asset-reliability-center \
  --app-service-principal-client-id bd5178cb-8774-4d77-bc63-b4f491716d0b
```

Preflight performs the following checks and stops without changing workspace resources:

1. Require a clean Git worktree and resolve the exact commit.
2. Generate and reread `.build-info.json` containing only that commit.
3. Validate the Databricks CLI version.
4. Resolve the explicit profile, compare its host with `--expected-host`, verify the deploying
   user belongs to the supplied existing group, and confirm the explicit workload identity is the
   existing App service principal.
5. Run `uv sync --locked` and `npm ci`.
6. Run Ruff, all Pytest tests, TypeScript checking, and the Vite production build.
7. Confirm `requirements.txt` matches `uv.lock` and `frontend/dist` is newer than every frontend
   source input.
8. Rebuild `.artifacts/app` from an explicit runtime allowlist and verify that it contains no
   managed Genie definition, job/pipeline source, tests, documentation, caches, or local state.
9. Validate the bundle with the explicit target/profile/group and prove no tracked file changed.

Any failure stops the process before remote mutation.

## Execute the ordered deployment

After reviewing the preflight output, repeat the same command with `--execute`:

```bash
uv run python scripts/deploy.py \
  --profile fe_vm_rtm \
  --target dev \
  --expected-host https://fevm-classic-stable-realtime-demos.cloud.databricks.com \
  --access-group users \
  --catalog classic_stable_realtime_demos_catalog \
  --schema asset_reliability_command_center \
  --app-name asset-reliability-center \
  --app-service-principal-client-id bd5178cb-8774-4d77-bc63-b4f491716d0b \
  --execute
```

The script applies these steps in order:

1. A dedicated least-privilege compute policy and deployer-run UC grant job prepare the existing
   App service principal for the migration and fixed-size RTM workload. The RTM resource declares
   `SINGLE_USER` directly.
2. The helper deploys a one-shot Lakebase migration job under that App service principal, grants
   it read access to the synced bundle source, and runs it synchronously. The job applies
   `sql/lakebase_schema.sql` and verifies required runtime columns; failure stops deployment.
3. The helper stops any active synthetic Kafka producer and governed-history pipeline-runner jobs.
   Their definitions may remain as inactive production-reference/rollback resources.
4. The standalone Rate RTM job deploys under the same workload identity. The helper re-verifies
   source access, restarts only that continuous workload, and checks fresh Rate telemetry.
5. `databricks apps deploy` creates the App snapshot; App compute is restarted only if its required
   OBO scopes are not yet effective.
6. The policy-function job updates the UC function.
7. The governance helper reconciles the finalized Skill, App source permission, OAuth MCP source,
   MCP service tool selectors, and native `ASK` policy. Its probe must return `ASK` without source
   execution, after first proving exact tool discovery and a benign source read.
8. Read-only checks inspect the App, bundle summary, Rate RTM freshness and sandbox allocation.

## Execute an App-only deployment

For an eligible App-only commit, run the same preflight command with `--app-only`, review its
explicit `scope App only` output, and then add `--execute`:

```bash
uv run python scripts/deploy.py \
  --profile fe_vm_rtm \
  --target dev \
  --expected-host https://fevm-classic-stable-realtime-demos.cloud.databricks.com \
  --access-group users \
  --catalog classic_stable_realtime_demos_catalog \
  --schema asset_reliability_command_center \
  --app-name asset-reliability-center \
  --app-service-principal-client-id bd5178cb-8774-4d77-bc63-b4f491716d0b \
  --app-only
```

After preflight passes, repeat it with `--execute`. The helper runs `databricks sync` only against
the validated App source directory. The explicit include overrides the repository's ignored
`.artifacts/` parent while the sync client's own `.databricks/` state remains excluded. It then
calls `databricks apps deploy <app-name>` with that exact source path. Supplying the App name
selects the direct API mode and avoids the CLI's enhanced project deployment, which would
reconcile the full bundle. Existing OBO scopes are checked, and App compute is additionally
restarted only if a required scope is unexpectedly absent. Complete the
App-specific live checks below, including `/api/build-info`, `/api/health`, and the changed user
journey.

### App-only live validation

1. Run `databricks apps get asset-reliability-center --profile fe_vm_rtm -o json` and confirm the
   deployment succeeded and the App is `RUNNING`.
2. Confirm the authenticated `/api/build-info` response contains the selected commit and
   `/api/health` remains `ok`.
3. Exercise the changed App journey and inspect browser console and network activity for UI work.
4. If the change touches agent, Genie, or OBO behavior, execute that specific integration path;
   otherwise do not restart or redeploy unrelated workloads merely to validate the App snapshot.

### App snapshot boundary

The bundle root is not a valid App source directory. It contains the serialized Genie resource,
which the bundle deploys as a special workspace node. Databricks Apps snapshot import does not
support that node type. `scripts/deploy.py` therefore copies only `backend/`, `frontend/dist/`,
the Lakebase schema migration, runtime manifests/locks, and `.build-info.json` into the ignored
`.artifacts/app/` directory. The App resource snapshots that directory; the Genie Agent, jobs,
pipeline, governed Skill, tests, scripts, and documentation remain independently deployable
bundle/repository assets outside it.

Do not hand-edit or commit `.artifacts/app`. Every preflight removes and recreates it, preventing
stale or sensitive local files from surviving into a deployment.

The RTM readiness wait defaults to ten minutes. Override only when classic-compute startup
routinely requires longer:

```bash
--table-timeout-seconds 900
```

## Complete-demo live validation

Automated resource checks do not prove the business path. Complete this acceptance sequence in
the deployed App and record identifiers as text:

1. Confirm `/api/build-info` returns the deployed commit and `/api/health` is `ok`, not degraded.
2. Confirm the App reports fresh Rate RTM/telemetry and evaluator state.
3. Inspect the Rate RTM job and physical plan. It must be running on fixed-size classic compute,
   use `RealTimeStreamScan`, use the Rate source, and advance deterministic slot sequences.
4. Confirm the obsolete synthetic producer and governed-history pipeline runner are stopped.
5. Authenticate as two distinct Databricks users. Record their distinct sandbox, generator-slot
   and active-scenario identifiers; neither user may see the other's incidents or history.
6. Trigger P-204 for user A. User B must remain at baseline. Confirm P-202 and P-207 resolve
   independently with no action in both sandboxes.
7. Confirm user A's autonomous Sonnet workflow loads the Skill, discovers the configured
   MCP service's tools, assesses high risk, and stops at native `ASK` before submission.
8. Attempting `/mcp` as a human App user must return `403`. Gateway source access as the configured
   App service principal must remain successful.
9. Approve once through the native Gateway URL. Confirm exactly one replay, one work order, one
   immutable governance receipt, and one terminal model response.
10. Confirm user B cannot fetch, follow up, approve, or create a work order for user A's incident.
11. Trigger and approve independently for user B; confirm separate receipts and work orders.
12. Restart user A's sandbox. Verify only user A's tasks and scenario are archived and user B's
    active scenario and timeline are unchanged.

Follow [EVIDENCE.md](EVIDENCE.md) to save the one-time, sanitized, text-readable acceptance
package. Screenshots can supplement it but cannot replace raw output.

## Rollback

Rollback is forward-only for data schemas:

1. Create a temporary `feat/rollback_{reason}` branch at the last accepted commit.
2. Run the same preflight and deployment entry point with the same explicit target inputs.
3. Do not reverse or drop Lakebase/UC schema changes. Migrations must remain compatible with both
   the retained legacy writer and the personal-sandbox readers.
4. Re-run the complete live validation, including the Gateway policy probe and Rate RTM job.
5. Record the rollback commit and new deployment/run identifiers in the evidence or release log.

Never use `git reset --hard`, destructive table replacement, checkpoint deletion, or a pipeline
full refresh as a demo rollback mechanism.
