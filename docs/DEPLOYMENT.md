# Maintainer deployment and live acceptance

Deployment is manual. A Git push or merge never changes the workspace. The authoritative,
customer-portable install/update contract is [Customer installation and lifecycle](CUSTOMER_DEPLOYMENT.md);
this page records the additional maintainer rules for the existing demonstration target.

## Existing target

| Input | Value |
| --- | --- |
| CLI profile | `fe_vm_rtm` |
| Bundle target | `dev` |
| App | `asset-reliability-center` |
| Catalog/schema | `classic_stable_realtime_demos_catalog.asset_reliability_command_center` |
| App admission group | `users` |
| Lakebase project | `asset-reliability-command-center` |

Treat these as explicit command inputs for this target, not defaults in deployable source. Never
select a CLI profile implicitly. Do not read, print or persist a token; the named CLI profile owns
authentication.

The active demo is the Spark Rate personal-sandbox path. Kafka, Lakeflow and Genie are optional
production-reference assets and are not included by the primary bundle.

## Checked entry point

Use `scripts/deploy.py` with one lifecycle operation:

- `preflight`: local validation and read-only workspace discovery;
- `install`: first complete installation;
- `update`: complete update of an existing installation; or
- `update --app-only`: direct App snapshot update with no bundle/data/governance mutation.

The complete option list and copyable command are maintained in
[the customer guide](CUSTOMER_DEPLOYMENT.md#command-template). For this workspace, fill that
template with the table above and the reviewed Lakebase, warehouse, Foundation Model, Model
Service, Skill, MCP, policy and MLflow values. Pass `--profile fe_vm_rtm` explicitly.

Always run the identical inputs through `preflight` before `install` or `update`. The script
requires a clean committed worktree, stages `.artifacts/app/app.yaml` from the environment-neutral
template, executes the full local gate, and runs strict bundle validation. Remote mutation occurs
only for `install` or `update`.

## Scope decision

Use `update --app-only` only when every behavioural change is inside:

- `frontend/`;
- App-owned `backend/` code;
- `app.yaml.template`; or
- App dependency manifests.

Use a complete `update` for changes to the bundle, Lakebase schema, Spark/RTM, Unity Catalog,
Skill, MCP, Unity Gateway policy, Model Service, MLflow experiment/evaluation or optional
production-reference assets. Documentation/test-only changes need no deployment.

App-only mode is fail-closed to exactly two mutations: sync the generated runtime-only source to
the App's existing narrow workspace path, then create a direct App snapshot. It cannot invoke a
bundle deployment, migration, RTM restart or governance helper.

## Complete deployment order

The checked installer/update performs:

1. profile/host/group verification and the local quality gate;
2. foundational bundle/App creation on first install and resolution of that App's generated
   service principal;
3. least-privilege grants and idempotent Lakebase migration;
4. primary bundle reconciliation and Rate RTM restart/readiness;
5. MLflow trace-table grants;
6. policy-function update and Skill/MCP/Gateway reconciliation;
7. final generated App snapshot; and
8. read-only resource checks.

The App service principal is discovered from the target App. Never copy the previous demo client
ID into a new workspace. OBO tokens remain process-local and are never deployment inputs.

## Live acceptance

Automated success proves resource reconciliation, not the demo journey. Before recording a
deployment as accepted:

1. Confirm the App is `RUNNING`; `/api/build-info` returns the selected commit and `/api/health`
   reports healthy Lakebase, Rate RTM, telemetry and evaluator state.
2. Confirm the RTM job uses Spark Rate, Real-Time Mode, the native PostgreSQL sink and the
   configured Unity Catalog checkpoint volume.
3. Sign in as two users and confirm independent sandbox, slot, scenario, history and controls.
4. Reset user A. P-202 and P-207 must each produce one normal transient, resolve with no action and
   never recur in that scenario. User B must remain unchanged.
5. Trigger P-204 for user A. Confirm the LangGraph loads the governed Skill, discovers MCP tools at
   runtime, invokes the Foundation Model API through the Unity Gateway Model Service and pauses on
   native `ASK` before submission.
6. Approve once. Confirm exact replay, one work order, one immutable receipt and a terminal model
   response. User B must not be able to access or approve user A's incident.
7. Confirm the managed MLflow experiment contains the real trace with pump, incident and outcome
   tags; exercise the App's experiment/trace links.
8. Run the real-trace evaluation job only after genuine P-202, P-207 and P-204 traces exist. It
   must fail rather than substitute synthetic outputs when any case is absent.

For UI changes, inspect the deployed App's console and network behaviour. Browser/computer-use
automation is not part of this runbook; use the team's approved manual inspection method.

## Evidence and rollback

Record only sanitised source commit, deployment/resource identifiers and text outputs. Never
record tokens, cookies, connection strings or generated credentials. The older Kafka build under
`evidence/` remains dated evidence and must not be represented as proof of the current Rate,
LangGraph or MLflow deployment.

Rollback is a forward deployment of a known-good commit. Use an App-only rollback only when both
the failed change and replacement are wholly inside the App boundary. Never reverse Lakebase
migrations, delete checkpoints, run a pipeline full refresh or use destructive Git commands as a
deployment rollback.
