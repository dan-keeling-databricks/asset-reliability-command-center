# Customer installation and lifecycle guide

This guide installs the Asset Reliability Command Centre into a Databricks workspace without
reusing any identity or resource identifier from the original demonstration environment. The
checked deployment entry point is `scripts/deploy.py`; **Git operations alone never change a workspace.**

The default installation is the personal-sandbox experience:

```text
Spark Rate source → Spark Real-Time Mode → Lakebase sandbox slots
    → authenticated user's pump estate → Databricks App

Alert → LangGraph agent → Unity Gateway Model Service → Foundation Model API
    → governed Skill and MCP tools → native ASK → approved work order

LangGraph execution → MLflow experiment and Unity Catalog trace tables
```

Kafka, Lakeflow Declarative Pipelines and Genie are retained as optional production-reference
assets. They are not required to install, run, health-check or demonstrate the personal sandbox.
Do not configure or start them unless you are deliberately extending the demo towards a real
plant-ingress design.

## Lifecycle boundaries

| Operation | Use it for | Workspace effect |
| --- | --- | --- |
| Preflight | Every install or update | Read-only discovery plus local validation; no remote mutation |
| Install | First deployment to a target | Creates/reconciles the complete active Rate-source demo |
| Update | A reviewed change outside the App-only boundary | Reconciles the complete active demo in dependency order |
| App-only update | React, FastAPI or App dependency/configuration changes only | Replaces only the App snapshot; leaves data and governance resources running |
| Rollback | Restore the last accepted application behaviour | Forward-deploys a known-good commit; never reverses schemas or deletes checkpoints |

There is intentionally no one-command destructive uninstall. See
[Decommissioning](#decommissioning) for the ownership boundary and safe removal order.

## Prerequisites

The deploying operator needs:

- Databricks CLI 1.17 or later, authenticated with OAuth under a named profile;
- `uv`, Node.js and npm;
- an existing Unity Catalog catalog and SQL warehouse;
- permission to create or update the project schema, managed checkpoint volume, jobs, compute
  policy, Databricks App, Lakebase project resources, MLflow experiment, Unity Gateway Model
  Service, governed Skill, MCP service and policy function;
- an existing workspace group whose members may open the App; and
- capacity for the fixed-size classic Spark Real-Time Mode workload.

The configuration contains names and non-secret resource identifiers only. Do not place OAuth
tokens, client secrets, passwords or generated MCP credentials in the example values file,
command history, Git or deployment evidence. Authentication remains in the selected CLI profile.

## Prepare customer values

Copy [the customer values example](examples/customer-values.example.yml) outside the repository
or into an ignored local file and replace every angle-bracket placeholder. The example is a
reviewable worksheet, not a file loaded by the deployment script. Pass its values through the
corresponding checked command-line options documented below.

Important choices:

- Use a dedicated project schema and Lakebase operational schema.
- Supply the Lakebase project, branch, database, database resource and endpoint names; the
  installer derives the full workspace resource paths.
- Select an App name of 26 characters or fewer using lowercase letters, numbers and hyphens.
- Use an existing App access group; the installer does not create people or groups.
- Size `sandbox_capacity` for concurrent authenticated users. The default design uses one shared
  Rate stream, not one Spark query per user.
- Keep the initial Rate settings at two samples per second per pump and four source partitions
  unless load testing justifies a change.
- Select a Foundation Model API model that is available in the target region.
- Keep every governed asset name inside the chosen catalog and schema.
- Use a workspace-absolute MLflow experiment path, such as
  `/Shared/<project>-agent`, and an existing Unity Catalog trace catalog/schema with a stable
  trace-table prefix. The trace location may be the project catalog/schema, but remains explicit.

The App service principal is created with, and resolved from, the target App. Do not copy a client
ID from another workspace. If an optional expected client ID is supplied during an update, it is
only an identity assertion and must match the App discovered in the target workspace.

## Review and authenticate

Work from a clean, reviewed commit on a `feat/{feature_name}` branch for pre-merge testing, or from
the accepted commit on `main` for installation:

```bash
git status --short
git rev-parse HEAD
databricks auth describe --profile <customer-profile> -o json
```

Confirm that the profile reports exactly the expected workspace host. Never rely on the default
profile, and pass `--profile <customer-profile>` on every Databricks command.

Before a first install, verify the existing customer-owned dependencies:

```bash
databricks current-user me --profile <customer-profile> -o json
databricks warehouses get <sql-warehouse-id> --profile <customer-profile> -o json
databricks groups list \
  --filter "displayName eq '<app-access-group>'" \
  --profile <customer-profile> \
  -o json
```

The checked preflight remains the final authority: it compares the profile host, resolves the App
identity, validates dependencies and stops before mutation.

## First installation

### 1. Run read-only preflight

Run the checked entry point with the `preflight` operation and all values from the customer
worksheet. The final command form is shown in
[Command template](#command-template). Do not proceed if the printed host, user, commit, App name
or scope is unexpected.

Preflight must:

1. require a clean Git commit and write its hash to the ignored build-info artifact;
2. verify the explicit profile and exact expected host;
3. validate all customer-supplied names, resource identifiers and capacity values;
4. install locked local dependencies and run Ruff, Pytest, frontend unit/type checks and build;
5. construct a runtime-only App snapshot and scan it for excluded files and original-environment
   hardcodes;
6. strictly validate the bundle with the selected profile, target and variable values; and
7. finish without creating, updating, starting or stopping any workspace resource.

### 2. Install the complete active demo

After reviewing successful preflight output, repeat the same inputs with the `install` operation.
Installation reconciles resources in dependency order:

1. foundational bundle resources, compute policy and managed checkpoint volume;
2. Lakebase project/branch/database resources and the idempotent operational schema migration;
3. least-privilege Unity Catalog grants for the resolved App service principal;
4. the shared Rate-to-Lakebase RTM job and telemetry freshness check;
5. Unity Gateway Model Service, MLflow experiment and trace-table access;
6. governed Skill, MCP connection/service tools and native-`ASK` policy;
7. the generated App snapshot and required OBO scopes; and
8. read-only post-install resource checks.

The synthetic Kafka producer and governed-history pipeline runner remain stopped. Genie is not an
active App dependency in the default installation.

### 3. Complete live acceptance

An installer reporting success proves resource reconciliation, not the business journey. Before
sharing the App, complete and record this acceptance:

1. Confirm the App is `RUNNING`, `/api/build-info` reports the selected commit and `/api/health`
   reports healthy Rate RTM, telemetry and evaluator freshness.
2. Inspect the RTM job: it uses the Spark Rate source, Real-Time Mode, fixed-size classic compute,
   the native PostgreSQL sink and the configured Unity Catalog checkpoint volume.
3. Sign in as two distinct users. Confirm different sandbox, generator-slot and scenario IDs and
   no cross-user incidents or history.
4. In user A's sandbox, run **Clear history & replay**. P-202 and P-207 must each cross the
   threshold once, resolve with no action and not recur. User B must remain unchanged.
5. Trigger P-204 for user A. Confirm the autonomous graph loads the governed Skill, discovers MCP
   tools at runtime, reasons through the configured Model Service and stops at native `ASK` before
   work-order execution.
6. Approve once through Unity Gateway. Confirm one exact replay, one work order, one immutable
   receipt and one terminal model response.
7. Confirm MLflow contains the real LangGraph trace and that the App links only to verified
   experiment/trace URLs.
8. Repeat independently for user B, then restart user A's sandbox and verify user B is unchanged.

Store only sanitised identifiers and outputs as evidence. Never record OBO tokens, cookies,
connection strings or credentials.

## Updating an installation

Updates are commit-based and forward-only:

1. Review and check out the intended commit on a clean branch.
2. Compare it with the currently deployed commit returned by `/api/build-info`.
3. Reuse the target's customer values; do not substitute values from another workspace.
4. Classify the change using the scope rules below.
5. Run `preflight` for the selected scope and review the printed target and plan.
6. Run `update` with the identical values and selected scope.
7. Complete the matching live acceptance checks and record the new commit/deployment identifiers.

Use a complete update for any change to:

- `databricks.yml` or a non-App bundle resource;
- Lakebase schema or migrations;
- Spark Rate/RTM, checkpointing or Unity Catalog data-plane code;
- the governed Skill, MCP connection/service, Gateway policy or Model Service;
- MLflow experiment, trace persistence or evaluation resources; or
- any optional production-reference asset.

A complete update may reconcile and restart the resources it owns. It must not activate Kafka,
Lakeflow or Genie merely because their inactive definitions remain in the repository.

## App-only updates

Use an App-only update only when every changed runtime file is inside the App snapshot boundary:

- `frontend/` source and styles;
- `backend/` routes and App-hosted runtime/presentation logic;
- the environment-neutral `app.yaml.template`; or
- App Python dependency manifests (`pyproject.toml`, `uv.lock`, `requirements.txt`).

Documentation, tests and deployment-tooling-only commits require no workspace deployment.

Do **not** use App-only mode for `databricks.yml`, `sql/lakebase_schema.sql`, `src/`, `skills/`,
governance SQL, Model Service, experiment/evaluation resources or optional Genie/Lakeflow assets.
When a commit crosses both boundaries, use a complete update.

App-only preflight still runs the complete local quality gate, creates a clean runtime-only
snapshot and validates the bundle. App-only `update` then synchronises and deploys that exact App
source directory through the direct Databricks Apps API. It must not run a bundle deployment,
migrate Lakebase, restart RTM or reconcile Gateway resources.

After an App-only update:

1. confirm the App is `RUNNING` and reports the new commit;
2. confirm `/api/health` is still healthy;
3. exercise the changed App journey; and
4. test agent/OBO integration only when the change touched that boundary.

## Rollback

Rollback means deploying a known-good commit as a new update. It is not a destructive Git or data
operation.

1. Identify the last accepted commit and its recorded customer values.
2. Create `feat/rollback_{reason}` at that commit; do not reset or rewrite `main`.
3. Run preflight against the same target with the same explicit values.
4. Select App-only scope only if both the failed change and the known-good replacement are wholly
   inside the App boundary. Otherwise use a complete update.
5. Run the update and repeat the corresponding live acceptance.
6. Record the known-good source commit and the new deployment/run identifiers.

Never drop or reverse Lakebase/Unity Catalog schema changes, delete streaming checkpoints, run a
pipeline full refresh or use `git reset --hard` as rollback. Migrations remain idempotent and
forward-compatible with the known-good runtime.

## Optional production-reference assets

The repository retains Kafka producer, Kafka-to-Lakebase, Lakeflow governed-history and Genie
definitions to explain how real plant ingress and analytical history fit the architecture. In the
customer demo baseline:

- Spark Rate is the only active telemetry source;
- the shared Rate RTM job is the only continuous synthetic data-plane workload;
- Lakebase is the active operational store;
- the App does not require Genie; and
- Kafka and Lakeflow jobs remain paused or absent from the active target.

Their bundle fragment is `resources/production_reference.yml`; the primary `databricks.yml` does
not include it. The adjacent Genie definition is a customer-rendered template rather than a
committed workspace-specific JSON resource.

Treat production-reference enablement as a separate, reviewed design change. It needs customer
Kafka connectivity/secrets, data contracts, retention, access grants, operating ownership and
capacity planning. Do not paste production credentials into the example values file.

## Command template

The deployment tool consumes explicit command-line values rather than loading the example YAML.
Use one command shape for preflight, install and update; change only the lifecycle operation and,
for an eligible App-only update, add `--app-only`.

Run the safe default as an explicit `preflight` first:

```bash
uv run python scripts/deploy.py preflight \
  --profile "<customer-cli-profile>" \
  --target "<customer-target>" \
  --expected-host "https://<customer-workspace-host>" \
  --access-group "<existing-workspace-group>" \
  --catalog "<existing-catalog>" \
  --schema "<project-schema>" \
  --volume "<managed-checkpoint-volume>" \
  --warehouse-id "<existing-sql-warehouse-id>" \
  --app-name "<asset-reliability-app-name>" \
  --lakebase-project-id "<lakebase-project-id>" \
  --lakebase-branch "<lakebase-branch>" \
  --lakebase-database "<lakebase-database>" \
  --lakebase-database-resource-name "<lakebase-database-resource-name>" \
  --lakebase-endpoint-name "<lakebase-endpoint-name>" \
  --lakebase-schema "<lakebase-operational-schema>" \
  --foundation-model "system.ai.<foundation-model-api-model>" \
  --model-service-name "<unity-gateway-model-service-name>" \
  --mlflow-experiment-name "/Shared/<customer-project>-agent" \
  --mlflow-trace-catalog "<unity-catalog-trace-catalog>" \
  --mlflow-trace-schema "<unity-catalog-trace-schema>" \
  --mlflow-trace-table-prefix "<unity-catalog-trace-table-prefix>" \
  --sandbox-capacity 32 \
  --telemetry-samples-per-second 2 \
  --telemetry-source-partitions 4 \
  --skill-id "<governed-skill-id>" \
  --mcp-service-id "<mcp-service-id>" \
  --mcp-connection-id "<mcp-source-connection-id>" \
  --policy-function-name "<submit-work-order-policy-function>" \
  --global-agent-concurrency 20
```

For first installation, rerun that exact command with `install` in place of `preflight`. For a
complete update, use `update`. Omitting the operation is safe and behaves as `preflight`, but an
explicit operation is clearer in change records.

For an eligible App-only change, add `--app-only` to both the `preflight` and `update` commands.
`install --app-only` is rejected: first installation must establish the complete dependency set.
An update may additionally pass
`--app-service-principal-client-id "<expected-existing-app-client-id>"` as an assertion; omit it
in the normal flow so the installer discovers the identity generated for the target App.

`--table-timeout-seconds <seconds>` changes only the readiness wait for the complete data-plane
operation. It does not weaken freshness or acceptance checks.

## Troubleshooting

- **Profile/host mismatch:** stop. Select the intended named profile or correct `expected_host`;
  never weaken the check.
- **App principal mismatch:** verify that the App belongs to this target. Do not substitute a
  principal copied from another workspace.
- **Strict bundle validation fails:** correct the customer values or target permissions. Do not
  edit tracked source to embed target IDs.
- **App is deployed but stopped:** use the checked lifecycle command. A bare bundle upload does
  not prove a running App.
- **RTM is stale:** inspect the continuous job, checkpoint volume grants and Lakebase endpoint;
  do not refresh/delete the checkpoint.
- **Gateway returns `ASK` repeatedly after approval:** stop retries and inspect the persisted
  approval claim/receipt before any manual action. Never bypass Gateway.
- **App-only plan includes non-App resources:** cancel and report a scope-classification defect.

## Decommissioning

Decommissioning is intentionally manual because the catalog, schema, warehouse, access group and
possibly Lakebase project may be customer-owned. First export the required audit/evidence records
and stop App access. Then stop the Rate RTM workload and App before removing bundle-owned jobs,
policies, Model Service, MCP/Skill resources, experiment and trace grants. Remove the project
schema, checkpoint volume, Lakebase data or project only after the customer data owner confirms
retention and ownership. Never run `bundle destroy` against an unreviewed target or assume it owns
the existing catalog, warehouse or group.

