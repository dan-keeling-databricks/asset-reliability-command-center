# Customer clean-install validation

This record tests the public installation guide as a customer would use it: from a clean,
workspace-neutral Git revision, with only a named OAuth profile and values discovered in the
target workspace. It records failures rather than editing around them silently. Workspace
identifiers are intentionally sanitised; no token, cookie, connection string or generated
credential is retained.

## Acceptance method

Each clean-room cycle must:

1. begin with no project App, jobs, Lakebase project, schema or governed agent assets;
2. discover the workspace catalog, warehouse, App group, enabled node type and available model;
3. run the documented read-only `preflight`;
4. run the same inputs through `install`;
5. verify App, RTM, Skill, MCP, Model Service, native-`ASK` and MLflow resources;
6. record every instruction, code and platform issue;
7. remove only the demo-owned resources; and
8. repeat from the revised published instructions without an undocumented correction.

## Attempt 1 — fresh Azure FEVM workspace

**Source:** `d0c6670d3d4493fbf1e06d1f33483a53bb04e02e`  
**Date:** 2026-11-18  
**Starting state:** no Apps, jobs or Lakebase projects; one workspace-local isolated catalog,
one serverless SQL warehouse, and the standard `users` group.

The documented preflight passed: Ruff, 191 Python tests, six frontend behavioural tests,
TypeScript, Vite build and strict bundle validation.

### Findings

| ID | Observation | Classification | Resolution |
| --- | --- | --- | --- |
| CI-001 | App creation failed with `404 NOT_FOUND` for database resource path ending in `databricks_postgres`. The new project reported PostgreSQL database `databricks_postgres` but resource ID `databricks-postgres`. | Instruction defect | The guide and worksheet now distinguish `status.postgres_database` from `database_id` and provide discovery commands. |
| CI-002 | Familiar Azure node types appeared in discovery but were marked `NotEnabledOnSubscription`. | Customer gotcha | The guide now filters for an empty `node_info.status` and warns against copying a node type between workspaces. |
| CI-003 | The first Lakebase migration cold start took about 11 minutes; RTM startup introduced a second long silent wait. | Customer gotcha | The guide now sets a 20–30 minute first-install expectation and explains the two synchronous cold-start gates. |
| CI-004 | `npm ci` reported one high-severity Vite advisory, while the deployment gate did not run `npm audit`. | Code/quality defect | Vite was updated to a non-vulnerable release and high-severity audit is now part of preflight. |
| CI-005 | Databricks Apps package installation reported conflicts with unused packages preinstalled in the base image. | Platform/build warning | The App started successfully and the declared runtime imports do not use those packages. Retain as an observable warning; do not add unrelated dependency pins solely to silence the base image. |
| CI-006 | The data-plane helper reported RTM `RUNNING` with `telemetry_freshness_verified: false`; freshness remains an authenticated App acceptance gate. | Acceptance boundary | Keep installation success separate from end-to-end acceptance, as the guide requires. |
| CI-007 | `bundle destroy` hid the deleted Lakebase project from normal listing but retained a seven-day tombstone. Reinstalling with the same ID failed with `A soft-deleted project currently has this resource name`. | Decommissioning defect | The clean-room procedure now requires direct `get-project` inspection and an explicit, retention-approved `delete-project --purge` for a dedicated demo project. |

After correcting only CI-001 at the command line, the remaining first attempt completed:

- Databricks App deployment succeeded and reached `RUNNING`;
- the continuous Spark Rate RTM task reached `RUNNING`;
- the governed Skill was created and finalised;
- all four MCP tools were discovered from the live service;
- the source-context guard rejected a context-free source path;
- Unity Gateway returned native `ASK` for `submit_work_order` and did not execute it; and
- the managed MLflow experiment and Unity Catalog trace tables were created.

This was not a clean pass because CI-001 required an undocumented correction. The first attempt
to start cycle 2 also failed on CI-007, proving that ordinary list calls were insufficient to
verify a clean Lakebase target. After the exact deleted demo project was explicitly purged,
`get-project` returned `NOT_FOUND`, and the App, Asset Reliability jobs and project schema
inventories were empty. A successful retry from that state is required before this record can
declare the guide clean-install validated.

## Attempt 2 — clean install from the revised public guide

**Source:** `562d57fea906d6c8017a8db6c709fdb5df5ffefe`

**Date:** 2026-11-18

**Starting state:** the App, Asset Reliability jobs, project schema, Model Service, MLflow
experiment, Skill, MCP service and connection, compute policy and bundle files were absent. The
exact dedicated Lakebase tombstone had been purged and `get-project` returned `NOT_FOUND`.

The repository was cloned afresh from the public remote. The documented preflight passed without
an undocumented correction: Ruff, 192 Python tests, six frontend behavioural tests, TypeScript,
Vite build, zero high-severity npm vulnerabilities and strict bundle validation all succeeded.

The documented `install` operation then completed in about 25 minutes using only values discovered
through the customer worksheet. The two expected quiet intervals were the Lakebase migration and
Rate RTM classic-compute cold starts; the printed run URLs were sufficient to distinguish these
waits from a stalled installation. No code, command argument or workspace resource needed an
out-of-band correction.

### Verified installation evidence

- App deployment `01f1e39f4c3c19cc9cc158b1e20f76a4` reached `SUCCEEDED`; the App and its
  compute both reached `RUNNING`/`ACTIVE`.
- The deployed `.build-info.json` reported
  `562d57fea906d6c8017a8db6c709fdb5df5ffefe`.
- RTM parent run `149037512533156` and task run `28071639722798` reached `RUNNING`.
- The live RTM job enables `spark.databricks.streaming.realTimeMode.enabled`, uses fixed classic
  compute and references the configured Lakebase endpoint and Unity Catalog Volume. Its deployed
  notebook was byte-for-byte identical to the selected commit; that source uses Spark Rate, the
  native `postgresql` streaming sink and a Volume checkpoint.
- The governed Skill was created and has a non-empty `finalize_time`.
- The live MCP service discovered exactly `get_asset_telemetry`, `assess_failure_risk`,
  `prepare_work_order` and `submit_work_order`; its source-context guard rejected a context-free
  call.
- The service policy referenced the live Unity Catalog function. The non-executing probe returned
  native `ASK` for `submit_work_order` and did not create a work order.
- MLflow experiment `3568841711763529`, its four managed Unity Catalog OpenTelemetry tables and
  the two trace views were present. All four tables contained zero rows before any user exercised
  the fresh App, so no trace is represented as evidence prematurely.
- App startup logs contained no runtime error. They retained the documented warnings about
  unused packages preinstalled in the Databricks Apps base image; Uvicorn started and live MCP
  proxy requests returned HTTP 200.

The App correctly returned HTTP 401 to an unauthenticated health request. Authenticated
`/api/build-info`, `/api/health`, two-user isolation and the P-202/P-207/P-204 business journey
remain live-acceptance activities: they require an admitted user session and are not silently
replaced with a shell token or fabricated output. Browser/computer-use automation was explicitly
out of scope for this validation. Control-plane evidence independently proved the selected build,
running App, running RTM, deployed source, governance assets and trace storage.

### Result

The revised public instructions now produce a clean customer installation from an empty target
without undocumented corrections. The installation guide is therefore clean-install validated.
This result proves reproducible installation; it does not replace the separate authenticated
business-journey acceptance checklist in `docs/DEPLOYMENT.md`.
