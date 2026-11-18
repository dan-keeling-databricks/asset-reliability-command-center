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

After correcting only CI-001 at the command line, the remaining first attempt completed:

- Databricks App deployment succeeded and reached `RUNNING`;
- the continuous Spark Rate RTM task reached `RUNNING`;
- the governed Skill was created and finalised;
- all four MCP tools were discovered from the live service;
- the source-context guard rejected a context-free source path;
- Unity Gateway returned native `ASK` for `submit_work_order` and did not execute it; and
- the managed MLflow experiment and Unity Catalog trace tables were created.

This was not a clean pass because CI-001 required an undocumented correction. A successful second
cycle after full demo-resource removal is required before this record can declare the guide
clean-install validated.
