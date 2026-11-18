# Personal Sandbox and Spark Rate Source Specification

Status: accepted for implementation

## Objective

Make the deployed demo safe for concurrent use by a wider Databricks audience. Every authenticated
App user receives an isolated synthetic pump estate, owns its controls, launches autonomous agent
runs only inside that estate, and approves only the governed actions created by that estate.

Replace the deployed synthetic Kafka producer and Kafka input of the operational Spark Real-Time
Mode (RTM) job with one Spark `rate` source. Do not create one Spark query, Lakebase branch, or
Databricks App per user.

The App's existing Architecture-tab visual remains the production reference architecture and
continues to show Kafka. Repository documentation, health labels, deployment output, and execution
evidence must explicitly distinguish that production reference from the deployed synthetic-demo
adapter, which uses the Spark Rate source.

## User experience decisions

1. All admitted users receive the same product behaviour. Groups control App admission only; they
   do not assign viewer, operator, or supervisor roles.
2. The authenticated Databricks identity automatically owns one active personal sandbox.
3. Every control is scoped to that sandbox: clear/restart, P-204 excursion, follow-up, approval,
   work-order creation, history, cost and execution timeline.
4. Remove the global shift-operator concept, handover menu, recent-user selector and read-only
   banner. No user can see or take over another user's sandbox.
5. The App must derive sandbox identity server-side from trusted Databricks forwarded identity
   headers. The client must never select or submit an arbitrary sandbox identifier.
6. A returning user reopens their active sandbox. Clear/restart archives only that user's current
   scenario and creates a new scenario in the same sandbox.
7. Personal sandboxes expire after a configurable idle period. Expiry releases finite generator
   capacity but preserves immutable governance receipts and historical audit rows.

## Runtime architecture

```text
Authenticated user
    -> Databricks App
    -> server-resolved personal sandbox in Lakebase
    -> sandbox-scoped controls and OBO identity

One Spark RTM query
    -> Spark Rate source
    -> deterministic slot/pump/tick telemetry
    -> native PostgreSQL streaming sink
    -> Lakebase trigger resolves active sandbox + scenario for the slot
    -> Lakebase trigger applies that sandbox's excursion state
    -> append-only telemetry + bounded live state
    -> App evaluator launches sandbox-scoped agent runs
    -> UC Skill + Unity Gateway MCP + native ASK
    -> sandbox-scoped work order and immutable receipt
```

The operational implementation uses Rate because all telemetry is synthetic. The retained
Architecture-tab Kafka path represents how production telemetry would enter the same downstream
RTM, Lakebase, App and governance components.

## Capacity and partitioning

- Configure a finite `sandbox_capacity`, initially 32, through the bundle and RTM task.
- Seed slots `0..sandbox_capacity-1` idempotently in Lakebase.
- Allocate one slot under a row lock when an authenticated user first needs a sandbox.
- Reuse the same slot for the same active sandbox. Reclaim only expired slots.
- Configure the Rate source for two samples per second for eight pumps in every slot. Initial
  target load: `32 * 8 * 2 = 512` source rows per second.
- Derive fields without nondeterministic Python UDFs:
  - `pump_index = pmod(value, 8)`
  - `slot = pmod(floor(value / 8), sandbox_capacity)`
  - `sequence = floor(value / (8 * sandbox_capacity))`
  - `pump_id = P-201 .. P-208`
  - deterministic event ID from slot, pump and sequence
- Preserve a fixed partition count and size RTM slots according to the sum of concurrent stages.
- Use a persistent Unity Catalog Volume checkpoint. Never use DBFS.

## Rate-source telemetry contract

The RTM DataFrame produces the existing physical measurements plus:

- `generator_slot INTEGER`
- deterministic `event_id`
- monotonically increasing per-slot/pump `sequence`
- `source = 'spark-rate-demo'`
- null Kafka provenance columns retained only for backward-compatible storage

Baseline values remain smooth and deterministic. The shared Rate stream contains no repeating
alert excursions. Lakebase derives the P-202 and P-207 normal-transient envelopes from active
scenario time so initial allocation and each explicit replay demonstrate autonomous no-action
reviews exactly once. P-204 degradation is activated separately by a durable, sandbox-scoped
Lakebase control row.

The stream continues to use the native `postgresql` sink, update mode, deterministic primary-key
upserts, a 100 ms batch interval and an RTM trigger. Sink retries and replay must remain idempotent.

## Lakebase schema and migration

Migrations must be idempotent, additive wherever possible, and compatible with the old App and
writer during the ordered deployment window. Do not drop operational or audit tables.

### New tables

`sandboxes`

- `sandbox_id UUID PRIMARY KEY`
- `owner_email TEXT NOT NULL UNIQUE`
- `generator_slot INTEGER NOT NULL UNIQUE`
- `status TEXT CHECK (status IN ('active', 'expired'))`
- `created_at`, `last_seen_at`, `expires_at`
- `version BIGINT` for allocation/restart concurrency

`sandbox_slots`

- `generator_slot INTEGER PRIMARY KEY`
- optional `sandbox_id UUID UNIQUE`
- lease timestamps and version
- rows seeded idempotently to configured capacity

`sandbox_controls`

- one row per sandbox/pump/control type
- P-204 excursion start time, status and version
- immutable initiating user identity and mutable lifecycle timestamps

### Existing-table changes

- Add and backfill `sandbox_id` on scenario, telemetry, live-state, incident, run, conversation,
  activity, work-order, receipt, command, identity and heartbeat paths where direct filtering or
  integrity requires it.
- Replace the global one-active-scenario uniqueness rule with one active scenario per sandbox.
- Retain legacy shift-operator tables for forward-only rollback compatibility but stop reading or
  writing them.
- Existing historical rows are assigned to one deterministic legacy sandbox.
- Index all hot paths with `sandbox_id` first.

### Telemetry trigger

Before accepting a generated event, Lakebase must:

1. Resolve the active, unexpired sandbox lease for `generator_slot`.
2. Resolve that sandbox's active scenario.
3. Return `NULL` for an unallocated/expired slot so no orphan telemetry is stored.
4. Stamp `sandbox_id` and active `scenario_id` onto the row.
5. Apply the active scenario's one-shot P-202/P-207 normal-transient curves.
6. Apply only that sandbox's active P-204 excursion curve.
7. Preserve deterministic source event IDs and sequence fencing.

The existing live-state trigger then upserts `(sandbox_id, scenario_id, pump_id)` and emits a
change notification. Telemetry events remain append-oriented; live state remains bounded.

## Backend isolation contract

- Resolve/create the sandbox from the trusted forwarded email on dashboard and event-stream entry.
- Add a sandbox-scoped repository boundary. Every public repository read/write used by the App,
  evaluator, runtime and MCP must require or derive a sandbox context.
- Dashboard snapshots contain only the authenticated user's sandbox.
- Incident lookup must return 404 when the incident does not belong to the caller's sandbox.
- Trigger, restart, approve and follow-up endpoints derive sandbox server-side and reject stale
  scenario versions.
- Restart cancels only local tasks belonging to that sandbox and archives only that sandbox's
  scenario/runs.
- The evaluator scans active sandboxes but creates incidents with sandbox-scoped uniqueness.
- OBO credentials remain process-local and keyed by sandbox/scenario. They are never persisted.
- Per-sandbox single-flight approval replay and global replay fencing remain intact.
- Apply global and per-sandbox agent concurrency limits so many users cannot create unbounded model
  or Gateway load.

## MCP and agent scoping

The MCP source is reached as the App service principal, so forwarded human headers cannot be used
to infer sandbox identity at the source boundary.

- Create an opaque run context for every agent run and persist its association with run, incident,
  scenario and sandbox.
- Add the internal run context to every MCP tool schema.
- Remove the internal field from the tool schema shown to the model.
- Inject the field server-side immediately before sending the actual Gateway request.
- MCP resolves the run context and rejects unknown, terminal or cross-incident contexts.
- `get_asset_telemetry`, `assess_failure_risk`, `prepare_work_order` and `submit_work_order` read or
  write only the resolved sandbox.
- Work-order creation additionally verifies that the incident, active scenario, pending approval
  replay claim and run context all belong to the same sandbox.
- Continue to discover tools dynamically from Unity Gateway. Do not hardcode tool activity in the
  UI.
- Unity Gateway remains the sole approval authority and the exact paused request is replayed once.

## Frontend contract

- Remove shift-operator state, handover controls and read-only messaging.
- Keep existing Live fleet, History and Architecture tabs and incident review behaviour.
- All control buttons are available because every signed-in user owns their sandbox.
- Update presentation copy from global command-centre control to personal sandbox where needed,
  without adding technical implementation clutter to the primary screen.
- Clear/restart copy must make clear that only the user's sandbox is reset.
- Live-update reconnect behaviour remains unchanged.
- Replace operational labels such as `Kafka -> Lakebase` with truthful source-neutral or
  `Spark -> Lakebase` language. Do not change the Architecture-tab HTML visual.
- Do not fabricate model, Skill, MCP, tool or policy events.

## Databricks resources and deployment

- Preserve the existing RTM bundle resource key/job identity where practical, but change its
  notebook source and parameters from Kafka to Rate.
- Stop deploying or restarting the synthetic Kafka producer as part of the active demo path.
  The old job definition may remain temporarily for forward-only rollback but must not be running.
- The governed Kafka/Lakeflow/Genie assets may remain as inactive production-reference resources;
  deployment and health checks must not require fresh Kafka records from them.
- Keep the Architecture-tab visual unchanged.
- Update the checked full-deployment helper to order:
  1. additive Lakebase migration;
  2. bundle/resource deployment;
  3. keep Kafka/Lakeflow/Genie outside the primary bundle as optional reference assets;
  4. start the Rate RTM job;
  5. reconcile Skill/MCP/Gateway;
  6. deploy the App;
  7. verify Rate telemetry and sandbox isolation.
- This work is never eligible for `--app-only` deployment.
- Every Databricks command must explicitly use `--profile fe_vm_rtm`.

## Access model

- Use one broad account-level App access group (or another explicitly selected existing principal)
  for `CAN_USE` admission.
- Do not create separate viewer/operator behaviour.
- UC/Genie permissions are independent and are not required for the personal operational sandbox
  unless Genie is deliberately retained for those users.
- App admission does not replace server-side sandbox isolation.

## Health and observability

- Replace producer/Kafka freshness with Rate-RTM generator freshness.
- Health is `ok` only when Lakebase is reachable, the Rate RTM heartbeat is fresh, telemetry exists
  for the current sandbox, and the evaluator heartbeat is fresh.
- Latency names and calculations must be truthful for Rate -> Lakebase -> App.
- RTM logs emit input rate, processed rate, sink rows and latency percentiles.
- Record active/expired sandbox counts, slot utilisation, telemetry rows/sec, agent concurrency and
  approval-wait counts without exposing user tokens.

## Retention and cleanup

- Default sandbox idle TTL: two hours, configurable.
- Expiry stops new telemetry by releasing the slot and archives active scenarios/runs safely.
- Never delete governance receipts.
- Retain operational telemetry for 24 hours using existing bounded cleanup patterns, now scoped and
  indexed by sandbox.
- Lakehouse Sink remains the intended durable path for long-term operational and AI audit data.

## Test requirements

### Unit and contract tests

- Two users obtain different sandbox IDs, slots, scenarios and snapshots.
- Triggering or restarting user A does not change user B.
- User A cannot fetch, follow up, approve or create a work order for user B's incident.
- Concurrent first access allocates one slot per user without duplicates.
- Expired slots are reclaimed without mutating immutable audit history.
- The Rate source uses deterministic slot/pump/sequence/event-ID expressions and contains no Kafka
  source configuration.
- Native PostgreSQL sink and UC Volume checkpoint contracts remain present.
- Unallocated slots do not create telemetry.
- Baseline excursions run once per active scenario and affect only its P-202/P-207 telemetry.
- The manually triggered degradation affects only the targeted sandbox and P-204.
- MCP run context is injected after model selection and cross-sandbox context is rejected.
- Approval replay remains exactly once under concurrent retries.
- Frontend contains no handover/read-only operator path and retains all runtime-driven activity.
- Deployment tests prove the primary bundle contains no optional continuous jobs and only the Rate
  RTM job is started.

### Integrated acceptance

1. Open the App as two distinct Databricks users.
2. Confirm different sandbox/scenario IDs and independent live pump values.
3. Trigger P-204 for user A; user B remains at baseline.
4. Confirm user A's agent discovers the governed Skill/MCP tools and reaches native `ASK`.
5. Confirm user B cannot see user A's incident or pending approval.
6. Approve user A once and verify one work order and immutable receipt in user A's sandbox.
7. Trigger and approve independently for user B.
8. Restart user A and confirm user B's scenario and agent state are unchanged.
9. Verify RTM physical plan uses `RealTimeStreamScan` and the Rate source.
10. Verify App health, Rate RTM heartbeat, slot utilisation and source-to-App latency.

## Documentation and evidence

- Preserve historical committed Kafka execution evidence as an explicitly dated archive.
- Add new evidence only from a genuine deployed Rate-source run.
- README, Architecture, Deployment, Demo and Tasks must distinguish:
  - deployed synthetic demo: Spark Rate -> RTM -> Lakebase;
  - production reference visual: Kafka -> RTM -> Lakebase.
- Do not claim current Kafka, governed-history or Genie execution after those paths become inactive.

## Agent-team ownership

- Data-plane agent owns `src/`, `databricks.yml`, and data-plane/deployment scripts and tests.
- State/runtime agent owns `sql/lakebase_schema.sql`, `backend/`, and backend tests.
- Frontend agent owns `frontend/` and frontend asset tests.
- Coordinator owns this specification, repository documentation, integration, conflict resolution,
  end-to-end validation, source control, deployment and live acceptance.

Agents must not modify files outside their ownership without coordinating first. Every agent works
in a separate `feat/{feature_name}` worktree, commits narrowly scoped changes, and reports its
commit hash plus validation results to the coordinator.
