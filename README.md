# Asset Reliability Command Centre

A working Databricks prototype for a process-manufacturing reliability problem: detect pump
bearing degradation early, autonomously assess the risk of failure before the next maintenance
window, and govern the resulting maintenance action with human approval.

## Business value

Process manufacturers and power generators depend on pumps for cooling, lubrication, and fluid
movement. A developing bearing fault can therefore become lost production, a forced outage, or
equipment damage if the organization detects it too late. Industry research gives the problem
material scale:

- Poor maintenance strategies can reduce a plant's productive capacity by **5–20%**, while
  unplanned downtime costs industrial manufacturers an estimated **$50 billion per year**
  ([Deloitte Insights, 2017](https://www2.deloitte.com/us/en/insights/focus/industry-4-0/using-predictive-technologies-for-asset-maintenance.html)).
- Pumps represent **27% of the electricity used by industrial systems in manufacturing**. The
  same U.S. Department of Energy guidance notes that pump downtime can cause substantial
  productivity loss in power plants
  ([U.S. Department of Energy, 2006](https://www.energy.gov/sites/prod/files/2014/05/f16/pump.pdf)).

The demo shows an operating model for intervening earlier. Continuous telemetry identifies a
threshold breach, an autonomous agent distinguishes a normal transient from a high-risk
degradation pattern, and a consequential maintenance work order stops for policy-controlled
human approval. Operations teams retain a complete record of the evidence, model activity,
tool calls, approval, and resulting action.

An illustrative target-site projection makes the value test concrete. For 24 critical pumps,
assume eight forced incidents a year, 7.5 downtime hours per incident, and £80,000 of operational
impact per hour. A 25% reduction would move the site from 60.0 to 45.0 downtime hours a year,
recover 15.0 hours of capacity, and reduce annual exposure from £4.8M to £3.6M. That is **£1.2M
of projected annual value** for the funding executive and, for the reliability leader, an
operational target of shifting two forced failures into planned work. These are transparent
illustrative assumptions, not observed prototype results; a buyer should replace them with its
asset register, failure history, downtime duration, and impact per hour.

The intended business outcome is protected availability with less emergency work. A production
deployment should measure value against an asset-class baseline using avoided forced-outage or
lost-production hours, the shift from emergency to planned maintenance, time from threshold
breach to risk decision, pumping energy intensity, and agent cost per completed workflow. The
public figures above establish the size of the industry problem; they are not claimed results of
this prototype.

The deployed synthetic demo implements one isolated data and AI journey per authenticated user:

```text
One shared Spark Rate source → Spark Real-Time Mode → Lakebase sandbox slots
    → authenticated user's personal pump estate → Databricks App command centre

Sandbox alert → code-defined Sonnet agent → UC Skill → Unity Gateway MCP
    → native ASK approval → governed work order
```

The Rate source is the demo adapter: one RTM query deterministically generates finite telemetry
slots and writes them through the native PostgreSQL sink. Lakebase maps a leased slot to the
authenticated user's active sandbox, applies that sandbox's excursion controls, and isolates all
operational, agent, approval, and work-order state. The Architecture tab intentionally remains the
production reference, where real plant telemetry would arrive through Kafka without changing the
downstream RTM, Lakebase, App, or governance pattern.

## What is in the build

- One shared Spark Rate source producing eight pumps for each of 32 initial sandbox slots.
- Standalone Rate-to-Lakebase Spark Real-Time Mode ingestion with deterministic replay keys.
- Server-resolved personal sandboxes with no shared operator or cross-user control path.
- A retained, inactive Kafka/Lakeflow implementation as the production-reference path.
- Unity Catalog tables, view, lineage, comments, a governed Skill, MCP service, and policy.
- Lakebase operational state, command, incident, agent, approval, and audit records.
- A code-defined autonomous agent using only `databricks-claude-sonnet-4-5`.
- Unity Gateway tool discovery and native `ASK` immediately before work-order submission.
- A React/FastAPI Databricks App with live fleet state, incident review, and agent activity.
- An OBO **Fleet telemetry analyst** drawer backed by a telemetry-only Genie Agent.
- Slot leases, sandbox-scoped restart isolation, single-flight approval replay, immutable
  governance receipts, concurrency-safe timeline ordering, and end-to-end freshness health.

All operational data is synthetic. No customer data belongs in this repository.

## Historical Kafka-build execution evidence

The following dated archive came from the earlier Kafka-based integrated deployment in the target
Databricks workspace on 2026-10-12. It proves that build only; it is not evidence for the current
Rate-source implementation. The outputs remain reproduced here so a text-only validator can
inspect the prior governed journey without following a directory link. The deployed App reported source commit
`940c383fa91f9bd98f5ca0994d370b820396e582`; later repository commits add only documentation and
sanitized evidence.

### Lakeflow pipeline run output and row counts

Pipeline `a0efdccf-440d-479b-8fc2-7cceb0bb3da2`, update
`66ebb87e-a19a-4d42-b647-4a139bbe155b`, reported `RUNNING`. Its event log reported the Bronze,
Silver, and quarantine flows as `RUNNING`. SQL statement
`01f1c626-d929-126b-9256-5813b00c6ab5` completed with:

```text
telemetry_bronze       3,884,768
telemetry_silver       3,884,704
telemetry_quarantine           0
pump_telemetry_history 3,884,704
```

### Genie conversation, generated SQL, and result

The App submitted this question through the signed-in user's OBO identity:

```text
Which pump had the highest average vibration over the last 10 minutes,
and what was its average vibration?
```

Genie reached `COMPLETED`, generated SQL containing
`ROUND(AVG(vibration_mm_s), 1)` over `pump_telemetry_history`, returned
`P-204 | 7.6`, and answered:

```text
For the last 10.0 minutes, pump P-204 had the highest average vibration at 7.6 mm/s.
```

### Autonomous agent and governed work-order execution

Incident `13c50285-aba5-444c-b5b6-bba810f2a452` autonomously started agent run
`74b9404f-f427-477f-a3ee-c0300efe7bdf`. The live timeline recorded:

```text
get_asset_telemetry   ALLOW  Executed      Vibration 5.845 mm/s
assess_failure_risk   ALLOW  Executed      Failure risk 96% (high)
prepare_work_order    ALLOW  Executed      Bearing available; duration 3.5 h
submit_work_order     ASK    Not executed  Waiting for human approval
submit_work_order     ALLOW  Executed      WO-35374 scheduled
```

The completed run used 9,715 input tokens, 892 output tokens, four tool calls, and an estimated
`$0.042525`. Unity Gateway created immutable approval receipt
`448ba4ce-f5ff-416a-bc11-e21ed1f1dde9`. A direct human call to `/mcp` returned HTTP `403` with
`Gateway source principal required`.

### Deployed Databricks App response

```json
{
  "health": {"status": "ok", "adapter": "lakebase"},
  "build_info": {"git_commit": "940c383fa91f9bd98f5ca0994d370b820396e582"},
  "platform": {
    "connection": "connected",
    "kafka_to_lakebase_latency_ms": 57,
    "lakebase_to_ui_latency_ms": 33,
    "end_to_end_latency_ms": 90
  },
  "pump_204": {"state_label": "Maintenance scheduled", "maintenance_label": "WO-35374 scheduled"}
}
```

The consolidated, keyword-indexed record is
[Databricks end-to-end execution evidence](DATABRICKS_END_TO_END_EXECUTION_EVIDENCE.md). A
[committed notebook with visible outputs](notebooks/DATABRICKS_END_TO_END_EXECUTION_EVIDENCE.ipynb)
and product-adjacent execution records provide additional text-readable entry points.

## Documentation

- [Executed validator evidence](evidence/README.md)
- [Business presentation (PDF)](docs/deck/asset-reliability-command-center.pdf)
- [Business presentation (text-readable Markdown)](docs/deck/asset-reliability-command-center.md)
- [Business presentation (Google Slides)](https://docs.google.com/presentation/d/1ZjGVpcxIoZDMIautKpRJVBGNnRTJNg9WKH3mFv0_1kI/edit)
- [Business demo flow](DEMO.md)
- [Technical architecture](docs/ARCHITECTURE.md)
- [Manual deployment and live acceptance](docs/DEPLOYMENT.md)
- [One-time text evidence package](docs/EVIDENCE.md)
- [Technical debt and accepted tradeoffs](docs/TECHNICAL_DEBT.md)
- [Current implementation status](TASKS.md)
- [Contribution and branching model](CONTRIBUTING.md)
- [Instructions for coding agents](AGENTS.md)

## Local development

Prerequisites are `uv`, Node.js/npm, and Databricks CLI 1.17 or later.

```bash
uv sync --locked
npm --prefix frontend ci
./scripts/dev.sh
```

Open `http://127.0.0.1:5173`. `APP_ADAPTER=local` provides a deterministic command-centre
fixture without Spark, Lakebase, model serving, or Unity Gateway. Genie is intentionally a
deployed OBO integration and is unavailable without a configured Genie space and forwarded
Databricks user token.

## Validation

```bash
uv run ruff check backend tests src scripts
uv run pytest
npm --prefix frontend run typecheck
npm --prefix frontend run build
uv run python scripts/deploy.py \
  --profile fe_vm_rtm \
  --target dev \
  --expected-host https://fevm-classic-stable-realtime-demos.cloud.databricks.com \
  --access-group users \
  --catalog classic_stable_realtime_demos_catalog \
  --schema asset_reliability_command_center \
  --app-name asset-reliability-center
```

The deployment preflight also stages the runtime-only App source and validates the bundle without
changing the workspace. Deployment remains manual. Use only the checked entry point documented in
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md); a Git push or merge never changes the workspace.
