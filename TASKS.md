# Build and deployment status

Target workspace: `fevm-classic-stable-realtime-demos` (`fe_vm_rtm`). The project uses the
existing `classic_stable_realtime_demos_catalog` and dedicated
`asset_reliability_command_center` schema. It creates no additional catalog or access group.

## Implemented in the repository

| Domain | Capability | Status |
| --- | --- | --- |
| Source | One shared Spark Rate source with deterministic telemetry for finite sandbox slots | Complete |
| Operational ingestion | Rate-to-Lakebase standalone Spark Real-Time Mode on dedicated classic compute | Complete |
| Isolation | Server-resolved personal sandbox, scenario and controls for every authenticated user | Complete |
| Production reference | Retained Kafka and governed-history assets, inactive in the synthetic demo | Complete |
| Lakebase | Live telemetry, current state, commands, incidents, runs, activities, work orders, identities, and heartbeats | Complete |
| Lakebase | Concurrency-safe ordering, approval replay lease, and immutable governance receipts | Complete |
| Autonomous AI | Alert-generated prompts and up to 20 concurrent LangGraph runs, including within one sandbox | Complete |
| Autonomous AI | Unity Gateway Model Service routing to the selected Foundation Model API | Complete |
| Observability | Managed MLflow experiment, UC traces, real-trace GenAI evaluation and App links | Complete |
| Governance | One configured MCP service with runtime `tools/list` discovery | Complete |
| Governance | Source MCP restricted to the App service principal | Complete |
| Governance | Reconciled selectors and native `ASK` policy with non-executing probe | Complete |
| Governance | Sandbox-scoped MCP context, restart isolation and single-flight exact-request replay | Complete |
| Production reference | Reproducible telemetry-only Genie definition and OBO adapter retained but hidden | Complete |
| App | Personal live fleet, alerting, autonomous activity, history and incident timeline | Complete |
| App | End-to-end health from connectivity, Rate RTM, telemetry and evaluator freshness | Complete |
| Deployment | Customer-portable bundle, generated App manifest and checked lifecycle entry point | Complete |
| Deployment | Customer clean-install validation, Lakebase resource-ID guidance and frontend audit gate | Complete |
| Documentation | Architecture, demo, deployment, debt, and evidence plan reconciled | Complete |

## Validation state

The integrated repository must pass this gate before deployment:

```bash
uv run ruff check backend tests src scripts
uv run pytest
npm --prefix frontend audit --audit-level=high
npm --prefix frontend run typecheck
npm --prefix frontend run build
databricks bundle validate --strict --target dev --profile fe_vm_rtm <explicit --var values>
```

The feature commits on which this documentation is based include:

- governed SQL pipeline and reproducible Genie Agent;
- OBO Genie App integration;
- governed runtime and deployment reconciliation hardening;
- governed LangGraph, Model Service and MLflow evaluation integration.

## Deployment state

The earlier Kafka command-centre build is deployed, with producer job `952037348530266` and RTM
job `17156207332698`. The personal-sandbox Rate-source redesign must not be marked deployed until
`scripts/deploy.py update` and the current
[live acceptance checklist](docs/DEPLOYMENT.md) complete against the merged commit.

After that acceptance, replace this section with the actual:

- merged Git commit and App deployment ID;
- Rate RTM parent/task run IDs and proof that optional production-reference jobs are absent or paused;
- two distinct sandbox, slot and scenario IDs;
- independent incident, work-order and governance-receipt IDs;
- App health/build-info output and observed latency;
- link to the committed one-time text [evidence package](docs/EVIDENCE.md).

## Remaining work before validator submission

1. Merge all approved feature branches into `main` and rerun the complete local gate.
2. Run checked preflight, review the selected workspace/user/commit, then execute deployment.
3. Complete two-user sandbox isolation, MCP denial, native approval and restart validation.
4. Capture and commit the one-time sanitized text evidence package.
5. Confirm repository access separately; presentation-deck work is intentionally outside this
   technical implementation scope.
