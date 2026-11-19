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
| Deployment | Customer-portable DAB, runtime-only App artifact and standard bundle entry point | Complete |
| Deployment | Customer-portable variables, Lakebase resource-ID guidance and frontend audit gate | Complete |
| Documentation | Architecture, demo, deployment and debt documentation reconciled | Complete |

## Validation

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
