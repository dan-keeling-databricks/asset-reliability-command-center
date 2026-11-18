# Customer-portable demo specification

Status: accepted for implementation

## Outcome

Package the Asset Reliability Command Centre as a customer-installable Databricks demo with no
dependency on the original demo workspace, user, service principal, catalog, schema, warehouse,
Lakebase project, App name or access group. A deployer supplies environment-specific values at
installation time; the repository remains a reusable source package.

The only active experience is the personal-sandbox demo: one shared Spark Rate source generates
finite synthetic telemetry slots, and every authenticated App user receives an isolated pump
estate with independent incidents, agent runs, approvals and work orders. Kafka, Lakeflow and
Genie are production-reference assets, not prerequisites for the default installation.

## Installation contract

The installation interface must require or derive these values:

- Databricks CLI profile and expected workspace host;
- existing Unity Catalog catalog and project schema name;
- existing SQL warehouse ID;
- App name and existing App access group;
- Lakebase project ID, branch, database, endpoint and operational schema;
- sandbox capacity, Rate samples per second and fixed source partition count;
- Foundation Model API registered-model name and Unity Gateway Model Service name;
- governed Skill name, MCP service name and tool-selection policy name;
- MLflow experiment workspace path and Unity Catalog trace destination;
- optional production-reference access group and Genie parent path.

No profile is selected implicitly. Every Databricks CLI command receives the deployer's explicit
`--profile`. Workspace resource IDs must come from bundle references or explicit variables, never
from the maintainer's environment.

## Resource model

The Declarative Automation Bundle owns or references:

1. Lakebase project and production branch resources used by the App and RTM writer.
2. Unity Catalog Volume for streaming checkpoints.
3. Least-privilege classic-compute policy.
4. One-shot Lakebase migration and grant jobs.
5. One standalone Spark Real-Time Mode Rate-to-Lakebase job.
6. Databricks App with forwarded user identity.
7. Unity Gateway Model Service routing to the selected Foundation Model API.
8. Managed MLflow experiment with Unity Catalog trace storage.
9. Real-trace GenAI evaluation job.
10. Optional inactive production-reference Kafka, Lakeflow and Genie resources.

The App service principal must be resolved from the created App resource wherever the platform
supports that reference. If a resource requires the principal before creation, installation must
use an explicit bootstrap/reconciliation phase rather than a committed client ID.

## Generated App configuration

`app.yaml` is an environment-neutral template or generated artifact input. Preflight produces the
runtime copy inside `.artifacts/app` from the selected bundle variables. It must:

- attach Lakebase, SQL warehouse, Unity Gateway Model Service and MLflow experiment resources;
- derive MCP, Skill and governed-document URLs from `DATABRICKS_HOST` plus configured
  Unity Catalog names;
- inject the model-service three-part name, experiment ID, trace destination and warehouse ID;
- retain `ai-gateway` user scope for OBO Gateway calls;
- contain no maintainer workspace host, organisation ID, principal ID or resource ID.

Generated configuration is ignored by Git and rebuilt for every checked deployment. App-only
updates stage and deploy only the App snapshot; they never reconcile, restart or redeploy the
data plane, Lakebase, Gateway resources, experiment or evaluation job.

## LangGraph and governance invariants

The autonomous runtime stays inside Databricks Apps and uses an explicit LangGraph:

1. load incident and in-memory OBO context;
2. load the governed Unity Catalog Skill;
3. discover MCP tools dynamically through Unity Gateway;
4. invoke the Unity Gateway Model Service backed by a Foundation Model API;
5. execute only the model-selected governed MCP capability;
6. loop over evidence when another decision is required;
7. pause on a genuine Unity Gateway `ASK`;
8. resume from Lakebase and replay the exact claimed request once;
9. complete a governed action or resolve with no action.

The UI never fabricates Skill, MCP, model, policy, tool-call or trace activity. OBO tokens and
opaque run contexts remain process-local and outside traceable graph state. Lakebase remains the
durable approval/checkpoint authority.

## MLflow tracing and evaluation

App startup enables `mlflow.langchain.autolog()` before the first graph run and binds the managed
experiment to the configured Unity Catalog trace schema. Root traces carry stable, non-secret
attributes for pump, incident and genuine terminal outcome. Trace links are constructed only from
the injected workspace host, numeric experiment ID and captured trace ID.

The evaluation job reads real persisted P-202, P-207 and P-204 traces. It must fail clearly when
the required examples do not exist; it must not synthesize model outputs or claim an evaluation
from design fixtures. Expectations cover no-action decisions for P-202/P-207 and governed-action
progression for P-204. `mlflow.genai.evaluate` records built-in and custom scorers in the managed
experiment, creating the basis for future cost/performance model comparison.

## Deployment sequence

The checked full installer performs:

1. local dependency, TypeScript, unit, artifact and bundle validation;
2. authenticated host/profile verification;
3. catalog, schema, warehouse, model and access-group preflight;
4. bundle deployment of foundational resources;
5. Lakebase migration and schema verification;
6. Unity Catalog grants for RTM, App, MLflow trace tables and evaluation;
7. Rate RTM start and freshness verification;
8. Skill, MCP service and Unity Gateway policy reconciliation;
9. generated App staging and App deployment;
10. live sandbox, isolation, agent, native-ASK and observability acceptance checks.

All remote mutation remains behind an explicit execute flag. Git push is never deployment.

## Acceptance criteria

- Repository and staged App scans find no original workspace host, organisation ID, service
  principal, warehouse ID, catalog or schema outside an explicitly labelled example file.
- A new customer configuration passes strict bundle validation without editing tracked source.
- First access creates an isolated sandbox and exactly one P-202 and P-207 normal transient.
- Reset creates a new scenario with exactly one fresh pair; reconnect cannot repeat either event.
- Two users can run agents concurrently without a cross-sandbox queue.
- Inference reaches the configured Foundation Model API through the Unity Gateway Model Service.
- Dynamic MCP discovery, native `ASK`, exact replay and immutable receipt behaviour remain intact.
- The Agent graph tab opens the configured MLflow experiment/traces and distinguishes design from
  runtime evidence.
- App-only deployment commands cannot mutate non-App bundle resources.
- The install, update, App-only update, rollback and uninstall boundaries are documented with a
  complete customer configuration example.
