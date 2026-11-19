# Install in a customer workspace

The demo is installed and updated with Databricks Declarative Automation Bundles. The customer
selects four workspace-level values; the bundle creates, configures and starts the remaining demo
resources.

## Prerequisites

Install:

- Databricks CLI 1.26 or later;
- Node.js and npm; and
- Git.

Use a workspace that supports Databricks Apps, Lakebase, Spark Real-Time Mode, Unity Gateway,
Foundation Model APIs and MLflow 3. The deploying user needs permission to create resources in an
existing Unity Catalog catalog and to create the App, Lakebase project, jobs, compute policy,
Model Service, experiment, Skill and MCP service used by the demo.

The customer supplies:

| Variable | Meaning |
| --- | --- |
| `catalog` | Existing Unity Catalog catalog in which the project schema is created |
| `schema` | New schema owned by this demo |
| `warehouse_id` | Existing serverless SQL warehouse ID used by setup tasks |
| `workload_node_type` | Classic node type enabled for Real-Time Mode in this workspace |

All App, Lakebase, model, experiment, Skill, MCP and policy names have portable defaults in
`databricks.yml` and can be overridden through ordinary bundle variables if required.

## Install

Clone the public repository and authenticate with an explicit profile:

```bash
git clone https://github.com/dan-keeling-databricks/asset-reliability-command-center.git
cd asset-reliability-command-center

databricks auth login \
  --host "https://<customer-workspace-host>" \
  --profile "<customer-profile>"
```

Set the four customer values:

```bash
export BUNDLE_VAR_catalog="<catalog>"
export BUNDLE_VAR_schema="<new-project-schema>"
export BUNDLE_VAR_warehouse_id="<sql-warehouse-id>"
export BUNDLE_VAR_workload_node_type="<enabled-classic-node-type>"
```

Deploy and start the complete demo:

```bash
databricks bundle deploy --target customer --profile "<customer-profile>"
```

The command builds the React application, creates the Databricks resources, applies the Lakebase
schema, starts continuous Spark Rate telemetry, publishes the governed Skill and MCP service, and
starts the App. The governance reconciliation uses serverless jobs compute, and the command waits
for its Skill, MCP and native-policy validation to finish. Continuous RTM classic compute can take
several minutes to begin producing telemetry after the deployment returns.

Display the installed resources and App URL:

```bash
databricks bundle summary --target customer --profile "<customer-profile>"
```

## Update

Pull the intended release and run the same deploy command. DABs calculates and applies the
required changes:

```bash
git pull --ff-only
databricks bundle deploy --target customer --profile "<customer-profile>"
```

Git operations alone never change the workspace. Only the explicit bundle deployment does so.

