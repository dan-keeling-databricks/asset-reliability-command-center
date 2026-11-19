# Quick Start

Deploy the complete Asset Reliability Command Centre into your Databricks workspace with one
Declarative Automation Bundle command. Allow about 15 minutes for the initial deployment, plus a
few minutes for the classic Real-Time Mode cluster to start.

The values below are synthetic examples. Replace them with values from your workspace.

## 1. Check the prerequisites

Your local machine needs:

- Databricks CLI 1.26 or later;
- Node.js and npm; and
- Git.

```bash
databricks version
node --version
npm --version
git --version
```

The target workspace must support Databricks Apps, Lakebase, Spark Real-Time Mode, Unity Gateway,
Foundation Model APIs and MLflow 3. Your account must be allowed to create the demo resources.

Before continuing, identify:

- an existing Unity Catalog catalog;
- a new schema name for this installation;
- an existing serverless SQL warehouse ID; and
- an enabled classic-compute node type.

For example, this guide uses `main`, `asset_reliability_demo`, `a1b2c3d4e5f67890` and
`i3.xlarge`. An Azure workspace might use `Standard_D4ds_v5` instead of `i3.xlarge`.

## 2. Clone the public repository

```bash
git clone https://github.com/dan-keeling-databricks/asset-reliability-command-center.git
cd asset-reliability-command-center
```

## 3. Sign in to the target workspace

Choose a memorable local profile name and replace the example workspace URL:

```bash
DEMO_PROFILE="asset-reliability-demo"

databricks auth login \
  --host "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
  --profile "${DEMO_PROFILE}"
```

Complete the browser sign-in, then confirm that the profile works:

```bash
databricks current-user me --profile "${DEMO_PROFILE}"
```

## 4. Set the four workspace values

```bash
export BUNDLE_VAR_catalog="main"
export BUNDLE_VAR_schema="asset_reliability_demo"
export BUNDLE_VAR_warehouse_id="a1b2c3d4e5f67890"
export BUNDLE_VAR_workload_node_type="i3.xlarge"
```

The catalog must already exist. The schema must be new. The warehouse ID and node type must be
enabled in the target workspace. Every other App, Lakebase, model, experiment, Skill, MCP and
policy name has a portable default.

## 5. Deploy the demo

```bash
databricks bundle deploy --target customer --profile "${DEMO_PROFILE}"
```

This one command builds the React application and creates, configures and starts the App,
Lakebase project, continuous Spark Rate telemetry, Unity Gateway Model Service, governed Skill,
MCP service and MLflow experiment.

## 6. Open the App

Display the deployed resources and copy the URL shown under **Apps**:

```bash
databricks bundle summary --target customer --profile "${DEMO_PROFILE}"
```

Open the App URL and sign in. Your first visit creates a private pump sandbox and starts the two
one-time normal transients. If the App initially reports that telemetry is starting, wait for the
classic Real-Time Mode cluster to reach `RUNNING`; refreshing the page is not required.

You are ready when the App shows live pump measurements and the platform status is connected.

## Next steps

- Follow the [business demo flow](../DEMO.md).
- Read the [technical architecture](ARCHITECTURE.md).
- Use the [customer installation reference](CUSTOMER_DEPLOYMENT.md) when updating the deployment.
