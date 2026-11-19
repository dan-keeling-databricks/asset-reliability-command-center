# Maintainer deployment

Customer installation is defined by `databricks.yml` and documented in
[Install in a customer workspace](CUSTOMER_DEPLOYMENT.md). The deployment entry point is the
standard Databricks bundle command; Git operations never deploy resources.

## Validate a change

Run the repository quality gate before publishing a release:

```bash
uv run ruff check backend tests src scripts
uv run pytest
npm --prefix frontend audit --audit-level=high
npm --prefix frontend run test:unit
npm --prefix frontend run typecheck
npm --prefix frontend run build
```

Validate the bundle with an explicit profile and the four customer-owned values:

```bash
databricks bundle validate --strict --target customer \
  --profile "<profile>" \
  --var catalog="<catalog>" \
  --var schema="<schema>" \
  --var warehouse_id="<warehouse-id>" \
  --var workload_node_type="<node-type>"
```

## Deploy

Set `BUNDLE_VAR_catalog`, `BUNDLE_VAR_schema`, `BUNDLE_VAR_warehouse_id` and
`BUNDLE_VAR_workload_node_type`, then run:

```bash
databricks bundle deploy --target customer --profile "<profile>"
```

DABs builds the runtime-only App source, deploys the declarative resources, runs the required
setup jobs, starts continuous RTM telemetry and starts the App. Re-running the same command applies
an update; no separate App-only or complete-deployment mode is required.

Inspect the installed resources or App logs with:

```bash
databricks bundle summary --target customer --profile "<profile>"
databricks apps logs asset-reliability-center --profile "<profile>"
```

The Kafka, Lakeflow and Genie production-reference definitions remain opt-in and are not included
by the customer target.
