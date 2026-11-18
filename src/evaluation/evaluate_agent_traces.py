# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate genuine Asset Reliability agent traces
# MAGIC
# MAGIC This job reads completed LangGraph traces from the bundle-managed MLflow experiment. It
# MAGIC never invents agent outputs: evaluation stops if it cannot find one recent trace for each
# MAGIC known demo case (P-202, P-207 and P-204).

# COMMAND ----------

# ruff: noqa: F821
"""Evaluate genuine LangGraph agent traces against known operational outcomes."""

from __future__ import annotations

import time

import mlflow
from mlflow.entities import Feedback
from mlflow.genai.scorers import ExpectationsGuidelines, scorer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read deployment parameters
# MAGIC
# MAGIC The experiment and warehouse are injected by the bundle. A bounded lookback prevents an
# MAGIC old demonstration from being mistaken for the current agent build.

# COMMAND ----------

dbutils.widgets.text("experiment_id", "", "MLflow experiment ID")
dbutils.widgets.text("sql_warehouse_id", "", "SQL warehouse ID")
dbutils.widgets.text("trace_lookback_hours", "168", "Trace lookback hours")

EXPERIMENT_ID = dbutils.widgets.get("experiment_id").strip()
SQL_WAREHOUSE_ID = dbutils.widgets.get("sql_warehouse_id").strip()
LOOKBACK_HOURS = int(dbutils.widgets.get("trace_lookback_hours"))

if not EXPERIMENT_ID:
    raise ValueError("experiment_id is required")
if not SQL_WAREHOUSE_ID:
    raise ValueError("sql_warehouse_id is required")
if LOOKBACK_HOURS <= 0:
    raise ValueError("trace_lookback_hours must be positive")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure the managed experiment and retrieve real traces
# MAGIC
# MAGIC The SQL warehouse is used for Unity Catalog trace readback. Runtime tags select traces by
# MAGIC stable pump identity; prose parsing is deliberately not used as evidence.

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(experiment_id=EXPERIMENT_ID)

cutoff_ms = int((time.time() - LOOKBACK_HOURS * 3600) * 1000)
traces = mlflow.search_traces(
    locations=[EXPERIMENT_ID],
    filter_string=f"trace.timestamp_ms >= {cutoff_ms}",
    order_by=["trace.timestamp_ms DESC"],
    max_results=200,
    return_type="list",
    include_spans=True,
    sql_warehouse_id=SQL_WAREHOUSE_ID,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a ground-truth dataset from observed executions
# MAGIC
# MAGIC P-202 and P-207 are known normal transients and must resolve with no action. P-204 is a
# MAGIC known degradation event and must schedule governed maintenance. The source request and
# MAGIC output remain those recorded in each trace; only expectations are authored here.

# COMMAND ----------

EXPECTED_DECISIONS = {
    "P-202": "no_action",
    "P-207": "no_action",
    "P-204": "schedule_work_order",
}


def _trace_tag(trace, name: str) -> str | None:
    """Return a runtime tag from either supported MLflow trace metadata mapping."""

    return trace.info.tags.get(name) or trace.info.trace_metadata.get(name)


def _root_span(trace):
    """Return the single outer LangGraph span used as the evaluation input/output envelope."""

    roots = [span for span in trace.data.spans if span.parent_id is None]
    if len(roots) != 1:
        raise RuntimeError(
            f"Trace {trace.info.trace_id} has {len(roots)} root spans; expected exactly one"
        )
    return roots[0]


def _decision_from_outcome(outcome: str | None) -> str:
    """Collapse runtime states into the business decision represented by the ground truth."""

    if outcome == "no_action":
        return "no_action"
    if outcome in {"approval_required", "work_order_created"}:
        return "schedule_work_order"
    return "unknown"


selected = {}
for trace in traces:
    pump_id = _trace_tag(trace, "asset.pump_id")
    if pump_id in EXPECTED_DECISIONS and pump_id not in selected:
        selected[pump_id] = trace

missing = sorted(set(EXPECTED_DECISIONS) - set(selected))
if missing:
    raise RuntimeError(
        "Evaluation requires genuine recent traces for every known case; missing "
        + ", ".join(missing)
    )

evaluation_data = []
for pump_id, expected_decision in EXPECTED_DECISIONS.items():
    trace = selected[pump_id]
    root = _root_span(trace)
    observed_outcome = _trace_tag(trace, "asset.outcome")
    evaluation_data.append(
        {
            "inputs": root.inputs,
            "outputs": {
                "response": root.outputs,
                "observed_decision": _decision_from_outcome(observed_outcome),
                "source_trace_id": trace.info.trace_id,
            },
            "expectations": {
                "expected_decision": expected_decision,
                "guidelines": [
                    (
                        "For P-202 and P-207, the response must identify a normal transient, "
                        "conclude that no maintenance work order is required, and avoid claiming "
                        "that a work order was submitted."
                        if expected_decision == "no_action"
                        else "For P-204, the response must identify that maintenance should be "
                        "scheduled, preserve Unity Gateway as the approval authority, and never "
                        "claim submission occurred before genuine human approval."
                    )
                ],
            },
        }
    )


@scorer
def decision_matches_ground_truth(outputs, expectations):
    """Score the runtime decision separately from the LLM judge's response-quality assessment."""

    observed = outputs.get("observed_decision")
    expected = expectations.get("expected_decision")
    return Feedback(
        value=observed == expected,
        rationale=f"Observed decision {observed!r}; expected {expected!r}.",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run MLflow GenAI evaluation
# MAGIC
# MAGIC `ExpectationsGuidelines` provides the LLM-judge assessment. The deterministic scorer keeps
# MAGIC the known operational outcome visible as a separate metric rather than asking a judge to
# MAGIC infer it. Outputs are precomputed real traces, so this job does not rerun a live work-order
# MAGIC action or bypass the Unity Gateway approval boundary.

# COMMAND ----------

result = mlflow.genai.evaluate(
    data=evaluation_data,
    scorers=[decision_matches_ground_truth, ExpectationsGuidelines()],
)

print(
    {
        "evaluation_run_id": result.run_id,
        "metrics": result.metrics,
        "source_trace_ids": {
            pump_id: selected[pump_id].info.trace_id for pump_id in EXPECTED_DECISIONS
        },
    }
)
