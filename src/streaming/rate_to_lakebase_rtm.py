# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Rate-to-Lakebase Real-Time Mode ingestion
# MAGIC
# MAGIC This standalone Structured Streaming query creates the synthetic pump signal directly from
# MAGIC Spark's Rate source and writes it to Lakebase through the native PostgreSQL sink. One query
# MAGIC serves every personal sandbox: deterministic generator slots are resolved to active
# MAGIC sandboxes and scenarios by the Lakebase `BEFORE INSERT` trigger.
# MAGIC
# MAGIC The query runs on fixed DBR 19 classic compute with Real-Time Mode enabled. Its checkpoint
# MAGIC is isolated from the retired Kafka query in a Unity Catalog volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime dependencies
# MAGIC
# MAGIC The notebook uses only runtime-provided PySpark APIs. All simulation fields are Spark SQL
# MAGIC expressions rather than Python UDFs, keeping the Rate source deterministic and compatible
# MAGIC with Real-Time Mode. JSON is used only for compact progress records in the job logs.

# COMMAND ----------

# ruff: noqa: F821
"""Generate deterministic personal-sandbox telemetry with Spark Rate and RTM."""

from __future__ import annotations

import json
import re
import time

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declare deployment parameters
# MAGIC
# MAGIC The bundle fixes the initial capacity at 32 sandboxes, with eight pumps sampled twice per
# MAGIC second in each slot. Lakebase endpoint and schema values remain deploy-time parameters; no
# MAGIC database password or broker credential is embedded in this source.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "runtime")
dbutils.widgets.text("sandbox_capacity", "32")
dbutils.widgets.text("samples_per_second", "2")
dbutils.widgets.text("source_partitions", "4")
dbutils.widgets.text(
    "lakebase_endpoint",
    "",
)
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("lakebase_table", "asset_reliability.telemetry_events")
# The native Lakebase sink in the demo workspace publishes at RTM epoch boundaries. A short
# epoch keeps telemetry visible without changing the RTM execution mode.
dbutils.widgets.text("trigger_interval", "10 seconds")
dbutils.widgets.text("diagnostic_seconds", "0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and validate configuration
# MAGIC
# MAGIC Configuration is read once before the query starts. The fixed source partition count is
# MAGIC deliberately explicit because RTM schedules all stages concurrently and must be sized by
# MAGIC slots rather than by autoscaling.

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
SANDBOX_CAPACITY = int(dbutils.widgets.get("sandbox_capacity"))
SAMPLES_PER_SECOND = int(dbutils.widgets.get("samples_per_second"))
SOURCE_PARTITIONS = int(dbutils.widgets.get("source_partitions"))
LAKEBASE_ENDPOINT = dbutils.widgets.get("lakebase_endpoint")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_TABLE = dbutils.widgets.get("lakebase_table")
DIAGNOSTIC_SECONDS = int(dbutils.widgets.get("diagnostic_seconds"))

for identifier in (CATALOG, SCHEMA, VOLUME):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", identifier):
        raise ValueError(f"Invalid Unity Catalog identifier: {identifier}")
if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", LAKEBASE_TABLE):
    raise ValueError("lakebase_table must be schema.table using simple identifiers")
if not LAKEBASE_ENDPOINT:
    raise ValueError("lakebase_endpoint is required")
if SANDBOX_CAPACITY <= 0:
    raise ValueError("sandbox_capacity must be positive")
if SAMPLES_PER_SECOND <= 0:
    raise ValueError("samples_per_second must be a positive integer")
if SOURCE_PARTITIONS <= 0:
    raise ValueError("source_partitions must be positive")

PUMP_COUNT = 8
ROWS_PER_SECOND = SANDBOX_CAPACITY * PUMP_COUNT * SAMPLES_PER_SECOND
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/checkpoints/pump_telemetry_rate_rtm"

print("spark.version =", spark.version)
print(
    "spark.databricks.streaming.realTimeMode.enabled =",
    spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "UNSET"),
)
print(
    json.dumps(
        {
            "source": "spark-rate-demo",
            "sandbox_capacity": SANDBOX_CAPACITY,
            "pumps_per_sandbox": PUMP_COUNT,
            "samples_per_second": SAMPLES_PER_SECOND,
            "rows_per_second": ROWS_PER_SECOND,
            "source_partitions": SOURCE_PARTITIONS,
            "checkpoint": CHECKPOINT,
        },
        sort_keys=True,
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query observability
# MAGIC
# MAGIC The listener emits input and processed rates, sink rows, and RTM latency percentiles. These
# MAGIC records let deployment acceptance prove the Rate query is advancing without adding another
# MAGIC sink or a second streaming query.

# COMMAND ----------


class ProgressLogger(StreamingQueryListener):
    """Emit compact Rate RTM progress records to the Databricks job logs."""

    def onQueryStarted(self, event):
        print(f"[RATE RTM START] id={event.id}")

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        print(
            "[RATE RTM PROGRESS]",
            json.dumps(
                {
                    "batch_id": progress.get("batchId"),
                    "input_rows": progress.get("numInputRows"),
                    "input_rps": progress.get("inputRowsPerSecond"),
                    "processed_rps": progress.get("processedRowsPerSecond"),
                    "latencies": progress.get("latencies"),
                    "sink_rows": (progress.get("sink") or {}).get("numOutputRows"),
                }
            ),
        )

    def onQueryTerminated(self, event):
        print(f"[RATE RTM TERMINATED] id={event.id} exception={event.exception}")


spark.streams.addListener(ProgressLogger())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate slot, pump, and sequence coordinates
# MAGIC
# MAGIC The monotonically increasing Rate value is mapped deterministically across pumps first,
# MAGIC then generator slots. Every `(slot, pump)` therefore receives exactly the configured sample
# MAGIC rate. Event identifiers contain only stable coordinates, so checkpoint replay upserts the
# MAGIC same Lakebase row rather than duplicating it.

# COMMAND ----------

rate = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", ROWS_PER_SECOND)
    .option("numPartitions", SOURCE_PARTITIONS)
    .load()
)

coordinates = (
    rate.withColumn("pump_index", F.pmod(F.col("value"), F.lit(PUMP_COUNT)).cast("int"))
    .withColumn(
        "generator_slot",
        F.pmod(F.floor(F.col("value") / F.lit(PUMP_COUNT)), F.lit(SANDBOX_CAPACITY)).cast(
            "int"
        ),
    )
    .withColumn(
        "sequence",
        F.floor(F.col("value") / F.lit(PUMP_COUNT * SANDBOX_CAPACITY)).cast("long"),
    )
    .withColumn("pump_id", F.format_string("P-%03d", F.col("pump_index") + F.lit(201)))
    .withColumn("elapsed_seconds", F.col("sequence") / F.lit(float(SAMPLES_PER_SECOND)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build deterministic correlated measurements
# MAGIC
# MAGIC Per-pump baselines match the original eight-pump estate. Smooth load waves and
# MAGIC deterministic pseudo-noise make every sample reproducible from slot, pump, and sequence.
# MAGIC The shared stream contains no repeating alert excursions. Lakebase applies the one-shot
# MAGIC P-202/P-207 replay relative to each personal scenario and the separately controlled P-204
# MAGIC degradation, keeping all demo controls sandbox-scoped.

# COMMAND ----------


def profile(values: tuple[float, ...]) -> Column:
    """Select one typed baseline value by the zero-based pump index."""

    return F.element_at(F.array(*(F.lit(value) for value in values)), F.col("pump_index") + 1)


flow_base = profile((182.0, 179.0, 185.0, 181.0, 177.0, 184.0, 180.0, 183.0))
pressure_base = profile((8.4, 8.2, 8.5, 8.3, 8.1, 8.4, 8.2, 8.3))
vibration_base = profile((1.20, 1.45, 1.70, 1.95, 2.20, 2.45, 2.70, 2.95))
temperature_base = profile((57.0, 58.0, 56.5, 59.0, 57.5, 56.0, 58.5, 57.0))
current_base = profile((42.0, 41.5, 42.5, 42.0, 41.0, 42.4, 41.8, 42.1))

pump_number = F.col("pump_index") + F.lit(1.0)
elapsed = F.col("elapsed_seconds")
load_wave = F.sin((elapsed / (F.lit(12.0) + pump_number * F.lit(1.7))) + pump_number * 0.73)
vibration_wave = F.sin(
    (elapsed / (F.lit(4.0) + pump_number * F.lit(0.55))) + pump_number * 1.11
)

# Separate deterministic phases avoid correlated noise while remaining stable under replay.
noise_a = F.sin(
    (F.col("sequence") + 1.0) * (pump_number + 2.0) * 1.618
    + (F.col("generator_slot") + 1.0) * 0.731
)
noise_b = F.sin(
    (F.col("sequence") + 3.0) * (pump_number + 5.0) * 0.913
    + (F.col("generator_slot") + 1.0) * 1.127
)
noise_c = F.sin(
    (F.col("sequence") + 7.0) * (pump_number + 11.0) * 0.577
    + (F.col("generator_slot") + 1.0) * 1.931
)


telemetry = coordinates.select(
    F.concat_ws(
        "-",
        F.lit("rate"),
        F.format_string("%02d", F.col("generator_slot")),
        F.col("pump_id"),
        F.format_string("%020d", F.col("sequence")),
    ).alias("event_id"),
    # The Lakebase BEFORE trigger replaces this null with the slot's active scenario before
    # NOT NULL and foreign-key checks execute. Unallocated or expired slots return NULL from the
    # trigger and therefore create no telemetry row.
    F.lit(None).cast("string").alias("scenario_id"),
    F.col("generator_slot"),
    F.col("pump_id"),
    F.col("sequence"),
    F.col("timestamp").alias("event_time"),
    F.round(
        flow_base
        + 2.0 * load_wave
        + 0.7 * noise_a,
        3,
    ).alias("flow_m3h"),
    F.round(
        pressure_base + 0.08 * load_wave + 0.03 * noise_b,
        3,
    ).alias("discharge_pressure_bar"),
    F.round(
        vibration_base
        + 0.10 * load_wave
        + 0.07 * vibration_wave
        + 0.04 * noise_c,
        3,
    ).alias("vibration_mm_s"),
    F.round(temperature_base + 0.35 * load_wave + 0.12 * noise_a, 3).alias(
        "bearing_temperature_c"
    ),
    F.round(
        current_base + 0.5 * load_wave + 0.2 * noise_b,
        3,
    ).alias("motor_current_a"),
    F.lit("spark-rate-demo").alias("source"),
    F.lit(None).cast("timestamp").alias("kafka_timestamp"),
    F.lit(None).cast("int").alias("kafka_partition"),
    F.lit(None).cast("long").alias("kafka_offset"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the native Lakebase sink
# MAGIC
# MAGIC The native PostgreSQL sink supplies workspace-managed authentication and transient retry.
# MAGIC Update mode, deterministic primary-key upserts, a 100 ms buffer interval, and a persistent
# MAGIC UC Volume checkpoint preserve idempotency across long-running RTM epochs and job restarts.

# COMMAND ----------

query = (
    telemetry.writeStream.format("postgresql")
    .outputMode("update")
    .option("endpoint", LAKEBASE_ENDPOINT)
    .option("database", LAKEBASE_DATABASE)
    .option("dbtable", LAKEBASE_TABLE)
    .option("batchinterval", "100 milliseconds")
    .option("batchsize", "1000")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(realTime=dbutils.widgets.get("trigger_interval"))
    .start()
)

print(f"Rate RTM query started: {query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run continuously or return bounded diagnostics
# MAGIC
# MAGIC Production waits for termination so the continuous Lakeflow Job supervises failures. A
# MAGIC positive diagnostic duration returns recent structured progress without changing the
# MAGIC deployed query contract.

# COMMAND ----------

if DIAGNOSTIC_SECONDS > 0:
    deadline = time.monotonic() + DIAGNOSTIC_SECONDS
    while query.isActive and time.monotonic() < deadline:
        time.sleep(5)
    summary = {
        "query_id": str(query.id),
        "is_active": query.isActive,
        "exception": str(query.exception()) if query.exception() else None,
        "recent_progress": [json.loads(item.json) for item in query.recentProgress[-3:]],
    }
    query.stop()
    dbutils.notebook.exit(json.dumps(summary, default=str))
else:
    query.awaitTermination()
