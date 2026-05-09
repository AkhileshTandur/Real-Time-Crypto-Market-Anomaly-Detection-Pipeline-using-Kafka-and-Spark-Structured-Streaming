"""Read the Delta Lake table written by the Coinbase streaming job.

Demonstrates Delta Lake-specific capabilities the CSV sink cannot offer:
  - schema/row-count inspection on a typed table
  - sample anomaly inspection with SQL-style filters
  - table history (one entry per streaming micro-batch)
  - time travel via ``versionAsOf``

Run after at least one streaming commit has landed at the configured
``--delta_path`` (default ``data/stream/coinbase_aggregates_delta``).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument(
        "--delta_path",
        default=str(Path("data") / "stream" / "coinbase_aggregates_delta"),
        help="Filesystem path of the Delta table written by the Coinbase streaming job.",
    )
    p.add_argument("--show_n", type=int, default=10, help="Rows to display in sample output.")
    return p.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("read-coinbase-delta-output")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    delta_path = args.delta_path

    if not Path(delta_path).exists():
        raise FileNotFoundError(
            f"Delta path not found: {delta_path}. "
            "Run streaming/spark_stream_kafka_coinbase_clean_aggregate.py with --sink delta first."
        )
    if not (Path(delta_path) / "_delta_log").exists():
        raise FileNotFoundError(
            f"No _delta_log under {delta_path}. The streaming job has not committed yet."
        )

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading Coinbase Delta table at: {delta_path}")
    df = spark.read.format("delta").load(delta_path)

    print("\n--- SCHEMA ---")
    df.printSchema()

    total = df.count()
    print(f"\nTotal rows in current version: {total:,}")
    if total == 0:
        print("Delta table is empty; need more replayed events for meaningful output.")
        spark.stop()
        return

    if "is_anomaly" in df.columns:
        print(f"\n--- SAMPLE ANOMALY ROWS (up to {args.show_n}) ---")
        df.filter("is_anomaly = true").orderBy("window_start").show(args.show_n, truncate=False)
    else:
        print("\nColumn 'is_anomaly' not found; showing top rows instead.")
        df.show(args.show_n, truncate=False)

    # Time-travel demonstration. Each streaming micro-batch creates a new Delta
    # commit so we can inspect the table history and read prior versions.
    from delta.tables import DeltaTable

    table = DeltaTable.forPath(spark, delta_path)
    history = table.history()
    print("\n--- TABLE HISTORY (most recent first) ---")
    history.select("version", "timestamp", "operation", "operationMetrics").show(20, truncate=False)

    versions = sorted(r["version"] for r in history.select("version").collect())
    if len(versions) >= 2:
        previous = versions[-2]
        print(f"\n--- TIME TRAVEL: reading version {previous} ---")
        prior = spark.read.format("delta").option("versionAsOf", previous).load(delta_path)
        prior_total = prior.count()
        print(f"Rows at version {previous}: {prior_total:,}")
        prior.orderBy("window_start").show(min(args.show_n, 5), truncate=False)
    else:
        print("\nOnly one version present; need more streaming commits to demonstrate time travel.")

    spark.stop()


if __name__ == "__main__":
    main()
