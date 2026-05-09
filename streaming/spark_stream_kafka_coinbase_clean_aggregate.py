"""Consume normalized Coinbase trade events from Kafka and compute windowed market signals.

This is the Coinbase-native version of the streaming job. Records on the
Kafka topic follow the project's normalized schema (see
``producer/coinbase_collector.py``):

    {"source": "coinbase", "product_id": "BTC-USD", "trade_id": ...,
     "price": "97000.50", "quantity": "0.123",
     "trade_time": "2026-05-08T12:34:56.123456Z",
     "side": "buy", "collected_at": "...", "raw_type": "match"}

The job:
  1. Reads the Kafka topic (``coinbase.trades``).
  2. Parses each value as JSON using the normalized schema.
  3. Cleans invalid records (null product_id/trade_time, non-positive
     price/quantity, parse failures).
  4. Applies an event-time watermark and tumbling window aggregation per
     product_id.
  5. Computes per-window market signals and rule-based anomaly flags.
  6. Writes to one of three sinks: console, csv, or Delta Lake.

Delta Lake is enabled only when ``--sink delta`` is selected so console/csv
runs do not need the Delta jars on the classpath.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--bootstrap_servers", default="localhost:9092")
    p.add_argument("--topic", default="coinbase.trades")
    p.add_argument(
        "--out_path",
        default=str(Path("data") / "stream" / "coinbase_aggregates_csv"),
        help="CSV sink output path.",
    )
    p.add_argument(
        "--delta_path",
        default=str(Path("data") / "stream" / "coinbase_aggregates_delta"),
        help="Delta sink output path.",
    )
    p.add_argument(
        "--checkpoint_path",
        default=str(Path("data") / "stream" / "checkpoints" / "coinbase_agg"),
    )
    p.add_argument("--window_seconds", type=int, default=60)
    p.add_argument("--watermark_seconds", type=int, default=120)
    p.add_argument("--trigger_seconds", type=int, default=5)
    p.add_argument("--sink", choices=["csv", "console", "delta"], default="delta")
    p.add_argument("--spread_threshold_pct", type=float, default=1.0)
    p.add_argument("--large_trade_value", type=float, default=100000.0)
    p.add_argument("--high_trade_count", type=int, default=500)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    builder = (
        SparkSession.builder.appName("coinbase-kafka-spark-clean-aggregate")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.caseSensitive", "true")
    )

    # Delta integration: register Delta SQL extension and catalog only when
    # the delta sink is selected. console/csv runs never load Delta so they
    # stay portable to plain pyspark installs.
    if args.sink == "delta":
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

    spark = builder.getOrCreate()

    # Schema matches the normalized Coinbase payload (see module docstring).
    trade_schema = T.StructType(
        [
            T.StructField("source", T.StringType(), True),
            T.StructField("product_id", T.StringType(), True),
            T.StructField("trade_id", T.LongType(), True),
            T.StructField("price", T.StringType(), True),
            T.StructField("quantity", T.StringType(), True),
            T.StructField("trade_time", T.StringType(), True),
            T.StructField("side", T.StringType(), True),
            T.StructField("collected_at", T.StringType(), True),
            T.StructField("raw_type", T.StringType(), True),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    value_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
    parsed = value_df.select(F.from_json("json_str", trade_schema).alias("d")).select("d.*")

    trades = (
        parsed.withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("double"))
        # Coinbase uses ISO-8601 strings (timezone-aware). Spark's timestamp
        # cast handles that directly.
        .withColumn("trade_time", F.to_timestamp(F.col("trade_time")))
    )

    trades_clean = trades.filter(
        F.col("product_id").isNotNull()
        & F.col("trade_id").isNotNull()
        & (F.col("price") > 0)
        & (F.col("price") < 1e9)
        & (F.col("quantity") > 0)
        & (F.col("quantity") < 1e9)
        & F.col("trade_time").isNotNull()
    )

    trades_clean = trades_clean.withColumn("trade_value", F.col("price") * F.col("quantity"))

    window_str = f"{args.window_seconds} seconds"

    agg = (
        trades_clean.withWatermark("trade_time", f"{args.watermark_seconds} seconds")
        .groupBy(F.window("trade_time", window_str), F.col("product_id"))
        .agg(
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("trade_value").alias("total_trade_value"),
            F.max("trade_value").alias("max_trade_value"),
            F.count("*").alias("trade_count"),
        )
        .withColumn(
            "price_spread_pct",
            F.when(
                F.col("avg_price") > 0,
                ((F.col("max_price") - F.col("min_price")) / F.col("avg_price")) * 100,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "is_anomaly",
            (F.col("price_spread_pct") >= F.lit(args.spread_threshold_pct))
            | (F.col("max_trade_value") >= F.lit(args.large_trade_value))
            | (F.col("trade_count") >= F.lit(args.high_trade_count)),
        )
        .withColumn(
            "anomaly_reason",
            F.concat_ws(
                ",",
                F.when(F.col("price_spread_pct") >= F.lit(args.spread_threshold_pct), F.lit("price_spread")),
                F.when(F.col("max_trade_value") >= F.lit(args.large_trade_value), F.lit("large_trade")),
                F.when(F.col("trade_count") >= F.lit(args.high_trade_count), F.lit("traffic_spike")),
            ),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("product_id"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("price_spread_pct"),
            F.col("total_quantity"),
            F.col("total_trade_value"),
            F.col("max_trade_value"),
            F.col("trade_count"),
            F.col("is_anomaly"),
            F.col("anomaly_reason"),
        )
    )

    print("Starting Coinbase streaming query...")
    print(f"  kafka bootstrap: {args.bootstrap_servers}")
    print(f"  topic:           {args.topic}")
    print(f"  sink:            {args.sink}")
    print(f"  out_path:        {args.out_path}")
    print(f"  delta_path:      {args.delta_path}")
    print(f"  checkpoint:      {args.checkpoint_path}")

    writer = agg.writeStream.outputMode("append").trigger(
        processingTime=f"{args.trigger_seconds} seconds"
    )

    if args.sink == "console":
        query = writer.format("console").option("truncate", "false").start()
    elif args.sink == "delta":
        # Delta sink: each micro-batch becomes a transactional commit so the
        # table supports time travel and ACID reads. Pair with
        # processing/read_coinbase_delta_output.py.
        query = (
            writer.format("delta")
            .option("path", args.delta_path)
            .option("checkpointLocation", args.checkpoint_path)
            .start()
        )
    else:
        query = (
            writer.format("csv")
            .option("path", args.out_path)
            .option("checkpointLocation", args.checkpoint_path)
            .option("header", "true")
            .start()
        )

    query.awaitTermination()


if __name__ == "__main__":
    main()
