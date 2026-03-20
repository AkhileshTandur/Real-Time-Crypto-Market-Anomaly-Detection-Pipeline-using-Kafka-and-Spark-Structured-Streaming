"""
Spark Structured Streaming consumer:
- Read Binance trades from Kafka topic as JSON
- Clean records (null/invalid price/quantity)
- Window aggregate (1-minute windows per symbol)
- Write aggregates to disk for EDA/visualization

Run example (adjust spark-submit for your environment):
  spark-submit streaming\spark_stream_kafka_binance_clean_aggregate.py ^
    --bootstrap_servers localhost:9092 ^
    --topic binance.trades ^
    --out_path data\\stream\\aggregates ^
    --checkpoint_path data\\stream\\checkpoints\\agg
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap_servers", default="localhost:9092")
    p.add_argument("--topic", default="binance.trades")
    p.add_argument("--out_path", default=str(Path("data") / "stream" / "aggregates_csv"))
    p.add_argument("--checkpoint_path", default=str(Path("data") / "stream" / "checkpoints" / "agg"))
    p.add_argument("--window_seconds", type=int, default=60)
    p.add_argument("--watermark_seconds", type=int, default=120)
    p.add_argument("--trigger_seconds", type=int, default=5)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("binance-kafka-spark-clean-aggregate")
        # Keep partitions reasonable for a class demo
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    # JSON schema matching Binance websocket trade stream.
    trade_schema = T.StructType(
        [
            T.StructField("e", T.StringType(), True),
            T.StructField("E", T.LongType(), True),
            T.StructField("s", T.StringType(), True),
            T.StructField("t", T.LongType(), True),
            T.StructField("p", T.StringType(), True),
            T.StructField("q", T.StringType(), True),
            T.StructField("T", T.LongType(), True),
            T.StructField("m", T.BooleanType(), True),
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

    # Normalize names
    trades = (
        parsed.withColumnRenamed("s", "symbol")
        .withColumn("price", F.col("p").cast("double"))
        .withColumn("quantity", F.col("q").cast("double"))
        .withColumn("trade_id", F.col("t").cast("long"))
        .withColumn("trade_time", F.to_timestamp((F.col("T") / F.lit(1000)).cast("double")))
    )

    # Clean: keep only rows with valid core fields
    trades_clean = trades.filter(
        F.col("symbol").isNotNull()
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
        .groupBy(F.window("trade_time", window_str), F.col("symbol"))
        .agg(
            F.avg("price").alias("avg_price"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("symbol"),
            F.col("avg_price"),
            F.col("volume"),
            F.col("trade_count"),
        )
    )

    out_path = args.out_path
    ckpt = args.checkpoint_path

    print("Starting streaming query...")
    print(f"  kafka bootstrap: {args.bootstrap_servers}")
    print(f"  topic: {args.topic}")
    print(f"  out_path: {out_path}")
    print(f"  checkpoint: {ckpt}")

    query = (
        agg.writeStream.format("csv")
        .outputMode("append")
        .option("path", out_path)
        .option("checkpointLocation", ckpt)
        .option("header", "true")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    # Keep running until you stop it (Ctrl+C).
    query.awaitTermination()


if __name__ == "__main__":
    main()

