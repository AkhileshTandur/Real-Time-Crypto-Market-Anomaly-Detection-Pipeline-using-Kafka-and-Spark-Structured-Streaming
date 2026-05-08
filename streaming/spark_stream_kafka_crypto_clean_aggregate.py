r"""Consume live crypto trade events from Kafka and compute windowed market signals."""

from __future__ import annotations

import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap_servers", default="localhost:9092")
    p.add_argument("--topic", default="crypto.trades")
    p.add_argument("--out_path", default=str(Path("data") / "stream" / "aggregates_csv"))
    p.add_argument("--checkpoint_path", default=str(Path("data") / "stream" / "checkpoints" / "agg"))
    p.add_argument("--window_seconds", type=int, default=60)
    p.add_argument("--watermark_seconds", type=int, default=120)
    p.add_argument("--trigger_seconds", type=int, default=5)
    p.add_argument("--sink", choices=["csv", "console"], default="csv")
    p.add_argument("--spread_threshold_pct", type=float, default=1.0)
    p.add_argument("--large_trade_value", type=float, default=100000.0)
    p.add_argument("--high_trade_count", type=int, default=500)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("crypto-kafka-spark-clean-aggregate")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )

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

    trades = (
        parsed.withColumnRenamed("s", "symbol")
        .withColumn("price", F.col("p").cast("double"))
        .withColumn("quantity", F.col("q").cast("double"))
        .withColumn("trade_id", F.col("t").cast("long"))
        .withColumn("trade_time", F.to_timestamp((F.col("T") / F.lit(1000)).cast("double")))
    )

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
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.sum("quantity").alias("volume"),
            F.sum("trade_value").alias("notional_volume"),
            F.max("trade_value").alias("max_trade_value"),
            F.count("*").alias("trade_count"),
        )
        .withColumn(
            "price_spread_pct",
            F.when(F.col("avg_price") > 0, ((F.col("max_price") - F.col("min_price")) / F.col("avg_price")) * 100).otherwise(
                F.lit(0.0)
            ),
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
            F.col("symbol"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("price_spread_pct"),
            F.col("volume"),
            F.col("notional_volume"),
            F.col("max_trade_value"),
            F.col("trade_count"),
            F.col("is_anomaly"),
            F.col("anomaly_reason"),
        )
    )

    out_path = args.out_path
    ckpt = args.checkpoint_path

    print("Starting streaming query...")
    print(f"  kafka bootstrap: {args.bootstrap_servers}")
    print(f"  topic: {args.topic}")
    print(f"  out_path: {out_path}")
    print(f"  checkpoint: {ckpt}")

    writer = agg.writeStream.outputMode("append").trigger(processingTime=f"{args.trigger_seconds} seconds")

    if args.sink == "console":
        query = writer.format("console").option("truncate", "false").start()
    else:
        query = (
            writer.format("csv")
            .option("path", out_path)
            .option("checkpointLocation", ckpt)
            .option("header", "true")
            .start()
        )

    query.awaitTermination()


if __name__ == "__main__":
    main()

