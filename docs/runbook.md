# Runbook

## Start Services

```bash
docker compose up -d kafka kafka-ui
```

Expected containers:

```text
crypto-kafka
crypto-kafka-ui
```

Spark is started on demand via `docker compose run` (see below).

## Check UIs

```text
Kafka UI: http://localhost:8080
Spark UI: http://localhost:4040  (only while Spark streaming is alive)
```

## Create Topic

```bash
docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic coinbase.trades --partitions 1 --replication-factor 1
```

## Generate or Collect Coinbase Data

Synthetic (preferred for reliable demos):

```bash
python producer/generate_coinbase_sample_data.py --num_records 5000
```

Live Coinbase WebSocket (`wss://ws-feed.exchange.coinbase.com`, `matches` channel):

```bash
python producer/coinbase_collector.py --duration_seconds 60
```

## Start Spark Streaming with Delta Sink

```bash
docker compose run --rm --service-ports spark-streaming \
  /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
  streaming/spark_stream_kafka_coinbase_clean_aggregate.py \
  --bootstrap_servers kafka:29092 \
  --topic coinbase.trades \
  --delta_path data/stream/coinbase_aggregates_delta \
  --checkpoint_path data/stream/checkpoints/coinbase_agg \
  --window_seconds 60 --watermark_seconds 120 \
  --sink delta
```

## Replay Records

```bash
python streaming/replay_coinbase_trades_to_kafka.py \
  --input_dir data/raw \
  --topic coinbase.trades \
  --bootstrap_servers localhost:9092 \
  --product_ids BTC-USD,ETH-USD \
  --sleep_mode none --max_events 5000
```

## Verify Kafka

In Kafka UI:

- open `coinbase.trades`
- confirm offsets are increasing
- inspect sample JSON messages

## Verify Spark

In Spark UI:

- check the Jobs and Stages tabs
- confirm the streaming query is active
- inspect executor status

Container logs:

```bash
docker logs crypto-spark-streaming --tail 100
```

## Verify Delta Lake Output

```bash
docker exec crypto-spark-streaming /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  processing/read_coinbase_delta_output.py \
  --delta_path data/stream/coinbase_aggregates_delta
```

You should see schema, row count, sample anomaly rows, table history, and a `versionAsOf` time-travel read once at least two micro-batches have committed.

## Offline Pipeline (cleaning + EDA + rule-based + ML)

```bash
python run_all.py
```

Outputs:

```text
data/cleaned/coinbase_cleaned_trades_*.csv
data/anomalies/latest_coinbase_trade_anomaly_scores.csv
data/anomalies/coinbase_ml_anomaly_results.csv
output/coinbase_*.png
```

## Stop Services

```bash
docker compose down
# add -v to also wipe Kafka volume + Delta files
```
