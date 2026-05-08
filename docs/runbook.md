# Runbook

## Start Services

```powershell
docker compose up -d
```

Expected containers:

```text
crypto-kafka
crypto-kafka-ui
crypto-spark-streaming
```

## Check UIs

```text
Kafka UI: http://localhost:8080
Spark UI: http://localhost:4040
```

Kafka UI should show the Kafka cluster and the `crypto.trades` topic after the topic has been created.

Spark UI is available only while the Spark streaming application is running.

## Create Topic

The compose file creates the topic automatically. To create it manually:

```powershell
docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic crypto.trades --partitions 1 --replication-factor 1
```

## Send Live Data

```powershell
py producer\coinbase_live_to_kafka.py --topic crypto.trades --bootstrap_servers localhost:9092 --products BTC-USD,ETH-USD
```

For a timed demo:

```powershell
py producer\coinbase_live_to_kafka.py --topic crypto.trades --bootstrap_servers localhost:9092 --products BTC-USD,ETH-USD --duration_seconds 120
```

## Verify Kafka

In Kafka UI:

- open `crypto.trades`
- confirm offsets are increasing
- inspect sample JSON messages

## Verify Spark

In Spark UI:

- check the Jobs and Stages tabs
- confirm the streaming query is active
- inspect executor status

Container logs are also useful:

```powershell
docker logs crypto-spark-streaming --tail 100
```

Spark writes streaming window aggregates to:

```text
data/stream/aggregates_csv/
```

## Streaming EDA Charts

Generate charts from Spark streaming output:

```powershell
py eda\eda_from_stream_aggregates.py
```

Outputs are written under:

```text
output/stream_*.png
```

## Stop Services

```powershell
docker compose down
```

## Offline ML Validation

```powershell
py run_all.py
```

Use this only for trade-level z-score and Isolation Forest scoring from files under `data/raw`.

Outputs are written under:

```text
output/
data/anomalies/
```

The anomaly folder contains both statistical and ML model outputs:

```text
data/anomalies/latest_trade_anomaly_scores.csv
data/anomalies/latest_ml_anomaly_scores.csv
```

## Dashboard Data

```powershell
py dashboard\build_dashboard_data.py
```

Open:

```text
dashboard/index.html
```

The dashboard reads `dashboard/data/dashboard_data.json`. It uses Spark streaming aggregate CSV files when they exist, and falls back to the latest cleaned trade file only when no stream output is available.
