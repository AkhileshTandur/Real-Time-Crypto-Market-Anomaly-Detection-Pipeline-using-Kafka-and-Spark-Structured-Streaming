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

Kafka UI should show the Kafka cluster and the `binance.trades` topic after the topic has been created.

Spark UI is available only while the Spark streaming application is running.

## Create Topic

```powershell
docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic binance.trades --partitions 1 --replication-factor 1
```

## Send Data

```powershell
py streaming\replay_binance_trades_to_kafka.py --input_dir data\raw --topic binance.trades --bootstrap_servers localhost:9092 --symbols BTCUSDT,ETHUSDT --sleep_mode none --max_events 2000
```

## Verify Kafka

In Kafka UI:

- open `binance.trades`
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

## Stop Services

```powershell
docker compose down
```

## Offline Pipeline

```powershell
py run_all.py
```

Outputs are written under:

```text
output/
data/anomalies/
```
