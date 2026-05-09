# UI Walkthrough

This project has four places to look during a demo.

## 1. Docker Desktop

Use Docker Desktop to see and control running services:

- `crypto-kafka`
- `crypto-kafka-ui`
- `crypto-spark-streaming` (only while the streaming `docker compose run` is alive)

Start Kafka and Kafka UI with:

```bash
docker compose up -d kafka kafka-ui
```

The Spark streaming container is launched separately (see `docs/runbook.md`) so it can be stopped/restarted without disturbing Kafka.

## 2. Kafka UI

Open:

```text
http://localhost:8080
```

Use it to inspect:

- Kafka cluster status
- topics, especially `coinbase.trades`
- partitions
- messages (each record is a normalized Coinbase trade JSON)
- consumer activity from Spark

## 3. Spark UI

Open while the Spark streaming application is running:

```text
http://localhost:4040
```

Use it to inspect:

- Spark jobs and stages
- the Structured Streaming tab (input rate, processing time, batch durations)
- SQL query plans for the windowed aggregation

The Spark UI exists only while the streaming application is alive. If the Spark container or local Spark process stops, `localhost:4040` stops working.

## 4. Visual Project Demo

Open:

```text
dashboard/index.html
```

This is the project explanation UI. It simulates the end-to-end flow:

```text
Coinbase WebSocket -> Raw JSONL -> Kafka -> Spark -> Delta + Anomaly Output
```

It is a teaching/explainer UI; live data flows are visible in Kafka UI, Spark UI, and the Delta transaction log (`data/stream/coinbase_aggregates_delta/_delta_log/`).
