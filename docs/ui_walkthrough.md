# UI Walkthrough

This project has three UI views.

## 1. Docker Desktop

Use Docker Desktop to see and control running services:

- `crypto-kafka`
- `crypto-kafka-ui`
- optional `crypto-spark-streaming`

Kafka and Kafka UI start with:

```powershell
docker compose up -d
```

If you want Spark to also run as a Docker service, start the `spark` profile:

```powershell
docker compose --profile spark up -d
```

After that, Docker Desktop will show the Spark container and its logs.

## 2. Kafka UI

Open:

```text
http://localhost:8080
```

Use it to inspect:

- Kafka cluster status,
- topics,
- `binance.trades`,
- partitions,
- messages,
- consumer activity.

## 3. Spark UI

Open while Spark is running:

```text
http://localhost:4040
```

Use it to inspect:

- Spark jobs,
- stages,
- SQL queries,
- streaming micro-batches,
- processing duration.

Spark UI only exists while the Spark streaming application is alive. If the Spark container or local Spark process stops, `localhost:4040` stops working.

## Visual Project Demo

Open:

```text
dashboard/index.html
```

This is the project explanation UI. It simulates the end-to-end flow:

```text
Binance -> Raw JSON -> Kafka -> Spark -> Anomaly Output
```
