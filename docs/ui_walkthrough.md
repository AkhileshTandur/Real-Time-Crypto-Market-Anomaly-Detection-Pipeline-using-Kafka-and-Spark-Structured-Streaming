# UI Walkthrough

## Kafka UI

Open:

```text
http://localhost:8080
```

Use it to verify:

- the Kafka cluster is running
- topic `crypto.trades` exists
- live messages are arriving
- offsets increase while the producer runs

## Spark UI

Open:

```text
http://localhost:4040
```

Use it to verify:

- the streaming job is active
- micro-batches are running
- Spark stages and executors are healthy
- the stream reads from Kafka and computes windowed aggregates

## Dashboard

Build dashboard data:

```powershell
py dashboard\build_dashboard_data.py
```

Open:

```text
dashboard/index.html
```

The dashboard is generated from the latest cleaned trade file and anomaly score file. It is not a live WebSocket dashboard; rebuild `dashboard_data.json` after generating new offline outputs.

## End-to-End Flow

```text
Coinbase WebSocket -> Kafka topic crypto.trades -> Spark -> anomaly output
```
