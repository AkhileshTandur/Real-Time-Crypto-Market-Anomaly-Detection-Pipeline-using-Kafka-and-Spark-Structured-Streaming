# Mid-Presentation Checklist

Use this file to verify that the presentation matches the provided PDF rubric.

## Required Storyline

1. Project title and goal: real-time crypto market anomaly detection.
2. Dataset: Binance public trade stream for BTCUSDT and ETHUSDT.
3. Big Data V: Velocity, because trades arrive continuously and require streaming analysis.
4. System architecture: Binance or JSONL replay -> Kafka -> Spark Structured Streaming -> cleaned aggregates -> anomaly windows -> EDA outputs.
5. Storage and distribution: raw JSONL in `data/raw`, Kafka topic `binance.trades`, Spark checkpoints and aggregate files in `data/stream`.
6. Data cleaning: null removal, invalid price/quantity filtering, timestamp conversion, duplicate trade ID removal, derived trade value.
7. Processing logic: event-time windows, watermarking, per-symbol aggregation, anomaly threshold rules.
8. EDA results: price over time, volume by symbol, trade count pattern, trade value distribution, anomaly windows.
9. Challenges: Binance/network access, local Spark/Kafka setup, need longer live collection for stronger baselines.
10. Next steps: Parquet/Delta storage, alerting, model-based anomaly detection, deployment.
11. Final slide: GitHub repository link and member contribution evidence.

## Evidence to Capture Before Submission

- Screenshot of `docker compose ps` showing Kafka running.
- Terminal output from the Kafka replay script showing events sent.
- Terminal output from `spark-submit` showing the streaming query started.
- File listing of `data/stream/aggregates_csv` after Spark writes output.
- Generated charts in `output/`.
- GitHub commit history or contribution summary for each member.

## Recommended 15-Minute Slide Flow

1. Problem and goal.
2. Dataset and Big Data Velocity justification.
3. Architecture diagram.
4. Data ingestion and Kafka topic.
5. Spark streaming cleaning and aggregation.
6. Anomaly detection rules.
7. EDA visualizations and insights.
8. Challenges and engineering decisions.
9. Final-project next steps.
10. Contributions and repository link.
