# Real-Time Crypto Market Anomaly Detection Pipeline

Production-style Big Data pipeline for collecting Binance crypto trades, streaming them through Kafka, processing them with Spark Structured Streaming, and detecting unusual market behavior in near real time.

The project is built around the **Velocity** Big Data requirement: crypto trades arrive continuously at high speed and need streaming ingestion, cleaning, aggregation, and anomaly detection instead of only static batch analysis.

## Project Goal

Detect abnormal crypto market activity from live or replayed Binance trade data. The pipeline focuses on:

- rapid price movement inside short time windows,
- unusually large notional trades,
- spikes in trade frequency,
- cleaned historical outputs for EDA and model tuning.

## Dataset Selection and Motivation

**Dataset:** Binance public trade stream for liquid crypto pairs such as `BTCUSDT` and `ETHUSDT`.

**Why this is a Big Data problem:** This project satisfies **Velocity**. Market trades are generated continuously and require near real-time ingestion and processing. A static CSV-only workflow would miss the operational problem: identifying unusual behavior while data is still arriving.

**Connection to goal:** Each trade contains symbol, price, quantity, trade ID, and event time. These fields directly support market surveillance features such as price spread, notional volume, trade count, and anomaly windows.

## Architecture

```text
Binance WebSocket / historical JSONL
        |
        v
Kafka topic: binance.trades
        |
        v
Spark Structured Streaming
  - JSON parsing
  - schema normalization
  - null and invalid-value filtering
  - event-time watermarking
  - 1-minute window aggregation
  - anomaly flagging
        |
        v
data/stream/aggregates_csv
        |
        v
EDA plots + anomaly reports
```

## Repository Structure

```text
producer/
  binance_collector.py                 Live Binance WebSocket collector
  generate_sample_data.py              Offline realistic sample generator
streaming/
  replay_binance_trades_to_kafka.py    Historical/live JSONL replay into Kafka
  spark_stream_kafka_binance_clean_aggregate.py
processing/
  data_cleaning.py                     Batch cleaning for EDA
  anomaly_detection.py                 Offline rolling z-score anomaly scoring
eda/
  eda_analysis.py                      EDA from cleaned trade files
  eda_from_stream_aggregates.py        EDA from Spark streaming aggregates
data/
  raw/                                 Raw Binance JSONL trades
  cleaned/                             Cleaned CSV files
  stream/                              Spark streaming output, ignored by git
output/                                Generated visualizations
docker-compose.yml                     Local Kafka broker
config.yaml                            Symbols and collection settings
```

## Environment Setup

Install Python dependencies:

```bash
py -m pip install -r requirements.txt
```

Start Kafka locally:

```bash
docker compose up -d
```

The Kafka broker listens on `localhost:9092` and auto-creates the `binance.trades` topic when the producer first writes to it.

Kafka UI is available at:

```text
http://localhost:8080
```

Use Kafka UI to inspect the `binance.trades` topic, partitions, messages, and consumer activity.

Spark UI is available while the Spark streaming job is running:

```text
http://localhost:4040
```

Use Spark UI to inspect jobs, stages, micro-batches, SQL queries, and streaming progress.

For a UI-focused walkthrough, see `docs/ui_walkthrough.md`.

## Visual Demo

Open `dashboard/index.html` in a browser to see an interactive simulation of how the real pipeline works without starting Kafka or Spark. The demo shows events flowing through Binance ingestion, raw storage, Kafka, Spark windows, and anomaly output.

## Run the Streaming Pipeline

1. Collect live Binance trades:

```bash
py producer/binance_collector.py
```

2. Replay raw JSONL files into Kafka:

```bash
py streaming/replay_binance_trades_to_kafka.py --input_dir data/raw --topic binance.trades --bootstrap_servers localhost:9092 --symbols BTCUSDT,ETHUSDT --sleep_mode event_time --speed_factor 50
```

3. Run Spark Structured Streaming:

```bash
spark-submit ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ^
  streaming/spark_stream_kafka_binance_clean_aggregate.py ^
  --bootstrap_servers localhost:9092 ^
  --topic binance.trades ^
  --out_path data\stream\aggregates_csv ^
  --checkpoint_path data\stream\checkpoints\agg ^
  --window_seconds 60 ^
  --watermark_seconds 120 ^
  --spread_threshold_pct 1.0 ^
  --large_trade_value 100000 ^
  --high_trade_count 500
```

On this Windows setup, use the helper script if plain `spark-submit` is not found:

```powershell
.\scripts\start_spark_streaming.ps1
```

4. Generate streaming EDA charts:

```bash
py eda/eda_from_stream_aggregates.py --input_dir data/stream/aggregates_csv --output_dir output --symbols BTCUSDT,ETHUSDT
```

## Batch EDA and Offline Validation

For a quick offline run:

```bash
py run_all.py
```

Run batch anomaly scoring on the latest cleaned file:

```bash
py processing/anomaly_detection.py
```

Outputs:

- `output/1_price_over_time.png`
- `output/2_volume_by_symbol.png`
- `output/3_trade_count_by_hour.png`
- `output/4_trade_value_distribution.png`
- `output/5_price_distribution_by_symbol.png`
- `output/6_trade_count_by_symbol.png`
- `output/stream_4_anomaly_windows.png` after streaming aggregation
- `data/anomalies/latest_trade_anomaly_scores.csv`

## Data Processing and Cleaning

The cleaning logic handles:

- schema normalization from Binance fields (`s`, `t`, `p`, `q`, `T`) into readable names,
- numeric conversion for price, quantity, and trade ID,
- timestamp conversion from milliseconds to event time,
- null removal for critical fields,
- invalid price and quantity filtering,
- duplicate removal by `trade_id`,
- derived `trade_value = price * quantity`.

The Spark streaming job applies equivalent cleaning before aggregation, so the real-time path and offline EDA path are consistent.

## Anomaly Detection Logic

The real-time Spark job flags anomalous windows when at least one condition is true:

- `price_spread_pct` exceeds the configured threshold,
- `max_trade_value` exceeds the large-trade threshold,
- `trade_count` exceeds the traffic-spike threshold.

The offline detector adds rolling z-score scoring per symbol for price and trade value. This provides a reproducible validation layer and a path toward a stronger ML model.

## EDA Insights to Present

Use only the strongest charts in a 15-minute presentation:

- price over time shows short-term movement and volatility,
- volume by symbol shows liquidity differences,
- trade count by hour shows market activity patterns,
- trade value distribution highlights skew and large-trade behavior,
- anomaly windows connect the analysis directly to the project goal.

## Requirements Coverage

| PDF requirement | Project coverage |
| --- | --- |
| Dataset meets at least one Big Data V | Meets **Velocity** through live/replayed trade streams |
| Real-time or simulated streaming pipeline | Kafka producer/replay plus Spark Structured Streaming consumer |
| Dataset description and motivation | Documented above with Binance trade stream rationale |
| Preprocessing and cleaning | Batch and streaming cleaning implemented |
| Big Data tools | Kafka and Spark Structured Streaming |
| Visualizations | Batch EDA and streaming aggregate charts |
| Insights and future direction | EDA insights plus anomaly scoring and ML path |
| Pipeline design and workflow | Architecture section explains input to final output |
| Environment setup and integration | Docker Kafka, Python, and Spark commands |
| Contribution evidence | Add GitHub commit history or member contribution slide in the final presentation |

## Challenges and Next Steps

Current practical challenges:

- Binance access can be blocked in some networks or regions, so replay mode is included.
- Spark/Kafka local setup depends on Java, Docker, and Spark installation.
- The committed sample data is intentionally small; real runs should collect or replay longer trade histories.

Planned improvements:

- persist streaming results to Parquet or Delta Lake for efficient downstream analytics,
- add alert delivery through Slack/email/webhook,
- tune thresholds per symbol using longer historical baselines,
- train an unsupervised anomaly model such as Isolation Forest or robust rolling quantiles,
- deploy the pipeline on a managed cluster or containerized environment.
