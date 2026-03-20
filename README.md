# Real-Time Crypto Market Anomaly Detection
## Mid-Presentation: Data Collection, Cleaning & EDA

### Quick Start

1. **Install dependencies**
   ```
   pip install -r requirements.txt
   ```

2. **Step 1: Collect data**
   - **Live (Binance WebSocket):** `python producer/binance_collector.py`
   Saves to `data/raw/`.

3. **Step 2: Clean and preprocess**
   ```
   python processing/data_cleaning.py
   ```
   Removes nulls, invalid values, duplicates. Saves to `data/cleaned/`.

4. **Step 3: Run EDA and generate charts**
   ```
   python eda/eda_analysis.py
   ```
   Creates 6 charts in `output/` for your presentation.

### Project Structure

```
crypto_project/
├── producer/
│   └── binance_collector.py   # WebSocket data collection
├── processing/
│   └── data_cleaning.py       # Preprocessing & cleaning
├── eda/
│   └── eda_analysis.py        # EDA visualizations
├── data/
│   ├── raw/                   # Raw JSON from Binance
│   └── cleaned/               # Cleaned CSV
├── output/                    # EDA charts (PNG)
├── config.yaml
├── requirements.txt
└── README.md
```

### Output Charts (for EDA slides)

1. `1_price_over_time.png` - Price over time by symbol
2. `2_volume_by_symbol.png` - Total volume by symbol
3. `3_trade_count_by_hour.png` - Trade count by hour
4. `4_trade_value_distribution.png` - Trade value histogram
5. `5_price_distribution_by_symbol.png` - Price box plot by symbol
6. `6_trade_count_by_symbol.png` - Trade count by symbol

### Cleaning Steps (for presentation)

1. Null removal in price, quantity, symbol
2. Invalid value filtering (price/quantity > 0, reasonable range)
3. Duplicate removal by trade_id
4. Type conversion (numeric, timestamp)
5. Derived field: trade_value = price × quantity

---

## Kafka + Spark (Mid-Presentation Velocity Requirement)

This section replays **historical Binance trade files** into Kafka, then uses **Spark Structured Streaming** to clean and compute 1-minute window aggregates.

### 1. Replay historical trades into Kafka
Assumption: you have a folder with Binance-style `*.json` / `*.jsonl` where each line is a trade JSON object.

```bash
python streaming/replay_binance_trades_to_kafka.py --input_dir "PATH_TO_HISTORICAL_TRADES" --topic binance.trades --bootstrap_servers localhost:9092 --symbols BTCUSDT,ETHUSDT --sleep_mode none
```

### 2. Spark Structured Streaming: consume + clean + aggregate
```bash
spark-submit streaming/spark_stream_kafka_binance_clean_aggregate.py ^
  --bootstrap_servers localhost:9092 ^
  --topic binance.trades ^
  --out_path "data\\stream\\aggregates_csv" ^
  --checkpoint_path "data\\stream\\checkpoints\\agg" ^
  --window_seconds 60 ^
  --watermark_seconds 120
```

### 3. EDA plots from streaming aggregates
```bash
python eda/eda_from_stream_aggregates.py --input_dir "data\\stream\\aggregates_csv" --output_dir output --symbols BTCUSDT,ETHUSDT
```
