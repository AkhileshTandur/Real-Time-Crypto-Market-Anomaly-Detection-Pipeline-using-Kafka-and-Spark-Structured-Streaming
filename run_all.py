"""Run the local batch pipeline: clean data, build charts, and score anomalies."""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect-live", action="store_true", help="Collect fresh Binance trades before processing.")
    return parser.parse_args()


def run(script: str, step: str):
    print(f"\n{'='*50}", flush=True)
    print(f"STEP: {step}", flush=True)
    print(f"{'='*50}", flush=True)
    result = subprocess.run([sys.executable, script], cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"Failed: {step}")
        sys.exit(1)


if __name__ == "__main__":
    args = parse_args()
    print("Crypto Project - Offline Validation Pipeline", flush=True)

    if args.collect_live:
        run(PROJECT_ROOT / "producer" / "binance_collector.py", "1. Collect from Binance WebSocket")
    elif not list(RAW_DIR.glob("trades_*.json")):
        raise FileNotFoundError(f"No raw trade files found in {RAW_DIR}. Run with --collect-live first.")

    run(PROJECT_ROOT / "processing" / "data_cleaning.py", "1. Clean and preprocess")
    run(PROJECT_ROOT / "eda" / "eda_analysis.py", "2. EDA and charts")
    run(PROJECT_ROOT / "processing" / "anomaly_detection.py", "3. Batch anomaly scoring")
    print("\nDone! Check output/ for charts and data/anomalies/ for anomaly scores.")
