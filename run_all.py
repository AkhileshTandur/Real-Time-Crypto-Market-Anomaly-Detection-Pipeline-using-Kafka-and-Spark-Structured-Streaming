"""Offline pipeline orchestrator for the Coinbase project.

Steps (in order):
  1. Clean the latest data/raw/coinbase_*.jsonl into data/cleaned/coinbase_cleaned_trades_*.csv
  2. Generate EDA charts under output/coinbase_*.png
  3. Score rule-based anomalies (rolling z-score per product_id)
  4. Score ML anomalies with IsolationForest and compare with the rule-based output

Use --collect-live to call producer/coinbase_collector.py first (real Coinbase
WebSocket). Otherwise this script expects at least one coinbase_*.jsonl file
already present in data/raw/.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--collect-live",
        action="store_true",
        help="Collect fresh Coinbase trades via WebSocket before processing.",
    )
    parser.add_argument(
        "--collect-duration",
        type=int,
        default=60,
        help="Seconds to collect when --collect-live is set (default 60).",
    )
    return parser.parse_args()


def run(script: Path, step: str, extra_args: list[str] | None = None) -> None:
    print(f"\n{'=' * 60}", flush=True)
    print(f"STEP: {step}", flush=True)
    print(f"{'=' * 60}", flush=True)
    cmd = [sys.executable, str(script)]
    if extra_args:
        cmd.extend(extra_args)
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"Failed: {step}")
        sys.exit(1)


def ensure_raw_data(args: argparse.Namespace) -> None:
    if args.collect_live:
        run(
            PROJECT_ROOT / "producer" / "coinbase_collector.py",
            "0. Collect from Coinbase WebSocket",
            extra_args=["--duration_seconds", str(args.collect_duration)],
        )
        return

    if list(RAW_DIR.glob("coinbase_*.jsonl")) or list(RAW_DIR.glob("coinbase_*.json")):
        return

    raise FileNotFoundError(
        f"No coinbase_*.jsonl files found in {RAW_DIR}.\n"
        "Run one of:\n"
        "  python producer/coinbase_collector.py             # live Coinbase WebSocket\n"
        "  python producer/generate_coinbase_sample_data.py  # synthetic Coinbase data\n"
        "Or re-run this script with --collect-live."
    )


if __name__ == "__main__":
    args = parse_args()
    print("Coinbase Crypto Project - Offline Validation Pipeline", flush=True)

    ensure_raw_data(args)

    run(PROJECT_ROOT / "processing" / "coinbase_data_cleaning.py", "1. Clean and preprocess (Coinbase)")
    run(PROJECT_ROOT / "eda" / "eda_analysis.py", "2. EDA and charts (Coinbase)")
    run(PROJECT_ROOT / "processing" / "anomaly_detection.py", "3. Rule-based anomaly scoring (Coinbase)")
    run(
        PROJECT_ROOT / "processing" / "ml_anomaly_detection.py",
        "4. ML anomaly detection - IsolationForest (Coinbase)",
    )
    print("\nDone! Check output/ for charts and data/anomalies/ for anomaly scores.")
