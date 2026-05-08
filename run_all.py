"""Run local trade-level validation: clean data and score statistical/ML anomalies."""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def parse_args():
    parser = argparse.ArgumentParser()
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

    if not list(RAW_DIR.glob("trades_*.json")):
        raise FileNotFoundError(f"No raw trade files found in {RAW_DIR}. Run producer/generate_sample_data.py first.")

    run(PROJECT_ROOT / "processing" / "data_cleaning.py", "1. Clean and preprocess")
    run(PROJECT_ROOT / "processing" / "anomaly_detection.py", "2. Statistical anomaly scoring")
    run(PROJECT_ROOT / "processing" / "ml_anomaly_detection.py", "3. ML anomaly scoring")
    print("\nDone! Check data/anomalies/ for statistical and ML anomaly scores.")
