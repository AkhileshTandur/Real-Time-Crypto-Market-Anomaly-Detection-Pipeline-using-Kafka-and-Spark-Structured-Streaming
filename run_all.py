"""
Run full pipeline: Collect -> Clean -> EDA
Use this for a quick demo before the presentation.
"""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent


def run(script: str, step: str):
    print(f"\n{'='*50}")
    print(f"STEP: {step}")
    print(f"{'='*50}")
    result = subprocess.run([sys.executable, script], cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"Failed: {step}")
        sys.exit(1)


if __name__ == "__main__":
    print("Crypto Project - Full Pipeline")
    run(PROJECT_ROOT / "producer" / "binance_collector.py", "1. Collect from Binance WebSocket")
    run(PROJECT_ROOT / "processing" / "data_cleaning.py", "2. Clean and preprocess")
    run(PROJECT_ROOT / "eda" / "eda_analysis.py", "3. EDA and charts")
    print("\nDone! Check the output/ folder for charts.")
