"""ML-based anomaly detection using Isolation Forest on cleaned crypto trades."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


PROJECT_ROOT = Path(__file__).parent.parent
CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
ANOMALY_DIR = PROJECT_ROOT / "data" / "anomalies"
ANOMALY_DIR.mkdir(parents=True, exist_ok=True)


def load_latest_cleaned() -> pd.DataFrame:
    files = list(CLEANED_DIR.glob("cleaned_trades_*.csv"))
    if not files:
        raise FileNotFoundError(f"No cleaned data found in {CLEANED_DIR}")
    latest = max(files, key=lambda f: f.stat().st_mtime)
    df = pd.read_csv(latest)
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    print(f"Loaded {len(df):,} cleaned trades from {latest.name}")
    return df


def add_symbol_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["symbol", "trade_time"]).copy()
    grouped = df.groupby("symbol", group_keys=False)

    df["price_return"] = grouped["price"].pct_change().replace([float("inf"), float("-inf")], 0).fillna(0)
    df["rolling_trade_count"] = grouped["trade_id"].rolling(window=25, min_periods=1).count().reset_index(level=0, drop=True)
    df["rolling_trade_value_mean"] = (
        grouped["trade_value"].rolling(window=25, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    return df


def score_with_isolation_forest(df: pd.DataFrame, contamination: float = 0.02) -> pd.DataFrame:
    required = {"symbol", "trade_time", "price", "quantity", "trade_value", "trade_id"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df = add_symbol_features(df)
    feature_cols = [
        "price",
        "quantity",
        "trade_value",
        "price_return",
        "rolling_trade_count",
        "rolling_trade_value_mean",
    ]

    scored_parts = []
    for symbol, sub in df.groupby("symbol", sort=False):
        sub = sub.copy()
        if len(sub) < 20:
            sub["ml_anomaly_score"] = 0.0
            sub["is_ml_anomaly"] = False
            scored_parts.append(sub)
            continue

        features = sub[feature_cols].fillna(0.0)
        scaled = StandardScaler().fit_transform(features)
        model = IsolationForest(contamination=contamination, random_state=42)
        labels = model.fit_predict(scaled)

        sub["ml_anomaly_score"] = -model.decision_function(scaled)
        sub["is_ml_anomaly"] = labels == -1
        sub["ml_model"] = "IsolationForest"
        scored_parts.append(sub)

    return pd.concat(scored_parts, ignore_index=True).sort_values(["symbol", "trade_time"])


def main() -> None:
    df = load_latest_cleaned()
    scored = score_with_isolation_forest(df)

    out_file = ANOMALY_DIR / "latest_ml_anomaly_scores.csv"
    scored.to_csv(out_file, index=False)

    anomalies = scored[scored["is_ml_anomaly"]]
    print(f"ML model: IsolationForest")
    print(f"Scored trades: {len(scored):,}")
    print(f"ML anomalies flagged: {len(anomalies):,}")
    print(f"Saved ML anomaly scores to {out_file}")

    if not anomalies.empty:
        cols = ["trade_time", "symbol", "price", "quantity", "trade_value", "ml_anomaly_score"]
        print("\nTop ML anomalies:")
        print(anomalies.sort_values("ml_anomaly_score", ascending=False)[cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
