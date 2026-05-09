"""Isolation Forest anomaly detection on cleaned Coinbase trade data.

Plug-in to the existing batch pipeline. Reads the latest
``data/cleaned/coinbase_cleaned_trades_*.csv`` produced by
``processing/coinbase_data_cleaning.py``, fits a scikit-learn IsolationForest
on a small feature set, and writes:

  - data/anomalies/coinbase_ml_anomaly_results.csv
  - output/coinbase_ml_anomaly_scatter.png
  - output/coinbase_ml_feature_importance.png
  - output/coinbase_ml_comparison.png
  - output/coinbase_ml_anomaly_scores_distribution.png

The comparison chart cross-references the ML labels with the rule-based
output (``data/anomalies/latest_coinbase_trade_anomaly_scores.csv``) so the
demo can show where the methods agree and disagree per Coinbase product.

Note on feature importance: scikit-learn's IsolationForest does not expose
native feature importances, so we plot ``|corr(feature, -anomaly_score)|``
as a transparent contribution proxy. That caveat is printed in the chart
and the summary.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


PROJECT_ROOT = Path(__file__).parent.parent
CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
ANOMALY_DIR = PROJECT_ROOT / "data" / "anomalies"
OUTPUT_DIR = PROJECT_ROOT / "output"
ANOMALY_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CONTAMINATION = 0.05
N_ESTIMATORS = 100
RANDOM_STATE = 42
ROLL_WINDOW = 100
ROLL_MIN = 20
FEATURES = ["price", "quantity", "trade_value", "price_zscore", "trade_value_zscore"]


def load_latest_cleaned() -> pd.DataFrame:
    files = list(CLEANED_DIR.glob("coinbase_cleaned_trades_*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No cleaned data found in {CLEANED_DIR}. "
            "Run processing/coinbase_data_cleaning.py first."
        )
    latest = max(files, key=lambda f: f.stat().st_mtime)
    df = pd.read_csv(latest)
    df["trade_time"] = pd.to_datetime(df["trade_time"], utc=True, errors="coerce")
    print(f"Loaded {len(df):,} cleaned Coinbase trades from {latest.name}")
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add the same per-product rolling z-scores used by the rule-based scorer.

    Aligning the feature definitions makes the rule vs ML comparison
    apples-to-apples: the methods see the same view of the data and only the
    decision rule differs.
    """
    df = df.sort_values(["product_id", "trade_time"]).copy()
    if "trade_value" not in df.columns:
        df["trade_value"] = df["price"] * df["quantity"]

    groups = df.groupby("product_id", group_keys=False)
    price_mean = (
        groups["price"].rolling(ROLL_WINDOW, min_periods=ROLL_MIN).mean().reset_index(level=0, drop=True)
    )
    price_std = (
        groups["price"].rolling(ROLL_WINDOW, min_periods=ROLL_MIN).std().reset_index(level=0, drop=True)
    )
    value_mean = (
        groups["trade_value"].rolling(ROLL_WINDOW, min_periods=ROLL_MIN).mean().reset_index(level=0, drop=True)
    )
    value_std = (
        groups["trade_value"].rolling(ROLL_WINDOW, min_periods=ROLL_MIN).std().reset_index(level=0, drop=True)
    )

    df["price_zscore"] = (
        ((df["price"] - price_mean) / price_std).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    )
    df["trade_value_zscore"] = (
        ((df["trade_value"] - value_mean) / value_std).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    )
    return df


def train_isolation_forest(df: pd.DataFrame) -> pd.DataFrame:
    """Fit IsolationForest on standardized features and attach predictions."""
    x = df[FEATURES].to_numpy()
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)

    model = IsolationForest(
        contamination=CONTAMINATION,
        n_estimators=N_ESTIMATORS,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(x_scaled)

    df = df.copy()
    df["anomaly_score"] = model.decision_function(x_scaled)
    df["ml_is_anomaly"] = model.predict(x_scaled) == -1
    return df


def feature_contribution(df: pd.DataFrame) -> pd.Series:
    """|corr(feature, -anomaly_score)| as a contribution proxy.

    sklearn IsolationForest has no native feature importances. Lower
    decision_function values are more anomalous, so correlating features
    with the negated score gives an interpretable "this feature drives
    the model's anomaly judgment" reading without a separate surrogate.
    """
    target = -df["anomaly_score"]
    contrib = {}
    for feat in FEATURES:
        col = df[feat]
        if col.std() == 0:
            contrib[feat] = 0.0
        else:
            contrib[feat] = float(abs(np.corrcoef(col, target)[0, 1]))
    return pd.Series(contrib).sort_values(ascending=False)


def load_rule_based_results() -> pd.DataFrame | None:
    rb_path = ANOMALY_DIR / "latest_coinbase_trade_anomaly_scores.csv"
    if not rb_path.exists():
        print(f"Rule-based results not found at {rb_path}; comparison chart will fall back.")
        return None
    rb = pd.read_csv(rb_path)
    return rb[["product_id", "trade_id", "is_anomaly"]].rename(columns={"is_anomaly": "rule_is_anomaly"})


def plot_scatter(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    normal = df[~df["ml_is_anomaly"]]
    anomaly = df[df["ml_is_anomaly"]]
    ax.scatter(normal["price"], normal["trade_value"], s=8, alpha=0.4, label="Normal", color="#2563eb")
    ax.scatter(
        anomaly["price"], anomaly["trade_value"], s=18, alpha=0.85, label="Anomaly", color="#c02626"
    )
    ax.set_xlabel("Price (USD)")
    ax.set_ylabel("Trade Value (USD)")
    ax.set_title("Coinbase Isolation Forest Anomalies: Price vs Trade Value")
    ax.legend()
    plt.tight_layout()
    out = OUTPUT_DIR / "coinbase_ml_anomaly_scatter.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out.name}")


def plot_feature_importance(contrib: pd.Series) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    contrib.plot(kind="bar", ax=ax, color="#2563eb", edgecolor="white")
    ax.set_ylabel("|corr(feature, -anomaly_score)|  (proxy)")
    ax.set_title("Coinbase Feature Contribution to IsolationForest Score (correlation proxy)")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    out = OUTPUT_DIR / "coinbase_ml_feature_importance.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out.name}")


def plot_comparison(df: pd.DataFrame, rule: pd.DataFrame | None) -> dict:
    counts = {"rule_only": 0, "both": 0, "ml_only": 0, "neither": 0}
    if rule is None:
        fig, ax = plt.subplots(figsize=(7, 5))
        ml_anom = int(df["ml_is_anomaly"].sum())
        ml_norm = len(df) - ml_anom
        ax.bar(["Normal", "Anomaly"], [ml_norm, ml_anom], color=["#2563eb", "#c02626"])
        ax.set_title("Coinbase IsolationForest Anomaly Counts (rule-based not available)")
        ax.set_ylabel("Records")
        plt.tight_layout()
        out = OUTPUT_DIR / "coinbase_ml_comparison.png"
        plt.savefig(out, dpi=150)
        plt.close()
        print(f"Saved {out.name}")
        return counts

    merged = df.merge(rule, on=["product_id", "trade_id"], how="left")
    merged["rule_is_anomaly"] = merged["rule_is_anomaly"].fillna(False).astype(bool)
    counts["both"] = int((merged["ml_is_anomaly"] & merged["rule_is_anomaly"]).sum())
    counts["ml_only"] = int((merged["ml_is_anomaly"] & ~merged["rule_is_anomaly"]).sum())
    counts["rule_only"] = int((~merged["ml_is_anomaly"] & merged["rule_is_anomaly"]).sum())
    counts["neither"] = int((~merged["ml_is_anomaly"] & ~merged["rule_is_anomaly"]).sum())

    fig, ax = plt.subplots(figsize=(8, 5))
    labels = ["Rule only", "Both", "ML only"]
    values = [counts["rule_only"], counts["both"], counts["ml_only"]]
    bars = ax.bar(labels, values, color=["#f59e0b", "#12805c", "#2563eb"], edgecolor="white")
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(), str(v), ha="center", va="bottom")
    ax.set_ylabel("Records")
    ax.set_title("Coinbase: Rule-based vs IsolationForest Anomaly Overlap")
    plt.tight_layout()
    out = OUTPUT_DIR / "coinbase_ml_comparison.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out.name}")
    return counts


def plot_score_distribution(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(df["anomaly_score"], bins=60, color="#2563eb", edgecolor="white")
    if df["ml_is_anomaly"].any():
        threshold = df.loc[df["ml_is_anomaly"], "anomaly_score"].max()
        ax.axvline(threshold, color="#c02626", linestyle="--", label=f"Anomaly cutoff ≈ {threshold:.3f}")
        ax.legend()
    ax.set_xlabel("Anomaly Score (decision_function; lower = more anomalous)")
    ax.set_ylabel("Frequency")
    ax.set_title("Coinbase IsolationForest Anomaly Score Distribution")
    plt.tight_layout()
    out = OUTPUT_DIR / "coinbase_ml_anomaly_scores_distribution.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"Saved {out.name}")


def main() -> None:
    df = load_latest_cleaned()
    df = engineer_features(df)
    df = train_isolation_forest(df)

    out_csv = ANOMALY_DIR / "coinbase_ml_anomaly_results.csv"
    df.to_csv(out_csv, index=False)
    print(f"Saved ML results to {out_csv}")

    plot_scatter(df)
    contrib = feature_contribution(df)
    plot_feature_importance(contrib)
    rule = load_rule_based_results()
    counts = plot_comparison(df, rule)
    plot_score_distribution(df)

    total = len(df)
    n_anom = int(df["ml_is_anomaly"].sum())
    rate = n_anom / total if total else 0.0
    print("\n--- COINBASE ML ANOMALY DETECTION SUMMARY ---")
    print(f"Total records:              {total:,}")
    print(f"Anomalies (ML):             {n_anom:,}")
    print(f"Effective anomaly rate:     {rate:.4f}")
    print(f"Configured contamination:   {CONTAMINATION:.4f}")
    print("\nFeature contribution (|corr| with -anomaly_score; correlation proxy):")
    print(contrib.to_string())
    if rule is not None:
        print("\nRule-based vs ML overlap:")
        print(f"  Both flagged:      {counts['both']:,}")
        print(f"  Rule-only flagged: {counts['rule_only']:,}")
        print(f"  ML-only flagged:   {counts['ml_only']:,}")
        print(f"  Neither flagged:   {counts['neither']:,}")


if __name__ == "__main__":
    main()
