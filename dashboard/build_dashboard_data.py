"""Build dashboard data from the latest project outputs."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "dashboard" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def latest_file(directory: Path, pattern: str) -> Path | None:
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def load_latest_cleaned() -> pd.DataFrame:
    cleaned_file = latest_file(ROOT / "data" / "cleaned", "cleaned_trades_*.csv")
    if cleaned_file is None:
        return pd.DataFrame()
    df = pd.read_csv(cleaned_file)
    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    return df


def load_latest_anomalies() -> pd.DataFrame:
    anomaly_file = ROOT / "data" / "anomalies" / "latest_trade_anomaly_scores.csv"
    if not anomaly_file.exists():
        return pd.DataFrame()
    df = pd.read_csv(anomaly_file)
    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    return df


def build_summary(df: pd.DataFrame, anomalies: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "total_records": 0,
            "symbols": [],
            "time_start": None,
            "time_end": None,
            "avg_trade_value": 0,
            "anomaly_count": 0,
        }

    anomaly_count = 0
    if not anomalies.empty and "is_anomaly" in anomalies.columns:
        anomaly_count = int(anomalies["is_anomaly"].astype(bool).sum())

    return {
        "total_records": int(len(df)),
        "symbols": sorted(df["symbol"].dropna().astype(str).unique().tolist()),
        "time_start": df["trade_time"].min().isoformat() if "trade_time" in df else None,
        "time_end": df["trade_time"].max().isoformat() if "trade_time" in df else None,
        "avg_trade_value": float(df["trade_value"].mean()) if "trade_value" in df else 0,
        "anomaly_count": anomaly_count,
    }


def build_price_series(df: pd.DataFrame) -> list[dict]:
    if df.empty or "trade_time" not in df:
        return []

    points = []
    for symbol, sub in df.sort_values("trade_time").groupby("symbol"):
        if len(sub) > 600:
            sub = sub.iloc[:: max(len(sub) // 600, 1)]
        for row in sub.itertuples(index=False):
            points.append(
                {
                    "symbol": str(row.symbol),
                    "time": row.trade_time.isoformat() if pd.notna(row.trade_time) else None,
                    "price": float(row.price),
                    "quantity": float(row.quantity),
                    "trade_value": float(row.trade_value),
                }
            )
    return points


def build_aggregates(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []

    rows = []
    for symbol, sub in df.groupby("symbol"):
        rows.append(
            {
                "symbol": str(symbol),
                "avg_price": float(sub["price"].mean()),
                "volume": float(sub["quantity"].sum()),
                "trade_count": int(len(sub)),
                "max_trade_value": float(sub["trade_value"].max()) if "trade_value" in sub else 0,
            }
        )
    return sorted(rows, key=lambda row: row["symbol"])


def build_anomaly_history(anomalies: pd.DataFrame) -> list[dict]:
    if anomalies.empty or "is_anomaly" not in anomalies.columns:
        return []

    flagged = anomalies[anomalies["is_anomaly"].astype(bool)].copy()
    if flagged.empty:
        return []

    flagged["score"] = flagged[["price_zscore", "trade_value_zscore"]].abs().max(axis=1)
    flagged = flagged.sort_values("score", ascending=False).head(50)

    rows = []
    for row in flagged.itertuples(index=False):
        reason = []
        if abs(float(row.price_zscore)) >= 3:
            reason.append("price_zscore")
        if abs(float(row.trade_value_zscore)) >= 3:
            reason.append("trade_value_zscore")
        rows.append(
            {
                "detected_at": row.trade_time.isoformat() if pd.notna(row.trade_time) else None,
                "symbol": str(row.symbol),
                "price": float(row.price),
                "quantity": float(row.quantity),
                "trade_value": float(row.trade_value),
                "price_zscore": float(row.price_zscore),
                "trade_value_zscore": float(row.trade_value_zscore),
                "reason": ", ".join(reason) if reason else "score_threshold",
            }
        )
    return rows


def main() -> None:
    cleaned = load_latest_cleaned()
    anomalies = load_latest_anomalies()
    payload = {
        "summary": build_summary(cleaned, anomalies),
        "price_series": build_price_series(cleaned),
        "aggregates": build_aggregates(cleaned),
        "anomalies": build_anomaly_history(anomalies),
    }

    out_file = DATA_DIR / "dashboard_data.json"
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {out_file}")


if __name__ == "__main__":
    main()
