"""Build dashboard data from Spark streaming outputs, with batch fallback."""

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


def read_csv_files(files: list[Path]) -> pd.DataFrame:
    frames = []
    for file in files:
        try:
            frames.append(pd.read_csv(file))
        except pd.errors.EmptyDataError:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_stream_aggregates() -> pd.DataFrame:
    files = sorted((ROOT / "data" / "stream" / "aggregates_csv").glob("**/*.csv"))
    df = read_csv_files(files)
    if df.empty:
        return df
    df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce")
    df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce")
    if "is_anomaly" in df.columns:
        df["is_anomaly"] = df["is_anomaly"].astype(str).str.lower().isin(["true", "1"])
    return df


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


def load_latest_ml_anomalies() -> pd.DataFrame:
    anomaly_file = ROOT / "data" / "anomalies" / "latest_ml_anomaly_scores.csv"
    if not anomaly_file.exists():
        return pd.DataFrame()
    df = pd.read_csv(anomaly_file)
    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    return df


def build_stream_payload(df: pd.DataFrame, ml_anomalies: pd.DataFrame) -> dict:
    total_records = int(df["trade_count"].sum()) if "trade_count" in df.columns else len(df)
    avg_trade_value = 0.0
    if {"notional_volume", "trade_count"}.issubset(df.columns) and df["trade_count"].sum() > 0:
        avg_trade_value = float(df["notional_volume"].sum() / df["trade_count"].sum())

    summary = {
        "source": "spark_stream",
        "total_records": total_records,
        "symbols": sorted(df["symbol"].dropna().astype(str).unique().tolist()),
        "time_start": df["window_start"].min().isoformat() if "window_start" in df else None,
        "time_end": df["window_end"].max().isoformat() if "window_end" in df else None,
        "avg_trade_value": avg_trade_value,
        "anomaly_count": int(df["is_anomaly"].sum()) if "is_anomaly" in df.columns else 0,
        "ml_anomaly_count": int(ml_anomalies["is_ml_anomaly"].astype(bool).sum())
        if not ml_anomalies.empty and "is_ml_anomaly" in ml_anomalies.columns
        else 0,
    }

    price_series = [
        {
            "symbol": str(row.symbol),
            "time": row.window_start.isoformat() if pd.notna(row.window_start) else None,
            "price": float(row.avg_price),
            "quantity": float(row.volume),
            "trade_value": float(row.notional_volume),
        }
        for row in df.sort_values("window_start").itertuples(index=False)
    ]

    aggregates = []
    for symbol, sub in df.groupby("symbol"):
        trade_count = sub["trade_count"].sum() if "trade_count" in sub else len(sub)
        avg_price = (
            (sub["avg_price"] * sub["trade_count"]).sum() / trade_count
            if "trade_count" in sub and trade_count > 0
            else sub["avg_price"].mean()
        )
        aggregates.append(
            {
                "symbol": str(symbol),
                "avg_price": float(avg_price),
                "volume": float(sub["volume"].sum()) if "volume" in sub else 0,
                "trade_count": int(trade_count),
                "max_trade_value": float(sub["max_trade_value"].max()) if "max_trade_value" in sub else 0,
            }
        )

    anomalies = []
    if "is_anomaly" in df.columns:
        flagged = df[df["is_anomaly"]].sort_values(["window_start", "symbol"], ascending=[False, True]).head(50)
        for row in flagged.itertuples(index=False):
            anomalies.append(
                {
                    "detected_at": row.window_start.isoformat() if pd.notna(row.window_start) else None,
                    "symbol": str(row.symbol),
                    "price": float(row.avg_price),
                    "quantity": float(row.volume),
                    "trade_value": float(row.notional_volume),
                    "price_zscore": 0.0,
                    "trade_value_zscore": 0.0,
                    "reason": row.anomaly_reason if getattr(row, "anomaly_reason", "") else "stream_rule",
                }
            )

    return {
        "summary": summary,
        "price_series": price_series,
        "aggregates": sorted(aggregates, key=lambda row: row["symbol"]),
        "anomalies": anomalies,
        "ml_anomalies": build_ml_anomaly_history(ml_anomalies),
    }


def build_batch_payload(cleaned: pd.DataFrame, anomalies: pd.DataFrame, ml_anomalies: pd.DataFrame) -> dict:
    if cleaned.empty:
        summary = {
            "source": "empty",
            "total_records": 0,
            "symbols": [],
            "time_start": None,
            "time_end": None,
            "avg_trade_value": 0,
            "anomaly_count": 0,
            "ml_anomaly_count": 0,
        }
    else:
        summary = {
            "source": "cleaned_batch",
            "total_records": int(len(cleaned)),
            "symbols": sorted(cleaned["symbol"].dropna().astype(str).unique().tolist()),
            "time_start": cleaned["trade_time"].min().isoformat() if "trade_time" in cleaned else None,
            "time_end": cleaned["trade_time"].max().isoformat() if "trade_time" in cleaned else None,
            "avg_trade_value": float(cleaned["trade_value"].mean()) if "trade_value" in cleaned else 0,
            "anomaly_count": int(anomalies["is_anomaly"].astype(bool).sum())
            if not anomalies.empty and "is_anomaly" in anomalies.columns
            else 0,
            "ml_anomaly_count": int(ml_anomalies["is_ml_anomaly"].astype(bool).sum())
            if not ml_anomalies.empty and "is_ml_anomaly" in ml_anomalies.columns
            else 0,
        }

    return {
        "summary": summary,
        "price_series": build_batch_price_series(cleaned),
        "aggregates": build_batch_aggregates(cleaned),
        "anomalies": build_stat_anomaly_history(anomalies),
        "ml_anomalies": build_ml_anomaly_history(ml_anomalies),
    }


def build_batch_price_series(df: pd.DataFrame) -> list[dict]:
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


def build_batch_aggregates(df: pd.DataFrame) -> list[dict]:
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


def build_stat_anomaly_history(anomalies: pd.DataFrame) -> list[dict]:
    if anomalies.empty or "is_anomaly" not in anomalies.columns:
        return []

    flagged = anomalies[anomalies["is_anomaly"].astype(bool)].copy()
    if flagged.empty:
        return []

    flagged["score"] = flagged[["price_zscore", "trade_value_zscore"]].abs().max(axis=1)
    rows = []
    for row in flagged.sort_values("score", ascending=False).head(50).itertuples(index=False):
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


def build_ml_anomaly_history(ml_anomalies: pd.DataFrame) -> list[dict]:
    if ml_anomalies.empty or "is_ml_anomaly" not in ml_anomalies.columns:
        return []

    flagged = ml_anomalies[ml_anomalies["is_ml_anomaly"].astype(bool)].copy()
    if flagged.empty:
        return []

    rows = []
    for row in flagged.sort_values("ml_anomaly_score", ascending=False).head(50).itertuples(index=False):
        rows.append(
            {
                "detected_at": row.trade_time.isoformat() if pd.notna(row.trade_time) else None,
                "symbol": str(row.symbol),
                "price": float(row.price),
                "quantity": float(row.quantity),
                "trade_value": float(row.trade_value),
                "ml_anomaly_score": float(row.ml_anomaly_score),
                "model": getattr(row, "ml_model", "IsolationForest"),
                "reason": "isolation_forest",
            }
        )
    return rows


def main() -> None:
    stream_aggregates = load_stream_aggregates()
    cleaned = load_latest_cleaned()
    anomalies = load_latest_anomalies()
    ml_anomalies = load_latest_ml_anomalies()

    if not stream_aggregates.empty:
        payload = build_stream_payload(stream_aggregates, ml_anomalies)
    else:
        payload = build_batch_payload(cleaned, anomalies, ml_anomalies)

    out_file = DATA_DIR / "dashboard_data.json"
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {out_file}")


if __name__ == "__main__":
    main()
