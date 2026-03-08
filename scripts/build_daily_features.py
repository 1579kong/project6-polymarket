from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SNAPSHOT_CSV = DATA_DIR / "market_snapshots.csv"
DAILY_CSV = DATA_DIR / "daily_features.csv"


def main() -> None:
    if not SNAPSHOT_CSV.exists():
        print(f"[WARN] no snapshot file: {SNAPSHOT_CSV}")
        return

    df = pd.read_csv(SNAPSHOT_CSV)
    if df.empty:
        print("[WARN] snapshot empty")
        return

    df["snapshot_time_utc"] = pd.to_datetime(df["snapshot_time_utc"], errors="coerce", utc=True)
    df = df.dropna(subset=["snapshot_time_utc", "date", "root_symbol"])
    df["yes_price"] = pd.to_numeric(df["yes_price"], errors="coerce")
    df["no_price"] = pd.to_numeric(df["no_price"], errors="coerce")
    df["volume_total"] = pd.to_numeric(df["volume_total"], errors="coerce")
    df["liquidity"] = pd.to_numeric(df["liquidity"], errors="coerce")

    # phase means
    phase_mean = (
        df.groupby(["date", "asset_class", "root_symbol", "market_phase"], as_index=False)["yes_price"]
        .mean()
        .pivot(index=["date", "asset_class", "root_symbol"], columns="market_phase", values="yes_price")
        .reset_index()
    )
    phase_mean.columns.name = None

    # daily basics
    daily = (
        df.groupby(["date", "asset_class", "root_symbol"], as_index=False)
        .agg(
            yes_open=("yes_price", "first"),
            yes_close=("yes_price", "last"),
            yes_mean=("yes_price", "mean"),
            yes_std=("yes_price", "std"),
            yes_min=("yes_price", "min"),
            yes_max=("yes_price", "max"),
            volume_mean=("volume_total", "mean"),
            liquidity_mean=("liquidity", "mean"),
            n_snapshots=("yes_price", "count"),
        )
        .sort_values(["date", "asset_class", "root_symbol"])
    )
    daily["daily_prob_change"] = daily["yes_close"] - daily["yes_open"]
    daily["intraday_range"] = daily["yes_max"] - daily["yes_min"]

    out = daily.merge(phase_mean, on=["date", "asset_class", "root_symbol"], how="left")
    out = out.rename(columns={"pre": "pre_prob", "open": "open_prob", "mid": "mid_prob", "close": "close_prob", "after": "after_prob"})

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(DAILY_CSV, index=False)
    print(f"[OK] daily features saved: {DAILY_CSV} rows={len(out)}")


if __name__ == "__main__":
    main()
