from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SNAPSHOT_CSV = DATA_DIR / "market_snapshots.csv"
WATCHLIST_CSV = DATA_DIR / "watchlist_current.csv"
DAILY_CSV = DATA_DIR / "daily_features.csv"


st.set_page_config(page_title="Project 6 - Polymarket", layout="wide")
st.title("Project 6: Polymarket Pipeline Dashboard")


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


snap = read_csv(SNAPSHOT_CSV)
watch = read_csv(WATCHLIST_CSV)
daily = read_csv(DAILY_CSV)

with st.sidebar:
    st.header("Filters")
    asset_filter = st.multiselect("Asset class", options=sorted(snap["asset_class"].dropna().unique().tolist()) if not snap.empty else [])
    symbol_filter = st.multiselect("Root symbol", options=sorted(snap["root_symbol"].dropna().unique().tolist()) if not snap.empty else [])
    st.caption("Tip: click Rerun in top-right after pipeline update.")

if not watch.empty:
    st.subheader("Current Watchlist")
    st.dataframe(watch, use_container_width=True)

if snap.empty:
    st.warning("No snapshots yet. Run pipeline first: `python scripts/polymarket_pipeline.py --once`")
    st.stop()

snap["snapshot_time_utc"] = pd.to_datetime(snap["snapshot_time_utc"], errors="coerce", utc=True)
snap = snap.dropna(subset=["snapshot_time_utc"])

flt = snap.copy()
if asset_filter:
    flt = flt[flt["asset_class"].isin(asset_filter)]
if symbol_filter:
    flt = flt[flt["root_symbol"].isin(symbol_filter)]

st.subheader("Live Probability Paths")
for symbol in sorted(flt["root_symbol"].dropna().unique()):
    d = flt[flt["root_symbol"] == symbol].sort_values("snapshot_time_utc")
    if d.empty:
        continue
    chart = d[["snapshot_time_utc", "yes_price", "no_price"]].set_index("snapshot_time_utc")
    st.markdown(f"**{symbol}**")
    st.line_chart(chart, height=220, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    st.subheader("Latest Snapshots")
    latest = (
        flt.sort_values("snapshot_time_utc")
        .groupby(["asset_class", "root_symbol"], as_index=False)
        .tail(1)
        .sort_values(["asset_class", "root_symbol"])
    )
    st.dataframe(latest, use_container_width=True)
with col2:
    st.subheader("Daily Features")
    if daily.empty:
        st.info("No daily features yet. Run `python scripts/build_daily_features.py`")
    else:
        st.dataframe(daily.sort_values(["date", "asset_class", "root_symbol"], ascending=[False, True, True]), use_container_width=True)
