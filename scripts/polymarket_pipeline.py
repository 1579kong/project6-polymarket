from __future__ import annotations

import argparse
import time
import uuid
from pathlib import Path

import pandas as pd

from market_selector import build_watchlist, get_event_detail, extract_outcome_probs
from utils import ensure_parent, market_phase_from_utc, now_utc, to_float


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SNAPSHOT_CSV = DATA_DIR / "market_snapshots.csv"
WATCHLIST_CSV = DATA_DIR / "watchlist_current.csv"
MAPPING_CSV = DATA_DIR / "market_mapping.csv"


def append_csv(path: Path, df: pd.DataFrame) -> None:
    if df.empty:
        return
    ensure_parent(path)
    if path.exists():
        old = pd.read_csv(path)
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(path, index=False)


def _event_is_still_open(event_id: str) -> bool:
    try:
        detail = get_event_detail(event_id)
    except Exception:
        return False
    return bool(detail.get("active")) and not bool(detail.get("closed")) and not bool(detail.get("archived"))


def load_or_refresh_watchlist(dynamic_stock_n: int, dynamic_crypto_n: int) -> pd.DataFrame:
    # Reuse current watchlist if all tracked events are still open.
    # If any event is closed/resolved, rebuild full watchlist from active events.
    if WATCHLIST_CSV.exists():
        try:
            current = pd.read_csv(WATCHLIST_CSV)
            if not current.empty and "event_id" in current.columns:
                all_open = True
                for event_id in current["event_id"].astype(str).tolist():
                    if not _event_is_still_open(event_id):
                        all_open = False
                        break
                if all_open:
                    print("[INFO] Reusing current watchlist (all events still open)")
                    return current
                print("[INFO] Detected closed/resolved event. Rebuilding watchlist from active events...")
        except Exception:
            pass
    return build_watchlist(dynamic_stock_n=dynamic_stock_n, dynamic_crypto_n=dynamic_crypto_n)


def save_watchlist_and_mapping(watchlist: pd.DataFrame) -> None:
    ensure_parent(WATCHLIST_CSV)
    watchlist.to_csv(WATCHLIST_CSV, index=False)

    if watchlist.empty:
        return
    map_cols = [
        "asset_class",
        "root_symbol",
        "watch_type",
        "event_id",
        "market_id",
        "question",
        "slug",
        "yes_token_id",
        "no_token_id",
    ]
    mapping = watchlist[map_cols].copy()
    mapping["updated_utc"] = now_utc().isoformat()
    if MAPPING_CSV.exists():
        old = pd.read_csv(MAPPING_CSV)
        mapping = pd.concat([old, mapping], ignore_index=True).drop_duplicates(
            subset=["event_id", "market_id", "yes_token_id", "no_token_id"], keep="last"
        )
    ensure_parent(MAPPING_CSV)
    mapping.to_csv(MAPPING_CSV, index=False)


def build_snapshot(watchlist: pd.DataFrame, run_id: str) -> pd.DataFrame:
    rows = []
    ts = now_utc()
    phase = market_phase_from_utc(ts)

    for rec in watchlist.to_dict("records"):
        status = "ok"
        yes_prob = rec.get("yes_prob")
        no_prob = rec.get("no_prob")
        volume = to_float(rec.get("volume"), 0.0)
        liquidity = to_float(rec.get("liquidity"), 0.0)
        open_interest = to_float(rec.get("open_interest"), 0.0)
        market_id = rec.get("market_id")
        try:
            detail = get_event_detail(rec["event_id"])
            markets = detail.get("markets", [])
            if markets:
                market = markets[0]
                market_id = market.get("id", market_id)
                yp, np_ = extract_outcome_probs(market)
                yes_prob = yp if yp is not None else yes_prob
                no_prob = np_ if np_ is not None else no_prob
                volume = to_float(market.get("volume"), volume)
                liquidity = to_float(market.get("liquidity"), liquidity)
                open_interest = to_float(detail.get("openInterest"), open_interest)
            else:
                status = "missing_markets"
        except Exception:
            status = "error"

        rows.append(
            {
                "run_id": run_id,
                "snapshot_time_utc": ts.isoformat(),
                "date": ts.date().isoformat(),
                "hour": ts.hour,
                "minute": ts.minute,
                "market_phase": phase,
                "asset_class": rec["asset_class"],
                "root_symbol": rec["root_symbol"],
                "watch_type": rec["watch_type"],
                "event_id": rec["event_id"],
                "market_id": market_id,
                "question": rec["question"],
                "slug": rec["slug"],
                "yes_token_id": rec.get("yes_token_id"),
                "no_token_id": rec.get("no_token_id"),
                "yes_price": yes_prob,
                "no_price": no_prob,
                "mid_price": ((yes_prob + (1.0 - no_prob)) / 2.0) if yes_prob is not None and no_prob is not None else None,
                "volume_total": volume,
                "liquidity": liquidity,
                "open_interest": open_interest,
                "match_score": rec.get("match_score"),
                "selection_score": rec.get("selection_score"),
                "fetch_status": status,
                "source_api": "gamma/events",
            }
        )
    return pd.DataFrame(rows)


def run_once(dynamic_stock_n: int, dynamic_crypto_n: int) -> None:
    run_id = str(uuid.uuid4())[:8]
    watchlist = load_or_refresh_watchlist(dynamic_stock_n=dynamic_stock_n, dynamic_crypto_n=dynamic_crypto_n)
    if watchlist.empty:
        print("[WARN] watchlist empty; skipping snapshot")
        return
    save_watchlist_and_mapping(watchlist)
    snap = build_snapshot(watchlist, run_id=run_id)
    append_csv(SNAPSHOT_CSV, snap)
    print(
        f"[OK] run={run_id} rows={len(snap)} fixed={int((watchlist['watch_type']=='fixed').sum())} "
        f"dynamic={int((watchlist['watch_type']=='dynamic').sum())}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-minutes", type=int, default=10)
    parser.add_argument("--dynamic-stock-n", type=int, default=2)
    parser.add_argument("--dynamic-crypto-n", type=int, default=1)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    if args.once:
        run_once(args.dynamic_stock_n, args.dynamic_crypto_n)
        return

    while True:
        run_once(args.dynamic_stock_n, args.dynamic_crypto_n)
        time.sleep(max(1, args.interval_minutes) * 60)


if __name__ == "__main__":
    main()
