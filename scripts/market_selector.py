from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import re

import pandas as pd

from utils import safe_request, to_float


EVENTS_URL = "https://gamma-api.polymarket.com/events"


FIXED_STOCKS = ["TSLA", "NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "SPY", "QQQ", "AMD"]
FIXED_CRYPTOS = ["BTC", "ETH", "SOL"]

STOCK_ALIASES = {
    "TSLA": ["tesla", "tsla"],
    "NVDA": ["nvidia", "nvda"],
    "AAPL": ["apple", "aapl"],
    "MSFT": ["microsoft", "msft"],
    "AMZN": ["amazon", "amzn"],
    "META": ["meta", "facebook", "fb"],
    "GOOGL": ["google", "googl", "alphabet"],
    "SPY": ["spy", "s&p 500", "sp500", "s&p"],
    "QQQ": ["qqq", "nasdaq 100", "nasdaq-100", "nasdaq"],
    "AMD": ["amd", "advanced micro devices"],
}

CRYPTO_ALIASES = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth"],
    "SOL": ["solana", "sol"],
}


@dataclass
class SelectedMarket:
    asset_class: str
    root_symbol: str
    watch_type: str
    event_id: str
    market_id: str
    question: str
    slug: str
    yes_token_id: str | None
    no_token_id: str | None
    yes_prob: float | None
    no_prob: float | None
    volume: float
    liquidity: float
    open_interest: float
    match_score: float
    selection_score: float


def _event_text(e: dict) -> str:
    return f"{e.get('title', '')} {e.get('slug', '')} {e.get('description', '')}".lower()


def get_active_events(limit: int = 200, max_pages: int = 60) -> list[dict]:
    rows: list[dict] = []
    for page in range(max_pages):
        payload = safe_request(
            EVENTS_URL,
            params={
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": page * limit,
            },
        )
        if not isinstance(payload, list) or not payload:
            break
        rows.extend(payload)
    return rows


def get_event_detail(event_id: Any) -> dict:
    data = safe_request(f"{EVENTS_URL}/{event_id}")
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected event detail for event_id={event_id}")
    return data


def extract_token_ids(market: dict) -> tuple[str | None, str | None]:
    ids = market.get("clobTokenIds") or market.get("tokenIds")
    if ids is None:
        return None, None
    if isinstance(ids, str):
        cleaned = ids.replace("[", "").replace("]", "").replace('"', "")
        parts = [x.strip() for x in cleaned.split(",") if x.strip()]
    elif isinstance(ids, list):
        parts = [str(x) for x in ids]
    else:
        parts = []
    if len(parts) < 2:
        return None, None
    return parts[0], parts[1]


def extract_outcome_probs(market: dict) -> tuple[float | None, float | None]:
    outcomes = market.get("outcomes")
    prices = market.get("outcomePrices")
    if outcomes is None or prices is None:
        return None, None
    if isinstance(outcomes, str):
        outcomes = outcomes.replace("[", "").replace("]", "").replace('"', "")
        outcomes = [x.strip() for x in outcomes.split(",") if x.strip()]
    if isinstance(prices, str):
        prices = prices.replace("[", "").replace("]", "").replace('"', "")
        prices = [x.strip() for x in prices.split(",") if x.strip()]
    if not isinstance(outcomes, list) or not isinstance(prices, list):
        return None, None
    mapping = {}
    for o, p in zip(outcomes, prices):
        try:
            mapping[str(o).strip().lower()] = float(p)
        except Exception:
            continue
    yes = mapping.get("yes", mapping.get("up"))
    no = mapping.get("no", mapping.get("down"))
    return yes, no


def _event_score(e: dict) -> float:
    return (
        0.50 * to_float(e.get("volume"), 0.0)
        + 0.30 * to_float(e.get("liquidity"), 0.0)
        + 0.20 * to_float(e.get("openInterest"), 0.0)
    )


def _alias_in_text(alias: str, text: str) -> bool:
    alias = alias.lower().strip()
    if not alias:
        return False
    if " " in alias or "-" in alias:
        return alias in text
    return re.search(rf"\b{re.escape(alias)}\b", text) is not None


def _best_event_for_aliases(events: list[dict], aliases: list[str], must_have: list[str]) -> tuple[dict | None, float]:
    aliases = [a.lower() for a in aliases]
    must_have = [m.lower() for m in must_have]
    best = None
    best_score = -1.0
    best_match = 0.0
    for e in events:
        txt = _event_text(e)
        alias_hits = sum(1 for a in aliases if _alias_in_text(a, txt))
        if alias_hits == 0:
            continue
        if not all(m in txt for m in must_have):
            continue
        s = _event_score(e)
        if s > best_score:
            best = e
            best_score = s
            best_match = float(alias_hits) / float(max(1, len(aliases)))
    return best, best_match


def _infer_symbol(event: dict, fallback_prefix: str) -> str:
    title = str(event.get("title", ""))
    m = re.search(r"\(([A-Z]{2,6})\)", title)
    if m:
        return m.group(1)
    slug = str(event.get("slug", ""))
    token = slug.split("-")[0].upper() if slug else fallback_prefix
    if token in {"WILL", "WHAT", "WHO", "IS", "THE"}:
        return fallback_prefix
    return token


def _selected_from_event(event: dict, asset_class: str, symbol: str, watch_type: str, match_score: float) -> SelectedMarket | None:
    detail = get_event_detail(event["id"])
    markets = detail.get("markets", [])
    if not markets:
        return None
    market = markets[0]
    yes_token_id, no_token_id = extract_token_ids(market)
    yes_prob, no_prob = extract_outcome_probs(market)
    return SelectedMarket(
        asset_class=asset_class,
        root_symbol=symbol,
        watch_type=watch_type,
        event_id=str(event.get("id")),
        market_id=str(market.get("id")),
        question=str(market.get("question", "")),
        slug=str(market.get("slug", "")),
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        yes_prob=yes_prob,
        no_prob=no_prob,
        volume=to_float(market.get("volume"), 0.0),
        liquidity=to_float(market.get("liquidity"), 0.0),
        open_interest=to_float(detail.get("openInterest"), 0.0),
        match_score=match_score,
        selection_score=_event_score(detail),
    )


def build_watchlist(dynamic_stock_n: int = 2, dynamic_crypto_n: int = 1) -> pd.DataFrame:
    events = get_active_events()
    picked: list[SelectedMarket] = []

    # Fixed stocks
    for sym in FIXED_STOCKS:
        event, match_score = _best_event_for_aliases(events, STOCK_ALIASES[sym], ["up or down"])
        if event is None:
            continue
        selected = _selected_from_event(event, "stock", sym, "fixed", match_score)
        if selected:
            picked.append(selected)

    # Fixed crypto
    for sym in FIXED_CRYPTOS:
        event, match_score = _best_event_for_aliases(events, CRYPTO_ALIASES[sym], [])
        if event is None:
            continue
        selected = _selected_from_event(event, "crypto", sym, "fixed", match_score)
        if selected:
            picked.append(selected)

    existing_event_ids = {x.event_id for x in picked}
    existing_symbols = {x.root_symbol for x in picked}

    # Dynamic pools
    stock_candidates = []
    crypto_candidates = []
    stock_terms = ["nasdaq", "nyse", "s&p", "spy", "qqq", "djia", "dow", "russell", "ndx", "spx"]
    crypto_terms = ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "doge", "xrp", "crypto"]

    for e in events:
        txt = _event_text(e)
        if str(e.get("id")) in existing_event_ids:
            continue
        if _alias_in_text("up or down", txt) and any(_alias_in_text(k, txt) for k in stock_terms):
            stock_candidates.append(e)
        if any(_alias_in_text(k, txt) for k in crypto_terms):
            crypto_candidates.append(e)

    stock_candidates.sort(key=_event_score, reverse=True)
    crypto_candidates.sort(key=_event_score, reverse=True)

    dyn_stock_added = 0
    for e in stock_candidates:
        if dyn_stock_added >= dynamic_stock_n:
            break
        symbol = _infer_symbol(e, "DYN_STOCK")
        if symbol in existing_symbols:
            continue
        selected = _selected_from_event(e, "stock", symbol, "dynamic", match_score=0.5)
        if selected:
            picked.append(selected)
            existing_symbols.add(symbol)
            dyn_stock_added += 1

    dyn_crypto_added = 0
    for e in crypto_candidates:
        if dyn_crypto_added >= dynamic_crypto_n:
            break
        symbol = _infer_symbol(e, "DYN_CRYPTO")
        if symbol in existing_symbols:
            continue
        selected = _selected_from_event(e, "crypto", symbol, "dynamic", match_score=0.5)
        if selected:
            picked.append(selected)
            existing_symbols.add(symbol)
            dyn_crypto_added += 1

    df = pd.DataFrame([x.__dict__ for x in picked])
    if df.empty:
        return df
    return df.sort_values(["asset_class", "watch_type", "root_symbol"]).reset_index(drop=True)
