# Project 6: Polymarket Data Pipeline

This repository contains a practical MVP for Project 6:

- High-frequency Polymarket snapshot collection (5/10-minute ready)
- Fixed + dynamic watchlist selection (stocks + crypto)
- CSV accumulation for reproducible research
- Daily feature aggregation for ML/regression/backtest
- Streamlit dashboard scaffold
- GitHub Actions scheduler template

## Structure

```text
project6_polymarket/
├── data/
├── scripts/
│   ├── utils.py
│   ├── market_selector.py
│   ├── polymarket_pipeline.py
│   └── build_daily_features.py
├── dashboard/
│   └── app.py
├── .github/
│   └── workflows/
│       └── polymarket_10min.yml
└── requirements.txt
```

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run one collection cycle:

```bash
python scripts/polymarket_pipeline.py --once
```

3. Build daily features:

```bash
python scripts/build_daily_features.py
```

4. Open dashboard:

```bash
streamlit run dashboard/app.py
```

## Output Files

- `data/market_snapshots.csv`: append-only high-frequency snapshot table
- `data/watchlist_current.csv`: currently tracked markets
- `data/market_mapping.csv`: symbol-market mapping history
- `data/daily_features.csv`: daily aggregated feature table

## Notes

- API endpoints are not always consistently filtered by query params. This pipeline uses event-level selection and event detail refresh for robustness.
- For production use, add retry/backoff, schema validation, and robust logging.
- In GitHub Actions mode, CSVs are uploaded as artifacts (download-only) to avoid local file-lock conflicts.
