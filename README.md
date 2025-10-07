# SOLUSDT Run-Mode Data Lake

This repository aggregates 15-minute Binance futures data for `SOL/USDT` along with
macro and market structure indicators required for the "Run" trading regime.
The project emphasises reliability, small incremental changes, and fully audited
data pipelines.

## Repository Layout

```
├── analysis/              # Jupyter notebooks for exploratory analysis and regime EV checks
├── data/                  # Time-partitioned raw & processed datasets committed via CI
├── scripts/               # Fetchers and processors (Python)
├── tests/                 # Indicator unit tests (pytest)
├── .github/workflows/     # Automation pipelines (GitHub Actions)
└── README.md              # Project overview and operations manual
```

### Data Directories
- `data/price_patterns/YYYY-MM-DD.json`: consolidated 15m price snapshot including
  RSI, MACD, Bollinger regimes, and detected chart pattern labels.
- `data/volatility/YYYY-MM-DD_atr.csv`: ATR and volatility statistics for stop-loss
  calibration across the most recent 30 days.
- `data/vwap/YYYY-MM-DD.json`: session VWAP and directional bias flags.
- `data/funding/YYYY-MM-DD_rates.csv`: 8-hour funding history, predicted rate, and
  crowding annotations.
- `data/oi_volume/YYYY-MM-DD.csv`: open interest, volume, leverage ratio, and spike flags.
- `data/altseason/YYYY-MM-DD.json`: BlockchainCenter altseason index and component metrics.
- `data/btc_d/YYYY-MM-DD.csv`: BTC dominance time-series and trend classification.
- `data/eth_btc/YYYY-MM-DD.json`: ETH/BTC ratio with short/medium term change deltas.
- `data/total_caps/YYYY-MM-DD.csv`: TOTAL2/TOTAL3 market caps and rotation signals.
- `data/stable_etf/YYYY-MM-DD.json`: Stablecoin supply stock/flow plus ETF & shutdown status feed.

### Scripts
- `scripts/fetch_binance.py`: Fetches SOL/USDT OHLCV candles, computes indicators,
  and serialises all 10 data products using ccxt + pure Python indicator formulas.
- `scripts/__main__.py` (optional entry point): reserved for orchestration if we
  expand to multi-symbol coverage.

### Analysis
Place notebooks in `analysis/` using the naming convention `YYYYMMDD_<topic>.ipynb`.
Run notebooks via the provided Docker or local Python environment. Notebooks should
consume data from the `data/` partition and must never write secrets or API keys.

## Getting Started

1. **Environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt  # requirements file will be derived from scripts usage
   ```
   Required libraries: `ccxt`, `pandas`, `numpy`, `requests`.

2. **Local Test Run**
   ```bash
   pytest -q
   ```

3. **Manual Data Pull**
   ```bash
   python scripts/fetch_binance.py --write --out data
   ```
   The command fetches the latest 100 15m candles, computes indicators,
  writes price snapshots to `data/price_patterns/`, and prints the file paths.
  Use `--debug` to print fallback diagnostics when troubleshooting API access
  (e.g., regional geoblocking). Domain failover preserves futures endpoints on
  futures mirrors (`fapi*`) and spot endpoints on spot mirrors (`api*`), so set
  `BINANCE_API_BASE` to the specific host family you need (e.g.,
  `https://fapi1.binance.com` for futures). When Binance's default domain is
  blocked, choose an accessible mirror from the same family (for example,
  `https://fapi.binancefuture.com` or `https://fapi.binance.me`) to avoid
  hitting the wrong service.

## Automation (GitHub Actions)

` .github/workflows/update_data.yml` defines a cron-based workflow that executes the
fetch script every 15 minutes. The workflow caches dependencies, respects API rate
limits, and avoids committing when no data changes are detected.

## Data Quality & Missing Data Policy

- Missing values are stored as explicit `null` (JSON) or empty strings (CSV), never
  forward-filled. Downstream analytics must treat missing data as risk and either
  drop rows or interpolate with caution.
- All outbound API calls include 10s timeouts and retry with exponential backoff.
- Unit tests cover indicator math to prevent silent schema drifts.

## Contributing

- Work on feature branches prefixed with `codex/` and open pull requests only.
- Follow Conventional Commits (e.g., `feat: add atr calculator`).
- Update both `README.md` and `CHANGELOG.md` when introducing new data products or
  modifying workflows.

## License

This repository is private and for research/trading use only. Redistribution
requires explicit permission.
