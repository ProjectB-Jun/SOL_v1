# Scripts

Python utilities for data acquisition and processing. Key entry points:
- `fetch_binance.py`: pulls SOL/USDT 15m futures data, calculates indicators,
  and serialises outputs to `data/` directories.

Scripts must support offline testing; factor out network IO for deterministic
unit tests and guard external calls with timeouts.
