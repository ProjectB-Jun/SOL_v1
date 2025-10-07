# Changelog

## [0.1.1] - 2025-02-16
- Retry Binance domain fallbacks on generic network timeouts/resets so transient
  `Read timed out` errors rotate to the next mirror before failing the job.
- Add additional Binance futures/spot mirror domains (`binanceusdt`, `binanceusds`,
  `binancezh`) so geoblocked runners have more fallback hosts before failing.
- Add Binance domain failover and debug flag to tolerate regional API blocking.
- Document debug usage and provide environment override guidance.
- Fix failover base rewriting to support ccxt's nested URL maps and futures hosts.
- Prevent futures endpoints from being rewritten to spot mirrors during failover,
  avoiding `BadSymbol` errors when geoblocks require host rotation.
- Extend the fallback host list with Binance futures-friendly mirrors to bypass
  451 "restricted location" outages in CI environments.

## [0.1.0] - 2025-02-15
- Scaffold repository structure for SOL/USDT 15m data aggregation.
- Add indicator computation utilities and automation blueprint.
