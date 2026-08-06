# Market Data API — Contract & Sample Responses

Vendor documentation for the third-party market data service this pipeline consumes, plus the
recorded sample responses used by the test suite.

> **Base URL:** `https://api.example.com`
>
> `example.com` is reserved by RFC 2606 for documentation, so it never resolves to a live
> service. The base URL is injectable — `MarketDataIngestion(api_key=..., base_url=...)` — so
> it can be pointed at a local mock or a sandbox host.

All payloads in `src/fixtures/` are synthetic. They are shaped like real vendor responses but
contain no real market data, and no test makes a network call.

Authentication: `apikey` query parameter on every request.

---

## 1. `GET /price/{ticker}` — current quote

**Request**

```
GET /price/AAPL?apikey=<key>
```

**200 response** — [`price_AAPL.json`](price_AAPL.json)

```json
{
  "ticker": "AAPL",
  "price": 135.95,
  "currency": "USD",
  "timestamp": "2026-08-05T20:00:00Z",
  "volume": 84276641,
  "open": 133.67,
  "high": 136.93,
  "low": 132.60,
  "previous_close": 133.67,
  "change": 2.28,
  "change_percent": 1.71,
  "market_status": "closed",
  "source": "consolidated_tape"
}
```

Also available: [`price_GOOGL.json`](price_GOOGL.json), [`price_MSFT.json`](price_MSFT.json) —
same schema, used by the multi-ticker tests.

Field notes:

- `timestamp` is ISO-8601 UTC (`Z` suffix), **not** a Unix epoch.
- `price` is a JSON number. Some vendors emit it as a string (`"135.95"`) instead.
- `market_status` is one of `open`, `closed`, `pre_market`, `post_market`. Outside market
  hours, `price` is the last trade rather than a live tick.
- `change` is relative to `previous_close`; `change_percent` is that delta as a percentage.
- The response is wider than the fields this pipeline currently consumes.

---

## 2. `GET /history/{ticker}` — historical daily bars

**Request**

```
GET /history/AAPL?apikey=<key>&days=30
```

**200 response** — [`history_AAPL.json`](history_AAPL.json) (30 daily OHLCV bars)

```json
{
  "ticker": "AAPL",
  "currency": "USD",
  "interval": "1d",
  "requested_days": 30,
  "data": [
    {
      "date": "2026-06-25",
      "open": 138.40,
      "high": 138.71,
      "low": 136.81,
      "close": 137.66,
      "volume": 42201304
    },
    {
      "date": "2026-06-26",
      "open": 137.66,
      "high": 138.60,
      "low": 137.46,
      "close": 138.06,
      "volume": 67431272
    },

    "  ... 26 more bars, one per trading day ...  ",

    {
      "date": "2026-08-04",
      "open": 133.39,
      "high": 134.46,
      "low": 132.54,
      "close": 133.67,
      "volume": 41131587
    },
    {
      "date": "2026-08-05",
      "open": 133.67,
      "high": 136.93,
      "low": 132.60,
      "close": 135.95,
      "volume": 84276641
    }
  ]
}
```

The elided middle rows above are an editorial placeholder for readability — the fixture file
itself contains all 30 bar objects and is valid JSON.

Field notes:

- `date` is `YYYY-MM-DD`, no time component.
- Rows are ordered **ascending by date** (oldest first).
- `days=30` means 30 *trading* days, so the calendar range is longer than 30 days and contains
  gaps for weekends and market holidays.
- `data` may legitimately be `[]` — a newly listed or delisted symbol has no bars.
- The final bar's `close` and `volume` match the current quote in `price_AAPL.json`, since both
  describe the same session.

---

## 3. Error responses

**429 Too Many Requests** — [`error_429.json`](error_429.json)

```json
{
  "error": {
    "code": "rate_limit_exceeded",
    "message": "Request quota exceeded for this API key. Retry after the interval indicated by the Retry-After header.",
    "retry_after_seconds": 8,
    "limit": 120,
    "window": "1m"
  },
  "request_id": "req_01HZQ8N4KP3VYB2MTX9RJC7FDW"
}
```

Sent with a `Retry-After: 8` header. The rate limit is 120 requests per minute per API key.

**500 Internal Server Error** — [`error_500.json`](error_500.json)

```json
{
  "error": {
    "code": "upstream_unavailable",
    "message": "The upstream market data provider did not respond in time. This is a transient condition; the request is safe to retry.",
    "retryable": true
  },
  "request_id": "req_01HZQ8P2M6TXKD4RA1VBSN0EYH"
}
```

**404 Not Found** — unknown symbol — [`error_404.json`](error_404.json)

```json
{
  "error": {
    "code": "symbol_not_found",
    "message": "No instrument matches ticker 'AAPLL'.",
    "retryable": false
  },
  "request_id": "req_01HZQ8QG9WFYN3CB5KDTRXM2VA"
}
```

Every error body carries `request_id` (quote this to vendor support) and, except for 429, a
`retryable` boolean. `retryable: false` means the request will fail identically on every
attempt.

---

## Using the fixtures in tests

The test suite stubs `requests` with `unittest.mock` rather than making HTTP calls. Fixtures
are loaded through the `load_fixture` helper in `market-data-test.py`, which resolves paths
relative to the test file so tests can be run from any working directory:

```python
import json
from pathlib import Path

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name):
    with open(FIXTURES / name) as f:
        return json.load(f)
```

Then attach a payload to a mocked response:

```python
from unittest.mock import Mock, patch

with patch("market_data.requests.Session.get") as mock_get:
    mock_get.return_value = Mock(
        status_code=200,
        **{"json.return_value": load_fixture("history_AAPL.json")},
    )
    df = ingestion.enrich_with_historical_context("AAPL", current_price=135.95)
```

For tests that need real HTTP round-trips against a local stub, `responses` or `requests-mock`
would need adding to `requirements-dev.txt` — neither is currently pinned.
