# Market Data Ingestion Pipeline

A Python service that fetches real-time and historical market data from an external API, enriches it with technical analysis indicators, and stores results in MongoDB for downstream analysis.

## Features

- **Real-time Market Data**: Fetch current price, volume, and timestamp for any ticker
- **Historical Data with Retry Logic**: Retrieve historical price data with exponential backoff retry mechanism
- **Technical Analysis**: Automatically compute moving averages, price changes, and percentage changes
- **Batch Processing**: Process multiple tickers in a single operation with graceful error handling
- **MongoDB Integration**: Store and verify market data in MongoDB for persistence and querying
- **Session-based HTTP**: Reuses HTTP connections for efficiency

## Requirements

### System Requirements
- Python 3.8+
- MongoDB 4.0+ (for storage operations)
- Network access to market data API (https://api.example.com)

### API Requirements
- Valid API key for market data provider
- API key must have permissions for:
  - `/price/{ticker}` endpoint
  - `/history/{ticker}` endpoint

The API contract, including a sample response for every endpoint and error code, is documented
in [`src/fixtures/API_CONTRACT.md`](src/fixtures/API_CONTRACT.md). Neither a live API key nor
network access is needed to run the test suite — see [Mock API Responses](#mock-api-responses).

## Installation

### 1. Clone or Download
```bash
git clone <repository-url>
cd market-data-ingestion
```

### 2. Create Virtual Environment (Recommended)
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

**For production/implementation only:**
```bash
pip install -r requirements.txt
```

**For development (includes testing tools):**
```bash
pip install -r requirements-dev.txt
```

### 4. Configure MongoDB

Ensure MongoDB is running and accessible at `mongodb://localhost:27017`. To verify:
```bash
mongosh "mongodb://localhost:27017"
```

## Usage

### Basic Example

```python
from interview_task_code import MarketDataIngestion

# Initialize with your API key
ingestion = MarketDataIngestion(api_key="your-api-key-here")

# Fetch current price for a ticker
current_data = ingestion.fetch_ticker_data("AAPL")
print(f"AAPL Price: ${current_data['price']}")

# Fetch and enrich with historical context
enriched_df = ingestion.enrich_with_historical_context("AAPL", current_data["price"])
print(enriched_df.head())

# Process multiple tickers
tickers = ["AAPL", "GOOGL", "MSFT", "AMZN"]
results = ingestion.process_multiple_tickers(tickers)

# Store price data in MongoDB
ingestion.store_price_data("AAPL", price=150.25, volume=1000000)

# Store and verify
verified = ingestion.store_and_verify("AAPL", price=150.25, volume=1000000)
print(f"Data stored and verified: {verified}")
```

### Configuration Options

```python
# Custom base URL
ingestion = MarketDataIngestion(
    api_key="your-api-key",
    base_url="https://custom-api.example.com"
)

# Historical data parameters
prices = ingestion.fetch_historical_prices(
    ticker="AAPL",
    days=90,           # Fetch 90 days of history
    max_retries=5      # Retry up to 5 times on failure
)
```

### Output Formats

#### Current Price Data
```python
{
    "ticker": "AAPL",
    "price": 150.25,
    "timestamp": "2026-08-05T10:00:00Z",
    "volume": 1000000
}
```

#### Enriched Historical Data (DataFrame)
```
        date   close  volume  price_change  pct_change  ma_7  current_price  price_vs_current
0 2026-08-01  150.00    1000           NaN         NaN   NaN         150.25            100.17
1 2026-08-02  151.00    1100          1.00        0.67   NaN         150.25            100.50
2 2026-08-03  149.50     950         -1.50       -0.99   NaN         150.25             99.50
...
```

## Mock API Responses

The test suite never touches the network. Every HTTP call is stubbed with
`unittest.mock`, and the payloads handed back to those stubs are real recorded response
bodies kept as JSON under `src/fixtures/`:

| Fixture | Endpoint / status | Contents |
| --- | --- | --- |
| `price_AAPL.json` | `GET /price/AAPL` → 200 | Current quote: price `135.95`, volume `84,276,641`, session close of 2026-08-05 |
| `price_GOOGL.json` | `GET /price/GOOGL` → 200 | Current quote: price `178.42`, volume `21,884,305` |
| `price_MSFT.json` | `GET /price/MSFT` → 200 | Current quote: price `412.88`, volume `18,402,776` |
| `history_AAPL.json` | `GET /history/AAPL?days=30` → 200 | 30 daily OHLCV bars, 2026-06-25 → 2026-08-05, oldest first |
| `error_429.json` | 429 Too Many Requests | Rate-limit envelope, sent with `Retry-After: 8` |
| `error_500.json` | 500 Internal Server Error | Transient upstream failure, `retryable: true` |
| `error_404.json` | 404 Not Found | Unknown symbol, `retryable: false` |

Full schemas, field semantics and the request format for each endpoint are in
[`src/fixtures/API_CONTRACT.md`](src/fixtures/API_CONTRACT.md).

Tests load these through the `load_fixture` helper in `market-data-test.py`, which resolves
paths relative to the test file:

```python
quote = load_fixture("price_AAPL.json")

mock_response = Mock()
mock_response.status_code = 200
mock_response.json.return_value = quote
mock_get.return_value = mock_response
```

The fixtures are internally consistent, so assertions can rely on them: the last bar in
`history_AAPL.json` has the same `close` and `volume` as the quote in `price_AAPL.json`, since
both describe the same trading session.

All values are synthetic. The payloads are shaped like a real vendor's, but the prices,
volumes and request IDs are generated — nothing here is real market data.

## Running Tests

### Run All Tests
```bash
python -m unittest discover -s . -p "test_*.py" -v
```

### Run Specific Test File
```bash
python -m unittest test_interview_task_code -v
```

### Run Specific Test Class
```bash
python -m unittest test_interview_task_code.TestMarketDataIngestion -v
```

### Run Specific Test Method
```bash
python -m unittest test_interview_task_code.TestMarketDataIngestion.test_fetch_ticker_data_success -v
```

### With Coverage Report (requires pytest and coverage)
```bash
pytest test_interview_task_code.py -v --cov=interview_task_code
```

## Data Pipeline Flow

```
1. fetch_ticker_data(ticker)
   └─> HTTP GET /price/{ticker}
       └─> Returns: current price, volume, timestamp

2. fetch_historical_prices(ticker, days, max_retries)
   └─> HTTP GET /history/{ticker}
       └─> On failure: retry with exponential backoff
       └─> Returns: list of daily price records

3. enrich_with_historical_context(ticker, current_price)
   ├─> fetch_historical_prices(ticker)
   ├─> Convert to DataFrame
   ├─> Calculate: price_change, pct_change, ma_7 (7-day moving average)
   └─> Returns: enriched DataFrame

4. process_multiple_tickers(tickers)
   └─> For each ticker:
       ├─> fetch_ticker_data(ticker)
       ├─> enrich_with_historical_context(ticker, price)
       └─> Results stored in dictionary

5. store_price_data(ticker, price, volume)
   └─> MongoDB insert_one()
       └─> Stores: ticker, price, volume

6. store_and_verify(ticker, price, volume)
   ├─> MongoDB insert_one()
   ├─> MongoDB find_one(ticker)
   └─> Returns: verification boolean
```

## API Endpoints

### Fetch Current Price
- **Endpoint**: `GET /price/{ticker}`
- **Parameters**: 
  - `ticker`: Stock symbol (e.g., "AAPL")
  - `apikey`: Your API key
- **Timeout**: 10 seconds
- **Response**: JSON with price, timestamp, volume

### Fetch Historical Prices
- **Endpoint**: `GET /history/{ticker}`
- **Parameters**:
  - `ticker`: Stock symbol
  - `days`: Number of historical days (default: 30)
  - `apikey`: Your API key
- **Timeout**: 10 seconds
- **Retry**: Exponential backoff on failure

## MongoDB Collections

### Prices Collection
```json
{
  "_id": ObjectId(...),
  "ticker": "AAPL",
  "price": 150.25,
  "volume": 1000000
}
```

## Performance Characteristics

| Operation | Typical Latency | Notes |
|-----------|-----------------|-------|
| fetch_ticker_data | 200-500ms | Single HTTP call |
| fetch_historical_prices | 500ms-2s | With retry logic, first success |
| enrich_with_historical_context | 1-3s | Historical fetch + DataFrame computation |
| process_multiple_tickers | N × (1-3s) | Linear with ticker count |
| store_price_data | 10-50ms | Single MongoDB insert |
| store_and_verify | 20-100ms | Insert + query |

## Error Handling

The pipeline handles:
- **Network timeouts**: HTTP calls timeout after 10 seconds
- **Malformed responses**: Invalid JSON responses
- **Missing API fields**: Missing ticker data
- **Database errors**: MongoDB connection failures
- **Batch failures**: Individual ticker failures don't stop batch processing

## Logging

The module prints diagnostic messages to stdout:
```
Retry 1/3 after 2s: Connection timeout
Retry 2/3 after 4s: Connection timeout
Could not fetch historical data for AAPL
Failed to process GOOGL: KeyError 'price'
```

## Troubleshooting

### "Connection refused to MongoDB"
- Ensure MongoDB is running: `mongosh "mongodb://localhost:27017"`
- Check connection string in `store_price_data()` method

### "Invalid API key"
- Verify API key is correct and has proper permissions
- Check API provider status page

### "Timeout waiting for API"
- Increase timeout value in `fetch_ticker_data()` and `fetch_historical_prices()`
- Check network connectivity

### "KeyError: 'price'"
- Verify ticker symbol is valid
- Check API response format matches expectations

## Development

### Adding New Tickers
Simply pass new ticker symbols to:
```python
ingestion.process_multiple_tickers(["NEW_SYMBOL", "ANOTHER_SYMBOL"])
```

### Customizing Technical Indicators
Modify `enrich_with_historical_context()` method to add:
- Different moving average windows
- RSI, MACD, Bollinger Bands
- Custom calculations

### Changing MongoDB Database
Update database/collection names in `store_price_data()` and `store_and_verify()`:
```python
db = client["your_database_name"]
collection = db["your_collection_name"]
```

## Dependencies Overview

### Core Dependencies
- **requests**: HTTP client for API calls
- **pandas**: Data manipulation and DataFrame operations
- **pymongo**: MongoDB driver

### Development/Testing
- **pytest**: Test runner (optional, unittest also works)
- **pytest-cov**: Coverage reporting
- **unittest**: Built-in Python testing (included)
- **unittest.mock**: Mocking framework (included)

## License

[Specify your license here]

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review test file for usage examples
3. Check the API contract and sample responses in [`src/fixtures/API_CONTRACT.md`](src/fixtures/API_CONTRACT.md)

## Version History

- **1.0.0** (2026-08-05): Initial release
  - Real-time price fetching
  - Historical data with retry logic
  - Technical analysis enrichment
  - MongoDB storage
  - Batch processing
