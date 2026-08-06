"""
Unit tests for the Market Data Ingestion Pipeline.

These tests verify the basic functionality of the data ingestion module.
Some tests include intentional issues for educational purposes.
"""

import json
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import requests
from market_data import MarketDataIngestion


# Recorded sample responses from the market data API. See fixtures/API_CONTRACT.md
# for the full schema of each payload.
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str) -> dict:
    """Load a recorded API response payload by filename."""
    with open(FIXTURES / name) as f:
        return json.load(f)


class TestMarketDataIngestion(unittest.TestCase):
    """Test suite for MarketDataIngestion class"""

    def setUp(self):
        """Set up test fixtures"""
        self.ingestion = MarketDataIngestion(api_key="test-key")

    def test_initialization(self):
        """Test that MarketDataIngestion initializes with correct parameters"""
        api_key = "test-api-key"
        base_url = "https://custom.api.com"
        ingestion = MarketDataIngestion(api_key=api_key, base_url=base_url)

        self.assertEqual(ingestion.api_key, api_key)
        self.assertEqual(ingestion.base_url, base_url)
        self.assertIsNotNone(ingestion.session)

    @patch('market_data.requests.Session.get')
    def test_fetch_ticker_data_success(self, mock_get):
        """Test successful ticker data fetch"""
        quote = load_fixture("price_AAPL.json")

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = quote
        mock_get.return_value = mock_response

        result = self.ingestion.fetch_ticker_data("AAPL")

        self.assertEqual(result["ticker"], "AAPL")
        self.assertEqual(result["price"], 135.95)
        self.assertEqual(result["volume"], 84276641)

    @patch('interview_task_code.requests.Session.get')
    def test_fetch_ticker_data_network_error(self, mock_get):

        mock_get.side_effect = requests.RequestException("Connection refused")

        with self.assertRaises(requests.RequestException):
            self.ingestion.fetch_ticker_data("AAPL")

    @patch('interview_task_code.requests.Session.get')
    def test_fetch_ticker_data_invalid_json(self, mock_get):

        mock_response = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        with self.assertRaises(ValueError):
            self.ingestion.fetch_ticker_data("AAPL")

    @patch('interview_task_code.requests.Session.get')
    def test_fetch_historical_prices_success(self, mock_get):
        """Test successful historical data fetch"""
        history = load_fixture("history_AAPL.json")

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = history
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = self.ingestion.fetch_historical_prices("AAPL", days=30)

        self.assertEqual(len(result), 30)
        self.assertEqual(result[0]["date"], "2026-06-25")
        self.assertEqual(result[0]["close"], 137.66)

    @patch('interview_task_code.time.sleep')
    @patch('interview_task_code.requests.Session.get')
    def test_fetch_historical_prices_with_retry(self, mock_get, mock_sleep):

        # First attempt is rate limited; the vendor tells us how long to wait
        # via the Retry-After header (see fixtures/error_429.json).
        mock_response_fail = Mock()
        mock_response_fail.status_code = 429
        mock_response_fail.headers = {"Retry-After": "8"}
        mock_response_fail.json.return_value = load_fixture("error_429.json")
        mock_response_fail.raise_for_status.side_effect = requests.RequestException(
            "429 Too Many Requests"
        )

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = load_fixture("history_AAPL.json")
        mock_response_success.raise_for_status.return_value = None

        mock_get.side_effect = [mock_response_fail, mock_response_success]

        result = self.ingestion.fetch_historical_prices("AAPL", max_retries=3)

        self.assertEqual(len(result), 30)

        mock_sleep.assert_called_with(2)

    def test_enrich_with_historical_context(self):

        historical_data = load_fixture("history_AAPL.json")["data"]
        current_price = load_fixture("price_AAPL.json")["price"]

        with patch.object(self.ingestion, 'fetch_historical_prices', return_value=historical_data):
            result = self.ingestion.enrich_with_historical_context("AAPL", current_price=current_price)

            self.assertIsNotNone(result)
            self.assertEqual(len(result), 30)
            self.assertTrue(result["ma_7"].iloc[0:6].isna().any())


    def test_process_multiple_tickers(self):
        """Test processing multiple tickers"""
        tickers = ["AAPL", "GOOGL", "MSFT"]

        with patch.object(self.ingestion, 'fetch_ticker_data') as mock_fetch_ticker:
            with patch.object(self.ingestion, 'enrich_with_historical_context') as mock_enrich:
                # One recorded quote per ticker, shaped like fetch_ticker_data's output
                quotes = {t: load_fixture(f"price_{t}.json") for t in tickers}
                mock_fetch_ticker.side_effect = [
                    {
                        "ticker": t,
                        "price": quotes[t]["price"],
                        "timestamp": quotes[t]["timestamp"],
                        "volume": quotes[t]["volume"],
                    }
                    for t in tickers
                ]

                mock_df = pd.DataFrame({
                    "date": ["2026-08-01"],
                    "close": [150.0],
                    "volume": [1000000],
                    "price_change": [0.0],
                    "pct_change": [0.0],
                    "ma_7": [150.0],
                    "current_price": [150.0],
                    "price_vs_current": [100.0],
                })
                mock_enrich.return_value = mock_df

                results = self.ingestion.process_multiple_tickers(tickers)

                self.assertEqual(len(results), 3)
                self.assertIn("AAPL", results)
                self.assertIn("GOOGL", results)
                self.assertIn("MSFT", results)

    @patch('interview_task_code.MongoClient')
    def test_store_price_data_success(self, mock_mongo_client):

        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client

        mock_collection = MagicMock()
        mock_client.__getitem__.return_value.__getitem__.return_value = mock_collection

        self.ingestion.store_price_data("AAPL", 150.25, 1000000)

        mock_collection.insert_one.assert_called_once()

    @patch('interview_task_code.MongoClient')
    def test_store_price_data_connection_fails(self, mock_mongo_client):

        mock_mongo_client.side_effect = Exception("Connection refused")

        with self.assertRaises(Exception):
            self.ingestion.store_price_data("AAPL", 150.25, 1000000)

    @patch('interview_task_code.MongoClient')
    def test_store_and_verify_success(self, mock_mongo_client):

        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client

        mock_collection = MagicMock()
        mock_client.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_collection.find_one.return_value = {"ticker": "AAPL", "price": 150.25}

        result = self.ingestion.store_and_verify("AAPL", 150.25, 1000000)

        self.assertTrue(result)

        self.assertEqual(mock_mongo_client.call_count, 1)

    @patch('interview_task_code.MongoClient')
    def test_store_and_verify_not_found(self, mock_mongo_client):
        """Test store and verify when data is not found"""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client

        mock_collection = MagicMock()
        mock_client.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_collection.find_one.return_value = None

        result = self.ingestion.store_and_verify("AAPL", 150.25, 1000000)

        self.assertFalse(result)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""

    def setUp(self):
        """Set up test fixtures"""
        self.ingestion = MarketDataIngestion(api_key="test-key")

    def test_fetch_historical_prices_all_retries_fail(self):
        """Test that all retries failing returns None"""
        with patch.object(self.ingestion, 'session') as mock_session:
            mock_session.get.side_effect = requests.RequestException("Network error")

            result = self.ingestion.fetch_historical_prices("AAPL", max_retries=2)

            self.assertIsNone(result)

    def test_enrich_with_no_historical_data(self):
        """Test enrichment when historical data fetch returns None"""
        with patch.object(self.ingestion, 'fetch_historical_prices', return_value=None):
            result = self.ingestion.enrich_with_historical_context("AAPL", current_price=100.0)

            self.assertIsNone(result)

    def test_process_multiple_tickers_with_failure(self):
        """Test that failures in one ticker don't crash entire batch"""
        tickers = ["AAPL", "INVALID"]

        quote = load_fixture("price_AAPL.json")

        with patch.object(self.ingestion, 'fetch_ticker_data') as mock_fetch:
            mock_fetch.side_effect = [
                {
                    "ticker": "AAPL",
                    "price": quote["price"],
                    "timestamp": quote["timestamp"],
                    "volume": quote["volume"],
                },
                requests.RequestException("API Error")
            ]

            with patch.object(self.ingestion, 'enrich_with_historical_context') as mock_enrich:
                mock_enrich.return_value = pd.DataFrame()

                results = self.ingestion.process_multiple_tickers(tickers)

                self.assertEqual(len(results), 1)
                self.assertIn("AAPL", results)


if __name__ == "__main__":
    unittest.main()
