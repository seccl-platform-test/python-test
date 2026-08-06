"""
Data Ingestion Pipeline for Market Data

This module pulls market data from an external API, enriches it with
historical context, and prepares it for analysis.
"""

import requests
import pandas as pd
import time
from typing import List, Dict, Optional
from pymongo import MongoClient


class MarketDataIngestion:
    """Handles fetching and processing market data from external APIs"""

    def __init__(self, api_key: str, base_url: str = "https://api.example.com"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()

    def fetch_ticker_data(self, ticker: str) -> Dict:
        """
        Fetch current price data for a given ticker symbol.

        Args:
            ticker: Stock ticker symbol (e.g., "AAPL")

        Returns:
            Dictionary with price data
        """
        url = f"{self.base_url}/price/{ticker}"
        params = {"apikey": self.api_key}

        response = self.session.get(url, params=params, timeout=10)
        data = response.json()

        return {
            "ticker": ticker,
            "price": data["price"],
            "timestamp": data["timestamp"],
            "volume": data["volume"]
        }

    def fetch_historical_prices(self, ticker: str, days: int = 30, max_retries: int = 3) -> Optional[List[Dict]]:
        """
        Fetch historical price data with retry logic.

        Args:
            ticker: Stock ticker symbol
            days: Number of historical days to fetch
            max_retries: Maximum retry attempts

        Returns:
            List of price dictionaries or None if all retries fail
        """
        url = f"{self.base_url}/history/{ticker}"
        params = {"apikey": self.api_key, "days": days}

        retry_count = 0

        while retry_count < max_retries:
            try:
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                return response.json()["data"]

            except requests.RequestException as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count 
                    print(f"Retry {retry_count}/{max_retries} after {wait_time}s: {e}")
                    time.sleep(2)

        return None

    def enrich_with_historical_context(self, ticker: str, current_price: float) -> pd.DataFrame:
        """
        Combine current price with historical data to create analysis dataframe.

        Args:
            ticker: Stock ticker symbol
            current_price: Current price of the ticker

        Returns:
            DataFrame with enriched data
        """
        historical = self.fetch_historical_prices(ticker)

        if not historical:
            print(f"Could not fetch historical data for {ticker}")
            return None

        # Convert to dataframe
        df = pd.DataFrame(historical)

        df["price_change"] = df["close"].diff()
        df["pct_change"] = df["close"].pct_change()
        df["ma_7"] = df["close"].rolling(window=7).mean()
        df["current_price"] = current_price
        df["price_vs_current"] = (df["close"] / current_price * 100).round(2)

        return df[["date", "close", "volume", "price_change", "pct_change", "ma_7", "current_price", "price_vs_current"]]

    def process_multiple_tickers(self, tickers: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Process data for multiple ticker symbols.

        Args:
            tickers: List of ticker symbols

        Returns:
            Dictionary mapping ticker to enriched dataframe
        """
        results = {}

        for ticker in tickers:
            try:
                current_data = self.fetch_ticker_data(ticker)
                enriched_df = self.enrich_with_historical_context(
                    ticker,
                    current_data["price"]
                )
                results[ticker] = enriched_df

            except Exception as e:
                print(f"Failed to process {ticker}: {e}")

        return results

    def store_price_data(self, ticker: str, price: float, volume: int) -> None:
        """
        Store price data in MongoDB.

        Args:
            ticker: Stock ticker symbol
            price: Current price
            volume: Trading volume
        """

        client = MongoClient("mongodb://localhost:27017")
        db = client["market_data"]
        collection = db["prices"]
        collection.insert_one({"ticker": ticker, "price": price, "volume": volume})

    def store_and_verify(self, ticker: str, price: float, volume: int) -> bool:
        """
        Store price data and verify it was written.

        Args:
            ticker: Stock ticker symbol
            price: Current price
            volume: Trading volume

        Returns:
            Boolean indicating if data exists in database
        """

        client = MongoClient("mongodb://localhost:27017")
        db = client["market_data"]
        collection = db["prices"]
        collection.insert_one({"ticker": ticker, "price": price, "volume": volume})

        result = collection.find_one({"ticker": ticker})

        return result is not None


# Example usage
if __name__ == "__main__":
    ingestion = MarketDataIngestion(api_key="your-api-key")

    # Single ticker
    data = ingestion.fetch_ticker_data("AAPL")
    print(f"Current AAPL price: ${data['price']}")

    # Multiple tickers
    tickers = ["AAPL", "GOOGL", "MSFT"]
    results = ingestion.process_multiple_tickers(tickers)

    for ticker, df in results.items():
        if df is not None:
            print(f"\n{ticker}:")
            print(df.head())
