from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
import yfinance as yf

from project001.config.logging_config import get_logging_config
from project001.utils import typing as personal_typing

# typing
StockFetcher = personal_typing.StockFetcher # Type alias for stock fetcher function
NewsFetcher = personal_typing.NewsFetcher # Type alias for news fetcher function
IngestFrames = personal_typing.IngestFrames # Type alias for ingest frames function
TickersFrames = personal_typing.TickersFrames # Type alias for tickers frames function

@dataclass
class IngestConfig:
    """
    Configuration class for raw data ingestion.

    Args:
        language (str): News language.
        days_back (int): Number of days to look back.
        api_key (str): NewsAPI key.
        period (str): Historical period for stock data.
    """
    language: str
    days_back: int
    api_key: str
    period: str

logger = get_logging_config(pipeline_name="raw_pipeline")

def _get_stock_data(ticker: str, period: str) -> Optional[pd.DataFrame]:
    """
    Fetch stock data from Yahoo Finance.

    Args:
        ticker (str): Stock ticker symbol.
        period (str): Historical period (e.g., '1d', '5d', '1mo').

    Returns:
        pd.DataFrame: Stock data.
    """
    try:
        logger.info(f"Fetching stock data for {ticker} with period '{period}'")
        ticker_obj = yf.Ticker(ticker)
        stock_data = ticker_obj.history(period=period)
        stock_data.reset_index(inplace=True) # Make that date column is not a index
        stock_data["ticker"] = ticker
        logger.info(f"Successfully retrieved stock data for {ticker}")
        return stock_data
    except Exception as e:
        logger.error(f"Failed to fetch stock data for {ticker}: {e}")
        return

def _get_news_data(company: str, language: str, days_back: int, api_key: str) -> Optional[pd.DataFrame]:
    """
    Fetch news data from NewsAPI.

    Args:
        company (str): Company name.
        language (str): News language.
        days_back (int): Number of days to look back.
        api_key (str): NewsAPI key.

    Returns:
        pd.DataFrame: News articles.
    """
    try:
        logger.info(f"Fetching news for '{company}' in language '{language}' from the last {days_back} days")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        url = "https://newsapi.org/v2/everything"
        params = {
            "q": company,
            "language": language,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
            "apiKey": api_key,
            "sortBy": "publishedAt",
            "pageSize": 100,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        news_data = response.json()

        if "articles" not in news_data or not news_data["articles"]:
            logger.warning(f"No articles found for '{company}'")
            return

        news = [
            {
                "title": article.get("title"),
                "description": article.get("description"),
                "url": article.get("url"),
                "publishedAt": article.get("publishedAt"),
                "source": article.get("source", {}).get("name"),
                "content": article.get("content"),
            }
            for article in news_data["articles"]
        ]

        if not news:
            logger.warning(f"No relevant news found for '{company}'")
            return

        logger.info(f"Successfully retrieved news for '{company}'")
        return pd.DataFrame(news)

    except Exception as e:
        logger.error(f"Failed to fetch news for '{company}': {e}")
        return

def ingest_raw_data(
    tickers: TickersFrames,
    config: IngestConfig,
    stock_fetcher: StockFetcher = _get_stock_data,
    news_fetcher: NewsFetcher = _get_news_data,
) -> Optional[IngestFrames]:
    """
    Ingest raw stock and news data.

    Args:
        - tickers (TickersFrames): Mapping of ticker symbols to company names.
        - config (IngestConfig): Configuration for news fetching.
        - stock_fetcher (StockFetcher): Function to fetch stock data.
        - news_fetcher (NewsFetcher): Function to fetch news data.

    Returns:
        Optional[IngestFrames]: Stock and news data.

    Note:
        The `config` parameter must contain the following attributes:
        - language (str): News language.
        - days_back (int): Number of days to look back.
        - api_key (str): NewsAPI key.
        - period (str): Historical period for stock data.
    """
    logger.info("Starting raw data ingestion")
    stock_data = {}
    news_data = {}

    for ticker in tickers.keys():
        try:
            data = stock_fetcher(ticker, period=config.period)
            if data is not None:
                stock_data[ticker.replace(".", "_")] = data
        except Exception as e:
            logger.error(f"Error during stock data fetching for {ticker}: {e}")

    for ticker, company in tickers.items():
        try:
            data = news_fetcher(
                company, config.language, config.days_back, config.api_key
            )
            if data is not None:
                news_data[ticker.replace(".", "_")] = data
        except Exception as e:
            logger.error(f"Error during news data fetching for {company}: {e}")

    logger.info("Raw data ingestion completed successfully")
    return stock_data, news_data if stock_data or news_data else None
