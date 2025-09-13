
import re

import pandas as pd

from project001.config.logging_config import get_logging_config
from project001.utils import typing as personal_typing

# typing
Transformer = personal_typing.Transformer # Type alias for transformer function
IngestFrames = personal_typing.IngestFrames # Type alias for ingest frames function
PartitionLoader = personal_typing.PartitionLoader # Type alias for partition loader function
TickersFrames = personal_typing.TickersFrames # Type alias for tickers frames function

logger = get_logging_config(pipeline_name="intermediate_pipeline")

def _to_snake_case(name: str) -> str:
    """Converts a string to snake_case."""
    name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    name = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', name)
    name = name.replace(' ', '_')
    return name.lower()

def _transform_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Transform data from pipeline 01_raw.

    Args:
        df (pd.DataFrame): DataFrame from pipeline 01_raw.
        ticker (str): Ticker of stock.

    Returns:
        Optional[pd.DataFrame]: DataFrame with transformed data.
    """

    try:
        logger.info(f"Transforming data for ticker {ticker}.")
        df = df.copy()

        logger.info("Converting columns to snake_case.")
        df.columns = [_to_snake_case(col) for col in df.columns]
        logger.info("Columns names transformed.")

        logger.info("Removing duplicated and null values.")
        qt_duplicated = df.duplicated().sum() # Count duplicated rows
        qt_null = df.isnull().sum().sum() # Count null values
        df = df.drop_duplicates().dropna() # Drop duplicated rows and null
        logger.info(f"Removed {qt_duplicated} duplicated rows and {qt_null} null values.")

        # Sort DataFrame by available date column
        date_column = next((col for col in ["date", "published_at"] if col in df.columns), None) # Get available date column

        if date_column:
            logger.info(f"Sorting data by {date_column}.")
            df.sort_values(by=date_column, inplace=True)
            logger.info(f"Data sorted by {date_column}.")

        logger.info(f"Transformed stock data for ticker {ticker}")
        return df

    except Exception as e:
        logger.error(f"Error transforming DataFrame for ticker {ticker}: {e}")
        raise e

def ingest_transformed_data(tickers: TickersFrames, raw_stock: IngestFrames, raw_news: IngestFrames, transformer: Transformer = _transform_data) -> IngestFrames:
    """
    Transform raw data from pipeline 01_raw.

    Args:
        tickers (TickersFrames): Mapping of ticker symbols to company names.
        raw_stock (IngestFrames): Stock data from pipeline 01_raw.
        raw_news (IngestFrames): News data from pipeline 01_raw.
        transformer (Transformer): Function to transform data.

    Returns:
        IngestFrames: Transformed data.
    """
    logger.info("Starting data transformation")

    news_data = {}
    stock_data = {}

    for ticker, company in tickers.items():
        ticker_name = ticker.replace(".", "_")
        logger.info(f"Attempting to transform data for ticker {ticker} ({ticker_name}).")

        # Transform stock data
        if ticker_name in raw_stock:
            try:
                stock_df = raw_stock[ticker_name]()
                if stock_df is not None and not stock_df.empty:
                    stock_data[ticker_name] = transformer(stock_df, ticker)
                    logger.info(f"Successfully transformed stock data for {ticker_name}.")
                else:
                    logger.warning(f"Stock data for {ticker_name} is empty or None. Skipping transformation.")
            except Exception as e:
                logger.error(f"Error loading or transforming stock data for {ticker_name}: {e}")
        else:
            logger.warning(f"No stock data partition found for {ticker_name}. Skipping.")

        # Transform news data
        if ticker_name in raw_news:
            try:
                news_df = raw_news[ticker_name]()
                if news_df is not None and not news_df.empty:
                    news_data[ticker_name] = transformer(news_df, ticker)
                    logger.info(f"Successfully transformed news data for {ticker_name}.")
                else:
                    logger.warning(f"News data for {ticker_name} is empty or None. Skipping transformation.")
            except Exception as e:
                logger.error(f"Error loading or transforming news data for {ticker_name}: {e}")
        else:
            logger.warning(f"No news data partition found for {ticker_name}. Skipping.")

    logger.info("Data transformation process completed.")
    return stock_data, news_data
