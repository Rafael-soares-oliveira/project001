import pandas as pd

from project001.config.logging_config import get_logging_config
from project001.utils import typing as personal_typing

# typing
Transformer = personal_typing.Transformer # Type alias for transformer function
IngestFrames = personal_typing.IngestFrames # Type alias for ingest frames function
TickersFrames = personal_typing.TickersFrames # Type alias for tickers frames function
TransformerPipeline = personal_typing.TransformerPipeline # Type alias for transformer pipeline function

logger = get_logging_config(pipeline_name="test_pipeline_03_primary")

def _remove_columns(columns: list) -> pd.DataFrame:
    """
    Remove columns from a DataFrame.

    Args:
        columns (list): A list of column names to be removed.

    Returns:
        pd.DataFrame: The DataFrame with specified columns removed.
    """
    def transformers(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove columns from a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame from which columns will be removed.

        Returns:
            pd.DataFrame: The DataFrame with specified columns removed.
        """
        logger.info(f"Removing columns {columns} from DataFrame")
        try:
            df = df.drop(columns=columns)
            logger.info(f"Columns {columns} removed successfully")
            return df
        except KeyError as e:
            logger.error(f"Error removing columns: {e}")
            raise
        except Exception as e:
            logger.error(f"Error removing columns: {e}")
            raise
    return transformers


def _lower_case_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert text columns in a DataFrame to lowercase.

    Args:
        df (pd.DataFrame): The DataFrame whose text columns will be converted to lowercase.

    Returns:
        pd.DataFrame: The DataFrame with text columns in lowercase.
    """
    logger.info("Converting text columns to lowercase")
    try:
        object_cols = df.select_dtypes(include=["object"]).columns
        df[object_cols] = df[object_cols].apply(lambda x: x.str.lower())
        logger.info("Text columns converted to lowercase successfully")
        return df
    except Exception as e:
        logger.error(f"Error converting text columns to lowercase: {e}")
        raise

def _round_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Round numeric columns in a DataFrame to 2 decimal places.

    Args:
        df (pd.DataFrame): The DataFrame whose numeric columns will be rounded.

    Returns:
        pd.DataFrame: The DataFrame with numeric columns rounded.
    """
    logger.info("Rounding numeric columns")
    try:
        float_columns = df.select_dtypes(include=["float64"]).columns
        df[float_columns] = df[float_columns].round(2)
        logger.info("Numeric columns rounded successfully")
        return df
    except Exception as e:
        logger.error(f"Error rounding numeric columns: {e}")
        raise

def _format_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format date columns in a DataFrame to a specified date format.

    Args:
        df (pd.DataFrame): The DataFrame whose date columns will be formatted.

    Returns:
        pd.DataFrame: The DataFrame with date columns formatted.
    """
    logger.info("Formatting date columns")
    try:
        date_columns = [col for col in ["date", "published_at"] if col in df.columns]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
        logger.info("Date columns formatted successfully")
        return df
    except Exception as e:
        logger.error(f"Error formatting date columns: {e}")
        raise

def _apply_transformers(df: pd.DataFrame, transformers: list[Transformer]) -> pd.DataFrame:
    """
    Apply a list of transformers to a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to transform.
        transformers (list[Transformer]): The list of transformers to apply.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    for transformer in transformers:
        df = transformer(df)
    return df

def ingest_transformed_data(
    tickers: TickersFrames,
    intermediate_stock: IngestFrames,
    intermediate_news: IngestFrames) -> IngestFrames:
    """
    Transform intermediate stock and news data.

    Args:
        tickers (TickersFrames): The tickers data.
        intermediate_stock (IngestFrames): The intermediate stock data.
        intermediate_news (IngestFrames): The intermediate news data.

    Returns:
        IngestFrames: The transformed stock and news data.
    """
    logger.info("Transforming intermediate data")

    news_data = {}
    stock_data = {}

    for ticker, company in tickers.items():
        ticker_name = ticker.replace(".", "_")
        logger.info(f"Attempting to transform data for ticker {ticker}.")

        stock_transformers = [
            _remove_columns(["ticker"]),
            _lower_case_text_columns,
            _round_numeric_columns,
            _format_date_columns,
        ]

        news_transformers = [
            _remove_columns(["url"]),
            _lower_case_text_columns,
            _format_date_columns,
        ]


        if ticker_name in intermediate_stock:
            try:
                stock_df = intermediate_stock[ticker_name]()
                stock_df = _apply_transformers(stock_df, stock_transformers)
                stock_data[ticker_name] = stock_df
            except Exception as e:
                logger.error(f"Error transforming stock data for ticker {ticker}: {e}")
        else:
            logger.warning(f"No stock data found for ticker {ticker}.")
            continue

        if ticker_name in intermediate_news:
            try:
                news_df = intermediate_news[ticker_name]()
                news_df = _apply_transformers(news_df, news_transformers)
                news_data[ticker_name] = news_df
            except Exception as e:
                logger.error(f"Error transforming news data for ticker {ticker}: {e}")
        else:
            logger.warning(f"No news data found for ticker {ticker}.")
            continue

    return stock_data, news_data
