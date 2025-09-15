"""Tests for the primary pipeline."""
import pandas as pd

from project001.config.logging_config import get_test_logging_config
from project001.pipelines._03_primary import nodes as primary_pipeline

logger = get_test_logging_config(test_name="test_pipeline_03_primary")

class TestPrimaryPipeline:
    """Test class for primary pipeline."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.pipeline = primary_pipeline

    def test_remove_columns(self, fake_stock):
        """Test removing columns from DataFrame."""
        # Add a ticker column to test its removal
        df = fake_stock.copy()

        # Create and apply the transformer
        remove_ticker = self.pipeline._remove_columns(['ticker'])
        result = remove_ticker(df)

        assert 'ticker' not in result.columns
        assert len(result.columns) == len(df.columns) - 1

    def test_lower_case_text_columns(self, fake_news):
        """Test converting text columns to lowercase."""
        # Create test data with mixed case
        df = fake_news.copy()

        result = self.pipeline._lower_case_text_columns(df)

        # Check if text columns are lowercase
        assert result['title'].str.islower().all()
        assert result['description'].str.islower().all()

    def test_round_numeric_columns(self, fake_stock):
        """Test rounding numeric columns."""
        df = fake_stock.copy()

        result = self.pipeline._round_numeric_columns(df)

        # Check if numeric columns are rounded to 2 decimal places
        assert (result['High'] == result['High'].round(2)).all()

    def test_format_date_columns(self, fake_stock, fake_news):
        """Test formatting date columns."""
        # Test with stock data
        stock_result = self.pipeline._format_date_columns(fake_stock.copy())
        assert pd.to_datetime(stock_result['Date']).dt.strftime('%Y-%m-%d').notna().all()

        # Test with news data
        news_result = self.pipeline._format_date_columns(fake_news.copy())
        assert pd.to_datetime(news_result['publishedAt']).dt.strftime('%Y-%m-%d').notna().all()

    def test_transformers_data_stock(self, fake_stock):
        """Test that the stock data is transformed correctly."""
        # Add ticker column to test data
        df = fake_stock.copy()

        # Define transformers
        transformers = [
            self.pipeline._remove_columns(['ticker']),
            self.pipeline._lower_case_text_columns,
            self.pipeline._round_numeric_columns,
            self.pipeline._format_date_columns,
        ]

        # Apply transformers
        result = self.pipeline._apply_transformers(df, transformers)

        # Check transformations
        assert 'ticker' not in result.columns
        assert result['Open'].round(2).equals(result['Open'])
        assert result['High'].round(2).equals(result['High'])
        assert result['Low'].round(2).equals(result['Low'])
        assert result['Close'].round(2).equals(result['Close'])

    def test_transformers_data_news(self, fake_news):
        """Test that the news data is transformed correctly."""
        # Add url column to test data
        df = fake_news.copy()

        # Define transformers
        transformers = [
            self.pipeline._remove_columns(['url']),
            self.pipeline._lower_case_text_columns,
            self.pipeline._format_date_columns,
        ]

        # Apply transformers
        result = self.pipeline._apply_transformers(df, transformers)

        # Check transformations
        assert 'url' not in result.columns
        assert result['title'].str.islower().all()
        assert pd.to_datetime(result['publishedAt']).dt.strftime('%Y-%m-%d').notna().all()

    def test_ingest_transformed_data_success(self, fake_stock, fake_news):
        """Test successful transformation of stock and news data."""
        def stock_loader():
            df = fake_stock.copy()
            return df

        def news_loader():
            df = fake_news.copy()
            return df

        tickers = {'TICK1.SA': 'Test Company'}
        intermediate_stock = {'TICK1_SA': stock_loader}
        intermediate_news = {'TICK1_SA': news_loader}

        stock_data, news_data = self.pipeline.ingest_transformed_data(
            tickers, intermediate_stock, intermediate_news)

        # Check stock data transformations
        assert 'TICK1_SA' in stock_data
        assert 'ticker' not in stock_data['TICK1_SA'].columns
        assert stock_data['TICK1_SA']['Open'].round(2).equals(stock_data['TICK1_SA']['Open'])

        # Check news data transformations
        assert 'TICK1_SA' in news_data
        assert 'url' not in news_data['TICK1_SA'].columns
        assert news_data['TICK1_SA']['title'].str.islower().all()
        assert pd.to_datetime(news_data['TICK1_SA']['publishedAt']).dt.strftime('%Y-%m-%d').notna().all()
