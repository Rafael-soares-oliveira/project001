import pandas as pd

from project001.config.logging_config import get_test_logging_config
from project001.pipelines._02_intermediate.nodes import (
    _to_snake_case,
    _transform_data,
    ingest_transformed_data,
)

logger = get_test_logging_config(test_name="test_pipeline_02_intermediate")

class TestIntermediatePipeline:
    def setup_method(self) -> None:
        self.tickers = {"EMBR3.SA": "Embraer", "PETR4.SA": "Petrobras"}
        self.ticker_key = "EMBR3.SA"
        self.company = "Embraer"
        self.language = "pt"

    def test_transform_data_stock(self, fake_stock: pd.DataFrame):
        """
        Test that the stock data is transformed correctly.

        Args:
            fake_stock (pd.DataFrame): Fake stock data.
        """
        df = _transform_data(fake_stock.reset_index(), self.ticker_key)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        # Check if the columns after transformation are in snake_case
        expected_cols = pd.Index([_to_snake_case(col) for col in fake_stock.reset_index().columns])
        assert df.columns.equals(expected_cols)

        # Check if the duplicated values and NaN values are removed
        assert not df.duplicated().any()
        assert not df.isnull().any().any()

        # Check if DataFrame is sorted by date
        assert df["date"].is_monotonic_increasing

    def test_transform_data_news(self, fake_news: pd.DataFrame):
        """
        Test that the news data is transformed correctly.

        Args:
            fake_news (pd.DataFrame): Fake news data.
        """
        df = _transform_data(fake_news, self.ticker_key)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        # Check if the columns after transformation are in snake_case
        expected_cols = pd.Index([_to_snake_case(col) for col in fake_news.columns])
        assert df.columns.equals(expected_cols)

        # Check if the duplicated values and NaN values are removed
        assert not df.duplicated().any()
        assert not df.isnull().any().any()

        # Check if DataFrame is sorted by date
        assert df["published_at"].is_monotonic_increasing

    def test_ingest_transformed_data(self, fake_stock: pd.DataFrame, fake_news: pd.DataFrame):
        """
        Test that the transformed data is ingested correctly.

        Args:
            fake_stock (pd.DataFrame): Fake stock data.
            fake_news (pd.DataFrame): Fake news data.
        """
        raw_stock = {self.ticker_key.replace(".", "_"): lambda: fake_stock}
        raw_news = {self.ticker_key.replace(".", "_"): lambda: fake_news}
        stock_data, news_data = ingest_transformed_data(raw_stock=raw_stock, raw_news=raw_news, tickers=self.tickers)
        assert isinstance(stock_data, dict)
        assert isinstance(news_data, dict)
        assert not stock_data[self.ticker_key.replace(".", "_")].empty
        assert not news_data[self.ticker_key.replace(".", "_")].empty

    def test_ingest_transformed_data_missing_ticker(self, fake_stock: pd.DataFrame, fake_news: pd.DataFrame):
        """
        Test that the function handles missing tickers gracefully.

        Args:
            fake_stock (pd.DataFrame): Fake stock data.
            fake_news (pd.DataFrame): Fake news data.
        """
        raw_stock = {self.ticker_key.replace(".", "_"): lambda: fake_stock}
        raw_news = {}
        stock_data, news_data = ingest_transformed_data(raw_stock=raw_stock, raw_news=raw_news, tickers=self.tickers)
        assert isinstance(stock_data, dict)
        assert isinstance(news_data, dict)
        assert not stock_data[self.ticker_key.replace(".", "_")].empty
        assert len(news_data) == 0

    def test_ingest_transformed_data_empty_dataframe(self, fake_news: pd.DataFrame):
        """
        Test that the function handles empty DataFrames correctly.

        Args:
            fake_news (pd.DataFrame): Fake news data.
        """
        raw_stock = {self.ticker_key.replace(".", "_"): lambda: pd.DataFrame()}
        raw_news = {self.ticker_key.replace(".", "_"): lambda: fake_news}
        stock_data, news_data = ingest_transformed_data(raw_stock=raw_stock, raw_news=raw_news, tickers=self.tickers)
        assert isinstance(stock_data, dict)
        assert isinstance(news_data, dict)
        assert len(stock_data) == 0
        assert not news_data[self.ticker_key.replace(".", "_")].empty

    def test_ingest_transformed_data_loader_exception(self, fake_news: pd.DataFrame):
        """
        Test that the function handles exceptions during data loading.

        Args:
            fake_news (pd.DataFrame): Fake news data.
        """
        def failing_loader():
            raise ValueError("Failed to load data")
        raw_stock = {self.ticker_key.replace(".", "_"): failing_loader}
        raw_news = {self.ticker_key.replace(".", "_"): lambda: fake_news}
        stock_data, news_data = ingest_transformed_data(raw_stock=raw_stock, raw_news=raw_news, tickers=self.tickers)
        assert isinstance(stock_data, dict)
        assert isinstance(news_data, dict)
        assert len(stock_data) == 0
        assert not news_data[self.ticker_key.replace(".", "_")].empty
