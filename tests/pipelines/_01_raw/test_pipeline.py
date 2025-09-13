from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from project001.config.logging_config import get_test_logging_config
from project001.pipelines._01_raw.nodes import (
    IngestConfig,
    _get_news_data,
    _get_stock_data,
    ingest_raw_data,
)

logger = get_test_logging_config(test_name="test_pipeline_01_raw")

# Test Class
class TestRawPipeline:
    def setup_method(self):
        self.config = IngestConfig(
            language="pt",
            days_back=1,
            api_key="api_key",
            period="1d"
        )
        self.tickers = {"EMBR3.SA": "Embraer", "PETR4.SA": "Petrobras"}
        self.company = "Embraer"
        self.ticker = "EMBR3.SA"

    @patch("project001.pipelines._01_raw.nodes.yf.Ticker")
    def test_get_stock_data(self, mock_ticker, fake_stock):
        mock_instance = MagicMock()
        mock_instance.history.return_value = fake_stock
        mock_ticker.return_value = mock_instance

        result = _get_stock_data(self.ticker, self.config.period)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "ticker" in result.columns
        assert "Date" in result.columns

    def test_get_news_data(self, fake_news):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None

        # Clean up NaN values and format the source as a dictionary to mimic the real API response
        articles = fake_news.dropna().to_dict(orient="records")
        for article in articles:
            article["source"] = {"name": article["source"]}

        mock_response.json.return_value = {
            "articles": articles
        }

        with patch("requests.get", return_value=mock_response):
            result = _get_news_data(self.company, self.config.language, self.config.days_back, self.config.api_key)
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert set(result.columns) == set(fake_news.columns)



    def test_get_news_data_http_error(self):
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {}
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("403 Forbidden")

        with patch("requests.get", return_value=mock_response):
            result = _get_news_data(self.company, self.config.language, self.config.days_back, self.config.api_key)
            assert result is None


    @patch("project001.pipelines._01_raw.nodes.yf.Ticker")
    def test_get_stock_data_empty_result(self, mock_ticker):
        mock_instance = MagicMock()
        mock_instance.history.return_value = pd.DataFrame()
        mock_ticker.return_value = mock_instance

        result = _get_stock_data("INVALID", self.config.period)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_ingest_raw_data_with_empty_fetchers(self):
        def empty_stock_fetcher(ticker, period):
            return pd.DataFrame()

        def empty_news_fetcher(company, language, days_back, api_key):
            return pd.DataFrame()

        stock_data, news_data = ingest_raw_data(
            tickers=self.tickers,
            config=self.config,
            stock_fetcher=empty_stock_fetcher,
            news_fetcher=empty_news_fetcher
        )

        for key in stock_data:
            assert stock_data[key].empty
        for key in news_data:
            assert news_data[key].empty

    @patch("project001.pipelines._01_raw.nodes._get_stock_data")
    @patch("project001.pipelines._01_raw.nodes._get_news_data")
    def test_ingest_multiple_tickers(self, mock_news, mock_stock, fake_stock, fake_news):
        mock_stock.side_effect = [fake_stock.copy(), fake_stock.copy()]
        mock_news.side_effect = [fake_news.copy(), fake_news.copy()]

        stock_data, news_data = ingest_raw_data(
            tickers=self.tickers,
            config=self.config,
            stock_fetcher=mock_stock,
            news_fetcher=mock_news
        )

        assert set(stock_data.keys()) == {"EMBR3_SA", "PETR4_SA"}
        assert set(news_data.keys()) == {"EMBR3_SA", "PETR4_SA"}
        for df in stock_data.values():
            assert not df.empty

    @patch("project001.pipelines._01_raw.nodes.yf.Ticker")
    def test_stock_data_has_expected_columns(self, mock_ticker, fake_stock):
        expected_columns = {"Date", "Open", "High", "Low", "Close", "Volume", "ticker"}
        mock_instance = MagicMock()
        mock_instance.history.return_value = fake_stock
        mock_ticker.return_value = mock_instance

        result = _get_stock_data(self.ticker, self.config.period)
        assert expected_columns.issubset(set(result.columns))

    def test_ingest_raw_data_stock_fetcher_fails(self):
        def failing_stock_fetcher(ticker, period):
            raise Exception("Stock fetcher failed")

        stock_data, news_data = ingest_raw_data(
            tickers=self.tickers,
            config=self.config,
            stock_fetcher=failing_stock_fetcher,
            news_fetcher=lambda *args, **kwargs: pd.DataFrame()  # Mock news fetcher
        )

        assert stock_data == {}
        assert set(news_data.keys()) == {"EMBR3_SA", "PETR4_SA"}
        assert all(df.empty for df in news_data.values())

    def test_ingest_raw_data_news_fetcher_fails(self):
        def failing_news_fetcher(company, language, days_back, api_key):
            raise Exception("News fetcher failed")

        stock_data, news_data = ingest_raw_data(
            tickers=self.tickers,
            config=self.config,
            stock_fetcher=lambda *args, **kwargs: pd.DataFrame(),  # Mock stock fetcher
            news_fetcher=failing_news_fetcher
        )

        assert news_data == {}
        assert set(stock_data.keys()) == {"EMBR3_SA", "PETR4_SA"}
        assert all(df.empty for df in stock_data.values())
