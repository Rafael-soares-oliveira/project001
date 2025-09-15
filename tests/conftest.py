from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


# Helpers
def make_fake_stock(days_back: int = 2):
    """
    Make a fake stock DataFrame. Date columns are not index.

    Args:
        days_back (int, optional): Number of days back to generate data. Defaults to 2.

    Returns:
        pd.DataFrame: Fake stock DataFrame.
    """
    today = datetime.now()
    start_date = today - timedelta(days=days_back)
    dates = pd.to_datetime([start_date + timedelta(days=i) for i in range(days_back + 1)])
    return pd.DataFrame({
        "Date": dates,
        "Open": [100.0546, np.nan, 101.7894],
        "High": [100.0213, 101.0213123, 102.54646],
        "Low": [99.89876, 100.64564, 101.21354],
        "Close": [101.64564, np.nan, 102.64568],
        "Volume": [1000, 1001, 1002],
        "Dividends": [0.0, 0.0, 0.0],
        "Stock Splits": [0.0, 0.0, 0.0],
        "ticker": ["TICK1.SA", "TICK2.SA", "TICK3.SA"]
    })

def make_fake_news(days_back: int = 2):
    """
    Make a fake news DataFrame. Date columns are not index.

    Args:
        days_back (int, optional): Number of days back to generate data. Defaults to 2.

    Returns:
        pd.DataFrame: Fake news DataFrame.
    """
    today = datetime.now()
    start_date = today - timedelta(days=days_back)
    middle_date = start_date + timedelta(days=1)
    return pd.DataFrame({
        "title": ["News 1", np.nan, "News 3"],
        "description": ["Desc 1", np.nan, "Desc 3"],
        "url": ["URL 1", np.nan, "URL 3"],
        "publishedAt": [start_date, middle_date, today],
        "source": ["Source 1", np.nan, "Source 3"],
        "content": ["Content 1", np.nan, "Content 3"],
    })

# Fixtures
@pytest.fixture
def fake_stock():
    return make_fake_stock()

@pytest.fixture
def fake_news():
    return make_fake_news()
