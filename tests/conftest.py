from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


# Helpers
def make_fake_stock(days_back: int = 2):
    today = datetime.now()
    start_date = today - timedelta(days=days_back)
    dates = pd.to_datetime([start_date + timedelta(days=i) for i in range(3)])
    return pd.DataFrame({
        "Open": [100.0, np.nan, 101.0],
        "High": [100.0, 101.0, 102.0],
        "Low": [99.0, 100.0, 101.0],
        "Close": [101.0, np.nan, 102.0],
        "Volume": [1000, 1001, 1002],
        "Dividends": [0.0, 0.0, 0.0],
        "Stock Splits": [0.0, 0.0, 0.0]
    }, index=pd.Index(dates, name="Date"))

def make_fake_news(days_back: int = 2):
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
