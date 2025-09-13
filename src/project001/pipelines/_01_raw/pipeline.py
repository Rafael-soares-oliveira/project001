
from kedro.pipeline import Node, Pipeline  # noqa
from .nodes import ingest_raw_data, IngestConfig

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        Node(
            func=IngestConfig,
            inputs={
                "language": "params:language",
                "days_back": "params:days_back",
                "api_key": "news_api_key",
                "period": "params:period",
            },
            outputs="ingest_config",
            name="ingest_config",
        ),
        Node(
            func=ingest_raw_data,
            inputs={
                "tickers": "params:tickers",
                "config": "ingest_config",
            },
            outputs=["01_raw_stock", "01_raw_news"],
            name="ingest_raw_data",
            namespace="raw_pipeline",
        ),
    ],
    tags=["raw_pipeline"],
)
