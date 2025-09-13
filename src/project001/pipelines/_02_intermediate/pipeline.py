from kedro.pipeline import Node, Pipeline  # noqa
from project001.pipelines._02_intermediate.nodes import ingest_transformed_data

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        Node(
            func=ingest_transformed_data,
            inputs={
                "raw_stock": "01_raw_stock", # import from pipeline 01_raw
                "raw_news": "01_raw_news", # import from pipeline 01_raw
                "tickers": "params:tickers",
            },
            outputs=["02_intermediate_stock", "02_intermediate_news"],
            name="ingest_transformed_data",
            namespace="intermediate_pipeline",
        ),
    ])
