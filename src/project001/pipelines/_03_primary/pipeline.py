from kedro.pipeline import Node, Pipeline  # noqa
from project001.pipelines._03_primary.nodes import ingest_transformed_data

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        Node(
            ingest_transformed_data,
            inputs={
                "tickers": "params:tickers",
                "intermediate_stock": "02_intermediate_stock",
                "intermediate_news": "02_intermediate_news",
            },
            outputs=[
                "03_primary_stock",
                "03_primary_news",
            ],
            namespace="primary_pipeline",
            name="ingest_transformed_data",
        ),
    ])
