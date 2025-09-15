import typing as tp

from pandas import DataFrame

StockFetcher = tp.Callable[[str, str], DataFrame]
NewsFetcher = tp.Callable[[str, str], DataFrame]
IngestFrames = tuple[dict[str, DataFrame], dict[str, DataFrame]]
Transformer = tp.Callable[..., DataFrame]
TransformerPipeline = tp.Callable[[DataFrame], DataFrame]
PartitionLoader = tuple[str, tp.Callable[[], DataFrame]]
TickersFrames = tuple[str, DataFrame]

