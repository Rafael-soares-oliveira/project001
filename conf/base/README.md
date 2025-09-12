#  Base configuration files
This folder contains global configuration files that are used by the project.

## Catalog
The catalog file contains the configuration for the pipelines. Each Node will have its own configuration.
```yaml
01_raw_news: # Name of configuration
  type: kedro_datasets.partitions.PartitionedDataSet # Type of dataset
  credentials: news_api # Credentials obtained from base/credentials.yml
  dataset: # Dataset configuration
    type: pandas.ParquetDataSet # Format of dataset
    save_args: # Arguments to save dataset
      index: False # Save without index
  path: data/01_raw/news # Path to save dataset
```

## Parameters
The parameters file contains parameters that are used by the project.
```yaml
tickers: # Tickers and companies that will be used in the project
  - EMBR3.SA: embraer
  - VALE3.SA: vale
period: "2mo" # Previous period to get stock values
language: "pt" # News language
days_back: 30 # Previous days to get the news
```
