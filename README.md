# Project001: Model for predicting attractiveness rate

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Overview
This project aims to develop a predictive model that estimates the attractiveness rate of financial assets, combining quantitative market data with qualitative sentiment analysis from news sources. It leverages Kedro for pipeline orchestration and UV as the package manager to ensure fast, reproducible, and isolated Python environments.

## Tech Stack Highlights
- 🔁**Kedro**: Modular pipeline framework for reproducible data science workflows.
- 📦**UV**: Ultra-fast Python package manager for dependency resolution and environment isolation.
- 💰**yfinance**: API for extracting historical and real-time stock market data.
- 🔍**NewsAPI**: Service for fetching news articles related to financial assets.
- 🧠**Transformers (Hugging Face)**: Library for state-of-the-art natural language processing models.

## Pipeline Architecture (Kedro)
The pipeline structure follow the data enginnering convention. <a href="https://docs.kedro.org/en/1.0.0/getting-started/faq/#data-catalog">Kedro: Data Catalog</a>.
- **Raw**: Ingest stock data and news data. Save as parquet files.
- **Intermediate**: Cleans and standardizes raw inputs. Save as parquet files.
- **Primary**: Applies core transformations and filters. Save as parquet files.
- **Feature**: Generate features: technical indicator + sentiment analysis. Merge both datasets. Save as parquet files.
- **Model Input**: Prepare final dataset for training and testing. Save as parquet files.
- **Model**: Trains predictive models on attractiveness rate.
- **Model Output**: Output predictions and confidence scores.
- **Reporting**: Builds visual reports and dashboards.

## Data Sources
- **Stock Data**: yfinance
- **News Data**: NewsAPI
- **Sentiment Analysis**: Transformers (Hugging Face)

## Modeling Strategy
- Combines quantitative indicators with qualitative sentiment features.
- Evaluates models using metrics such as accuracy, precision, recall, F1-score, and ROC-AUC.
- Supports hyperparameter tuning using Optuna and cross-validation.

## Logging
- Combine Kedro logging with personalized logging configuration.
- Console logging for real-time progress tracking.
- File logging for persistent records, stored in data-based folders.
- Log rotation to manage log file sizes.

## Tests
- Unit tests for individual components (nodes, functions) using pytest.
- Integration tests to verify the pipeline end-to-end.
- Data quality checks to ensure data integrity and consistency.
- Tests for logging configuration.