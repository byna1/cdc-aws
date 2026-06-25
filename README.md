# CDC AWS Pipeline

A Change Data Capture (CDC) pipeline that processes Kaggle datasets locally, uploads them to AWS S3 in Parquet format, and ingests them into Databricks for transformation and analysis using Delta Lake.

## Overview

This project implements an data engineering pipeline from dataset import to bronze layer on databricks:

1. **Data Ingestion**: Fetches datasets from Kaggle API
2. **Local Processing**: Converts raw data to Parquet format using DuckDB/Python
3. **Cloud Storage**: Uploads processed files to AWS S3 (bronze layer)
4. **Databricks Integration**: Merges data into Delta Lake tables with CDC logic

## Tech Stack

- **Python**: Data processing (boto3, DuckDB, pandas)
- **AWS S3**: Data lake (bronze layer)
- **Databricks**: Delta Lake & Unity Catalog
- **Git**: Version control

## Data Source & Limitations

This project uses real transactional data from the TeoMeWhy Channel's point system.

### Known Limitations

**CDC Logic Based on Modification Timestamps**

The CDC implementation relies on data modification dates to detect changes. However, some reference tables lack this metadata. This limitation is acceptable because:
- These tables contain static reference data (dimensions, lookup tables)
- They are rarely updated and don't require frequent synchronization
- Changes can be handled manually or through full refreshes when needed
