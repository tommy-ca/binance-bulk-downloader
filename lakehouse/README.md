# Binance Market Data Ingestion to Apache Iceberg

## Purpose

This project provides a framework for ingesting Binance market data (klines and trades) into an Apache Iceberg data lake. It automates the fetching, transformation, and loading of data, preparing it for large-scale analytics.

## Overview

The system is designed around the following components:

-   **Data Fetching:** Utilizes the `binance-bulk-downloader` tool to download historical klines (candlestick) and trades data from Binance.
-   **ETL Orchestration:** Employs Prefect flows, specifically `ingest_klines_flow.py` and `ingest_trades_flow.py`, to manage the Extract, Transform, Load (ETL) process. These flows handle:
    -   Fetching raw data using `binance-bulk-downloader`.
    -   Transforming the data into a structured format.
    -   Loading the transformed data into Apache Iceberg tables (currently a placeholder).
-   **Data Structuring:** Defines clear schemas for klines and trades data using PyIceberg, found in `klines_schema.py` and `trades_schema.py` respectively. This ensures data consistency and compatibility with the Iceberg format.

## Key Features

-   **Automated Data Ingestion:** Prefect flows enable automated data fetching and processing, which can be scheduled for regular updates.
-   **Schema Enforcement:** Predefined PyIceberg schemas ensure that klines and trades data adhere to a consistent structure before being loaded into the data lake.
-   **Apache Iceberg Integration (Placeholder):** The project is structured to integrate with Apache Iceberg. The data loading tasks (`load_data_to_iceberg`) are currently placeholders, designed to be replaced with actual Iceberg write logic. This allows for future setup with various Iceberg catalogs such as REST, Hive, or AWS Glue.

## Prerequisites

-   Python 3.9+
-   Prefect (e.g., version 2.x)
-   `binance-bulk-downloader` (included as part of this repository's tooling)
-   `pandas`
-   `pyiceberg`
-   An accessible Apache Iceberg catalog (e.g., a local REST catalog, MinIO with a REST catalog, AWS Glue, etc.). Setting up the Iceberg catalog itself is beyond the scope of this guide but is crucial for the full functionality of the data loading part.

## Setup & Configuration

### 1. Install Dependencies

It's recommended to use a virtual environment. The main `requirements.txt` in the repository root covers some dependencies. For the lakehouse specific components, ensure you have:

```bash
pip install prefect pandas pyiceberg
# Potentially other dependencies like boto3 if using AWS Glue, etc.
```

### 2. Configure Environment

Several environment variables might be needed depending on your setup:

-   **Prefect:**
    -   `PREFECT_API_URL`: Set this if you are using a Prefect server/cloud instance that is not running on the default local address (`http://127.0.0.1:4200/api`).
-   **Apache Iceberg Catalog:**
    -   Configuration for PyIceberg depends on the catalog type. For example:
        -   For a REST catalog: `PYICEBERG_CATALOG__DEFAULT__URI=http://localhost:8181`
        -   For AWS Glue: `PYICEBERG_CATALOG__DEFAULT__TYPE=glue`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION_NAME`.
    -   Refer to the [PyIceberg documentation](https://py.iceberg.apache.org/configuration/) for catalog-specific environment variables.
-   **Flow Configuration (Suggestion):**
    -   The `download_base_dir` variable within the flow scripts (`ingest_klines_flow.py`, `ingest_trades_flow.py`) specifies where downloaded Binance data is temporarily stored. Consider making this configurable via an environment variable or a Prefect parameter for better flexibility.

### 3. (Placeholder) Initialize Iceberg Tables

The `load_data_to_iceberg` tasks within the flows are currently **placeholders** and do not perform actual writes to Iceberg. To make this fully functional, you would need to:

1.  **Configure your Iceberg Catalog:** Ensure your chosen Iceberg catalog (e.g., REST, Hive, Glue) is properly set up and accessible from the environment where the Prefect flows will run.
2.  **Create Database/Namespace:** Ensure the target database or namespace (e.g., `lakehouse.binance`) exists within your Iceberg catalog. This might need to be created manually depending on your catalog.
    ```sql
    -- Example for creating a namespace if your catalog supports SQL
    CREATE NAMESPACE IF NOT EXISTS lakehouse.binance;
    ```
3.  **Implement Load Logic:** Modify the `load_data_to_iceberg` function in `lakehouse/iceberg_utils/load.py` to:
    -   Connect to your configured Iceberg catalog.
    -   Use the PyIceberg schemas (`klines_schema`, `trades_schema`) to create tables if they don't exist (e.g., `lakehouse.binance.klines`, `lakehouse.binance.trades`).
    -   Append or overwrite data in these Iceberg tables.

## Running the Flows

### Directly (for testing)

You can run the flows directly as Python scripts. The `if __name__ == "__main__":` blocks in each flow script execute the flow with example parameters.

```bash
# Navigate to the repository root before running
python lakehouse/flows/ingest_klines_flow.py
python lakehouse/flows/ingest_trades_flow.py
```

This is useful for local testing and development. Note that this will use the placeholder data loading logic.

### Via Prefect Python API (for ad-hoc runs with custom parameters)

You can import and run flows directly using Python, which is useful for ad-hoc executions with specific parameters.

```python
from lakehouse.flows.ingest_klines_flow import ingest_binance_klines_flow
from lakehouse.flows.ingest_trades_flow import ingest_binance_trades_flow
from datetime import date

# Example for Klines
klines_result = ingest_binance_klines_flow(
    symbol="BTCUSDT",
    start_date=date(2024, 3, 10),
    end_date=date(2024, 3, 11),
    interval="1d"
)

# Example for Trades
trades_result = ingest_binance_trades_flow(
    symbol="ETHUSDT",
    start_date=date(2024, 3, 10),
    end_date=date(2024, 3, 11)
)
```

## Deploying Scheduled Flows

Prefect deployments allow you to manage, schedule, and trigger flow runs via the Prefect API/UI.

1.  **Examine Deployment Scripts:**
    A deployment script example is provided in `lakehouse/deployments/klines_deployment.py`. This script defines how the `ingest_binance_klines_flow` should be deployed, including its schedule, default parameters, and infrastructure configuration.

2.  **Run the Deployment Script:**
    Execute the deployment script to register the flow with your Prefect backend (local server or Prefect Cloud).

    ```bash
    python lakehouse/deployments/klines_deployment.py
    ```

3.  **Agent Requirement:**
    For scheduled runs to be executed, you need a Prefect agent running and connected to the correct work pool/queue specified in your deployment (or the default one if not specified). The agent polls the Prefect backend for new flow runs and executes them.

    ```bash
    # Example: Start an agent polling the default work pool
    prefect agent start -q default
    ```

4.  **Customize Deployments:**
    -   **Schedule:** Modify the `schedule` (e.g., `CronSchedule`) in the deployment script to fit your desired ingestion frequency.
    -   **Parameters:** Adjust default parameters. For scheduled runs, you'll likely want dynamic date parameters (e.g., ingest data for "yesterday"). This logic needs to be implemented (see Future Enhancements).
    -   **Trades Deployment:** Create a similar deployment script (e.g., `trades_deployment.py`) for the `ingest_trades_flow.py`.

## Schemas

-   **`klines_schema.py`:** Defines the PyIceberg schema for Binance klines data. This includes fields like open time, open price, high price, low price, close price, volume, etc.
-   **`trades_schema.py`:** Defines the PyIceberg schema for Binance trades data. This includes fields like trade ID, price, quantity, time, is_buyer_maker, etc.

These schemas are crucial for ensuring data consistency when loading into Iceberg tables. Refer to the files for the detailed structure.

## Future Enhancements & Considerations

-   **Implement Full Iceberg Integration:** The most critical next step is to replace the placeholder `load_data_to_iceberg` function in `lakehouse/iceberg_utils/load.py` with actual Iceberg write logic using `pyiceberg`. This includes table creation, data appending/overwriting, and potentially partition management.
-   **Error Handling & Retries:** Enhance the Prefect flows with more robust error handling (e.g., try-except blocks around critical tasks) and leverage Prefect's built-in retry mechanisms for tasks that might fail due to transient issues (e.g., network errors).
-   **Configuration Management:** Utilize Prefect's configuration system (e.g., Blocks, global variables) for managing parameters like download directories, Iceberg table names, catalog details, and API keys, rather than hardcoding or relying solely on environment variables.
-   **Dynamic Date Handling for Schedules:** Implement logic within the flows or deployment parameter functions to dynamically calculate `start_date` and `end_date` for scheduled runs (e.g., ingest data for "yesterday," "last hour").
-   **Testing:** Add comprehensive unit tests for transformations and utility functions, and integration tests for the flows (potentially mocking the Binance downloader and Iceberg interactions).
-   **Incremental Loads:** Design flows to handle incremental data loading more effectively. This could involve:
    -   Checking the latest timestamp in the Iceberg table for a given symbol.
    -   Adjusting the `start_date` for the Binance downloader to fetch only new data.
    -   Carefully considering how to handle late-arriving data or corrections from Binance if applicable.
-   **Security:** Securely manage any credentials required for Binance (if API keys were to be used for private endpoints in the future) or the Iceberg catalog (e.g., AWS credentials, database passwords) using appropriate secret management tools like Prefect Blocks or environment variable managers.
-   **Data Partitioning:** Define and implement a partitioning strategy for the Iceberg tables (e.g., by date, symbol) to optimize query performance. This would be part of the Iceberg table creation logic.
-   **Backfills and Replays:** Develop strategies and potentially separate flows for backfilling historical data or replaying ingestion for specific periods if needed.
