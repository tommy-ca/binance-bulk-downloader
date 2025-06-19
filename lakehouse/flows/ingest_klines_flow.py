import os
import pandas as pd
from prefect import flow, task
from datetime import datetime

from binance_bulk_downloader.downloader import BinanceBulkDownloader
# Assuming schemas are in the parent directory relative to this flow's location
from lakehouse.schemas.klines_schema import klines_schema


@task
def fetch_klines_data(symbol: str, interval: str, start_date_str: str, end_date_str: str, download_dir: str, asset_type: str = "spot"):
    """
    Fetches klines data from Binance using BinanceBulkDownloader.
    This downloader gets monthly or daily files. We'll need to filter by date later if needed.
    """
    # The downloader typically gets all data for a symbol/interval or specific monthly/daily files.
    # It doesn't have fine-grained start/end date filtering for fetching,
    # so we download relevant files and then filter records.
    # For simplicity, we'll assume 'daily' files for now.
    # A more robust solution would determine 'daily' or 'monthly' based on date range.

    # Ensure download_dir exists
    os.makedirs(download_dir, exist_ok=True)

    downloader = BinanceBulkDownloader(
        destination_dir=download_dir,
        data_type="klines",
        data_frequency=interval,
        asset=asset_type, # e.g., "spot", "um" (USDT-M Futures), "cm" (COIN-M Futures)
        timeperiod_per_file="daily", # "daily" or "monthly"
        symbols=symbol.upper()
    )

    # The downloader's run_download method fetches all available files based on its configuration.
    # It stores them in subdirectories like:
    # <download_dir>/data/spot/daily/klines/<SYMBOL>/<INTERVAL>/<SYMBOL>-<INTERVAL>-YYYY-MM-DD.csv
    downloader.run_download()

    # Construct the path where files for this symbol and interval would be
    # This is an approximation; the downloader creates a nested structure.
    # We will need to scan the directory for actual downloaded CSV files.

    # Example path structure: data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01-01.csv
    # For now, we'll pass the base download_dir and let the transform task find the files.
    return os.path.join(download_dir, "data", asset_type, "daily", "klines", symbol.upper(), interval)

@task
def transform_klines_data(csv_files_path: str, symbol: str, interval: str, start_datetime: datetime, end_datetime: datetime) -> list[dict]:
    """
    Reads downloaded CSV klines data, transforms it to the target schema, and filters by date.
    """
    transformed_records = []

    # Define the expected CSV column names based on Binance documentation
    # [Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, Number of trades, Taker buy base asset volume, Taker buy quote asset volume, Ignore]
    column_names = [
        "open_time", "open_price", "high_price", "low_price", "close_price", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ]

    if not os.path.exists(csv_files_path):
        print(f"No data found at path: {csv_files_path}")
        return []

    for filename in os.listdir(csv_files_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(csv_files_path, filename)
            try:
                df = pd.read_csv(file_path, header=None, names=column_names)
            except pd.errors.EmptyDataError:
                print(f"Warning: Empty CSV file found and skipped: {file_path}")
                continue

            for _, row in df.iterrows():
                # Convert open_time and close_time from ms to seconds for TimestampType, then to datetime
                # PyIceberg TimestampType expects microseconds, or datetime objects.
                # Pandas to_datetime with unit='ms' is fine.
                record_open_time = pd.to_datetime(row["open_time"], unit='ms')

                # Filter by date range
                if not (start_datetime <= record_open_time < end_datetime):
                    continue

                transformed_records.append({
                    "open_time": record_open_time, # pd.Timestamp (datetime compatible)
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "open_price": float(row["open_price"]),
                    "high_price": float(row["high_price"]),
                    "low_price": float(row["low_price"]),
                    "close_price": float(row["close_price"]),
                    "volume": float(row["volume"]),
                    "close_time": pd.to_datetime(row["close_time"], unit='ms'), # pd.Timestamp
                    "quote_asset_volume": float(row["quote_asset_volume"]),
                    "number_of_trades": int(row["number_of_trades"]),
                    "taker_buy_base_asset_volume": float(row["taker_buy_base_asset_volume"]),
                    "taker_buy_quote_asset_volume": float(row["taker_buy_quote_asset_volume"]),
                })

    print(f"Transformed {len(transformed_records)} kline records for {symbol} ({interval}) between {start_datetime} and {end_datetime}.")
    return transformed_records

@task
def load_data_to_iceberg(data: list[dict], table_name: str, schema):
    """
    Placeholder task to load data into an Iceberg table.
    Actual implementation would involve:
    1. Configuring an Iceberg catalog (e.g., REST, Hive, Glue).
    2. Creating the Iceberg table if it doesn't exist, using the provided schema.
    3. Appending the data to the table.
    """
    if not data:
        print(f"No data to load into Iceberg table {table_name}.")
        return

    print(f"Attempting to load {len(data)} records into Iceberg table '{table_name}' (Placeholder).")
    # Example of how one might interact with PyIceberg (highly simplified):
    # from pyiceberg.catalog import load_catalog
    # catalog = load_catalog(...) # Needs configuration
    # if not catalog.table_exists(table_name):
    #     catalog.create_table(identifier=table_name, schema=schema)
    # catalog.load_table(table_name).append(data)
    print(f"Data for table '{table_name}' would be written here.")
    print("Sample record:", data[0] if data else "N/A")


@flow(name="Binance Klines Ingestion")
def ingest_binance_klines_flow(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    start_date: str = "2024-01-01", # YYYY-MM-DD
    end_date: str = "2024-01-02",   # YYYY-MM-DD
    asset_type: str = "spot" # "spot", "um", "cm"
):
    """
    Prefect flow to fetch Binance klines data, transform it, and load it into an Iceberg table.
    """
    download_base_dir = "./temp_binance_data" # Base directory for downloads

    # Parse date strings to datetime objects
    # The downloader works with daily/monthly files, so the exact time part of start/end is for filtering
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d") # end_date is exclusive for filtering

    print(f"Starting klines ingestion for {symbol}, interval {interval}, from {start_date} to {end_date}, asset type {asset_type}")

    # 1. Fetch data
    # The path returned is where the downloader *should* place the files.
    # This path is specific to the symbol and interval.
    downloaded_data_path_for_symbol_interval = fetch_klines_data(
        symbol=symbol,
        interval=interval,
        start_date_str=start_date,
        end_date_str=end_date,
        download_dir=os.path.join(download_base_dir, asset_type), # Pass a more specific path to fetch_klines_data
        asset_type=asset_type
    )

    # 2. Transform data
    # The downloaded_data_path_for_symbol_interval is the directory containing CSVs for that specific symbol/interval.
    transformed_data = transform_klines_data(
        csv_files_path=downloaded_data_path_for_symbol_interval,
        symbol=symbol,
        interval=interval,
        start_datetime=start_datetime,
        end_datetime=end_datetime
    )

    # 3. Load data to Iceberg (Placeholder)
    # Table name could be dynamic, e.g., based on asset_type, data_type
    iceberg_table_name = f"lakehouse.binance.{asset_type}_klines"
    load_data_to_iceberg(
        data=transformed_data,
        table_name=iceberg_table_name,
        schema=klines_schema
    )

    print(f"Klines ingestion flow for {symbol} ({interval}) completed.")

if __name__ == "__main__":
    # Example of how to run the flow locally
    # This requires Prefect server to be running, or it will use an ephemeral API.
    # To run with a local server:
    # 1. `prefect server start` in a separate terminal
    # 2. Then run this script.

    # For testing, let's try a very small date range.
    # The downloader will get daily files, so it will fetch the whole day file.
    ingest_binance_klines_flow(
        symbol="BTCUSDT",
        interval="1m",
        start_date="2024-05-01", # Adjust if this date has no data / files are large
        end_date="2024-05-02",   # Fetches data for 2024-05-01
        asset_type="spot"
    )

    # Example for futures:
    # ingest_binance_klines_flow(
    #     symbol="BTCUSDT",
    #     interval="1m",
    #     start_date="2024-05-01",
    #     end_date="2024-05-02",
    #     asset_type="um" # USDT-M futures
    # )
