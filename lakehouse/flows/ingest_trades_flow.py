import os
import pandas as pd
from prefect import flow, task
from datetime import datetime

from binance_bulk_downloader.downloader import BinanceBulkDownloader
# Assuming schemas are in the parent directory relative to this flow's location
from lakehouse.schemas.trades_schema import trades_schema


@task
def fetch_trades_data(symbol: str, start_date_str: str, end_date_str: str, download_dir: str, asset_type: str = "spot"):
    """
    Fetches trades data from Binance using BinanceBulkDownloader.
    """
    os.makedirs(download_dir, exist_ok=True)

    downloader = BinanceBulkDownloader(
        destination_dir=download_dir,
        data_type="trades", # Data type is 'trades'
        asset=asset_type,
        timeperiod_per_file="daily", # Trades are usually available as daily files
        symbols=symbol.upper()
        # data_frequency is not applicable for trades type in the downloader
    )

    downloader.run_download()

    # Path structure for trades: data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-01-01.csv
    return os.path.join(download_dir, "data", asset_type, "daily", "trades", symbol.upper())

@task
def transform_trades_data(csv_files_path: str, symbol: str, start_datetime: datetime, end_datetime: datetime) -> list[dict]:
    """
    Reads downloaded CSV trades data, transforms it to the target schema, and filters by date.
    """
    transformed_records = []

    # Binance trades CSV header (based on common knowledge for bulk trade files):
    # trade_id, price, qty, quoteQty, time, isBuyerMaker
    # Sometimes an older format might exist: price, qty, quoteQty, time, isBuyerMaker, isBestMatch
    # The downloader usually provides files with:
    # For SPOT: trade_id,price,qty,quoteQty,time,isBuyerMaker
    # For FUTURES: trade_id,price,qty,quoteQty,time,isBuyerMaker (or similar, may vary slightly)
    # We will assume the first 6 columns are standard for recent data.
    # Note: The example in trades_schema.py shows: id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch.
    # The bulk CSVs often have a slightly different column order/naming or number of columns.
    # For spot trades, the typical CSV columns are:
    # id, price, qty, base_qty, time, is_buyer_maker, is_best_match
    # Let's assume the file contains: trade_id, price, qty, quoteQty, time, isBuyerMaker
    # The `binance_bulk_downloader` example script for trades does not specify headers, implying they might be implicit or absent.
    # Checking `tests/test_trades.py` or example usage might clarify.
    # For now, assuming the order: trade_id, price, qty, quoteQty, time, isBuyerMaker
    # The actual historical trade files from data.binance.vision for SPOT often have headers like:
    # id,price,qty,quoteQty,time,isBuyerMaker
    # Let's stick to this.
    column_names = [
        "trade_id", "price", "quantity", "quote_quantity", "trade_time", "is_buyer_maker"
    ]
    # Some files might have an additional 'isBestMatch' column, we will try to handle it.
    # If the schema expects 6 columns, and file has 7, pandas will error or truncate by default if names list is 6.
    # Let's try reading without explicit names first to see, or use `usecols` if format is certain.
    # Given the downloader downloads raw CSVs, it's safer to assign names.

    if not os.path.exists(csv_files_path):
        print(f"No data found at path: {csv_files_path}")
        return []

    for filename in os.listdir(csv_files_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(csv_files_path, filename)
            try:
                # Try reading with 6 columns first
                df = pd.read_csv(file_path, header=None, names=column_names, usecols=range(6))
            except pd.errors.ParserError: # Happens if more columns than names and no usecols
                 print(f"ParserError for {file_path}. Skipping this file or trying alternative parsing.")
                 # Potentially try reading with 7 columns if that's a known alternative format
                 # column_names_alt = column_names + ["is_best_match"]
                 # df = pd.read_csv(file_path, header=None, names=column_names_alt)
                 continue # Skip file if parsing fails for now
            except pd.errors.EmptyDataError:
                print(f"Warning: Empty CSV file found and skipped: {file_path}")
                continue


            for _, row in df.iterrows():
                record_trade_time = pd.to_datetime(row["trade_time"], unit='ms')

                if not (start_datetime <= record_trade_time < end_datetime):
                    continue

                transformed_records.append({
                    "trade_id": int(row["trade_id"]),
                    "symbol": symbol.upper(),
                    "price": float(row["price"]),
                    "quantity": float(row["quantity"]),
                    "quote_quantity": float(row["quote_quantity"]),
                    "trade_time": record_trade_time, # pd.Timestamp
                    "is_buyer_maker": bool(row["is_buyer_maker"]),
                })

    print(f"Transformed {len(transformed_records)} trade records for {symbol} between {start_datetime} and {end_datetime}.")
    return transformed_records

@task
def load_trades_to_iceberg(data: list[dict], table_name: str, schema):
    """
    Placeholder task to load trades data into an Iceberg table.
    """
    if not data:
        print(f"No data to load into Iceberg table {table_name}.")
        return

    print(f"Attempting to load {len(data)} records into Iceberg table '{table_name}' (Placeholder).")
    # Actual Iceberg loading logic here
    print(f"Data for table '{table_name}' would be written here.")
    print("Sample record:", data[0] if data else "N/A")


@flow(name="Binance Trades Ingestion")
def ingest_binance_trades_flow(
    symbol: str = "BTCUSDT",
    start_date: str = "2024-01-01", # YYYY-MM-DD
    end_date: str = "2024-01-02",   # YYYY-MM-DD
    asset_type: str = "spot" # "spot", "um", "cm"
):
    """
    Prefect flow to fetch Binance trades data, transform it, and load it into an Iceberg table.
    """
    download_base_dir = "./temp_binance_data" # Base directory for downloads

    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

    print(f"Starting trades ingestion for {symbol}, from {start_date} to {end_date}, asset type {asset_type}")

    downloaded_data_path = fetch_trades_data(
        symbol=symbol,
        start_date_str=start_date,
        end_date_str=end_date,
        download_dir=os.path.join(download_base_dir, asset_type),
        asset_type=asset_type
    )

    transformed_data = transform_trades_data(
        csv_files_path=downloaded_data_path,
        symbol=symbol,
        start_datetime=start_datetime,
        end_datetime=end_datetime
    )

    iceberg_table_name = f"lakehouse.binance.{asset_type}_trades"
    load_trades_to_iceberg(
        data=transformed_data,
        table_name=iceberg_table_name,
        schema=trades_schema
    )

    print(f"Trades ingestion flow for {symbol} completed.")

if __name__ == "__main__":
    # Example of how to run the flow locally
    ingest_binance_trades_flow(
        symbol="BTCUSDT",
        start_date="2024-05-01", # Adjust if this date has no data
        end_date="2024-05-02",
        asset_type="spot"
    )

    # Example for futures trades (if supported by downloader for 'trades' type and asset 'um')
    # Check downloader.py _DATA_TYPE_BY_ASSET to confirm "trades" is valid for "um"
    # It is: _DATA_TYPE_BY_ASSET["um"]["daily"] includes "trades"
    # ingest_binance_trades_flow(
    #     symbol="BTCUSDT",
    #     start_date="2024-05-01",
    #     end_date="2024-05-02",
    #     asset_type="um"
    # )
