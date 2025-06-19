from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    StringType,
    DoubleType,
    LongType,
)

# Schema for Binance Klines (Candlestick Data)
# Based on common fields available from the Binance API
# See: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
# Example KLine data:
# [
#   1499040000000,      // Kline open time
#   "0.01634790",       // Open price
#   "0.80000000",       // High price
#   "0.01575800",       // Low price
#   "0.01577100",       // Close price
#   "148976.11427815",  // Volume
#   1499644799999,      // Kline close time
#   "2434.19055334",    // Quote asset volume
#   308,                // Number of trades
#   "1756.87402397",    // Taker buy base asset volume
#   "28.46694368",      // Taker buy quote asset volume
#   "0"                 // Ignore
# ]

klines_schema = Schema(
    # Open time of the kline, in milliseconds since epoch
    (1, "open_time", TimestampType()),
    # Symbol, e.g., BTCUSDT
    (2, "symbol", StringType()),
    # Interval, e.g., 1m, 5m, 1h, 1d
    (3, "interval", StringType()),
    # Open price
    (4, "open_price", DoubleType()),
    # High price
    (5, "high_price", DoubleType()),
    # Low price
    (6, "low_price", DoubleType()),
    # Close price
    (7, "close_price", DoubleType()),
    # Volume of assets traded
    (8, "volume", DoubleType()),
    # Close time of the kline, in milliseconds since epoch
    (9, "close_time", TimestampType()),
    # Volume of the quote asset traded
    (10, "quote_asset_volume", DoubleType()),
    # Number of trades executed during this kline
    (11, "number_of_trades", LongType()),
    # Volume of the base asset bought by takers
    (12, "taker_buy_base_asset_volume", DoubleType()),
    # Volume of the quote asset bought by takers
    (13, "taker_buy_quote_asset_volume", DoubleType()),
    schema_id=1
)

if __name__ == "__main__":
    print("Klines Schema:")
    print(klines_schema)
