from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType,
    StringType,
    DoubleType,
    TimestampType,
    BooleanType,
)

# Schema for Binance Trades
# Based on common fields available from the Binance API for historical trades
# See: https://binance-docs.github.io/apidocs/spot/en/#old-trade-lookup-market_data
# Example Trade data (for a single trade, actual API might return a list):
# {
#   "id": 28457,
#   "price": "4.00000100",
#   "qty": "12.00000000",
#   "quoteQty": "48.000012",
#   "time": 1499865549590, // Trade execution time
#   "isBuyerMaker": true,
#   "isBestMatch": true
# }
# Note: "isBestMatch" is often included but might not be essential for all analytics.
# "id" here refers to the trade ID.

trades_schema = Schema(
    # Unique trade ID
    (1, "trade_id", LongType()),
    # Symbol, e.g., BTCUSDT
    (2, "symbol", StringType()),
    # Price of the trade
    (3, "price", DoubleType()),
    # Quantity of the base asset traded
    (4, "quantity", DoubleType()),
    # Quantity of the quote asset traded
    (5, "quote_quantity", DoubleType()),
    # Time of the trade, in milliseconds since epoch
    (6, "trade_time", TimestampType()),
    # Boolean indicating if the buyer was the maker
    (7, "is_buyer_maker", BooleanType()),
    # Optional: Boolean indicating if the trade was the best price match at the time
    # (8, "is_best_match", BooleanType()), # Decided to omit for broader applicability
    schema_id=2
)

if __name__ == "__main__":
    print("Trades Schema:")
    print(trades_schema)
