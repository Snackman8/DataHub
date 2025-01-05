# --------------------------------------------------
#    Imports
# --------------------------------------------------
import pandas as pd
import random
from datetime import datetime, timedelta
import logging
from cache import cacheable


# --------------------------------------------------
#    Constants
# --------------------------------------------------
# Cache configuration
CACHE_DIR = "/tmp/stock_cache"


# --------------------------------------------------
#    CONFIG FILE
# --------------------------------------------------
CONFIG_FILE = '/etc/datamodule_example_caching.conf'
CONFIG = None
if os.path.isfile(CONFIG_FILE):
    CONFIG = configparser.ConfigParser()
    CONFIG.read(CONFIG_FILE)


# --------------------------------------------------
#    Queries
# --------------------------------------------------
@cacheable(cache_dir=CACHE_DIR, filename=__file__, lag_params=["end_date"], lag_from_utc_now=timedelta(days=1))
def fetch_stock_prices(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
Fetch fake stock prices for a given ticker and date range.

Params:
    ticker (str) - Stock ticker symbol (e.g., "AAPL", "GOOG").
    start_date (str) - Start date in 'YYYY-MM-DD' format.
    end_date (str) - End date in 'YYYY-MM-DD' format.

CSV Output
Example Query: &output=csv&ticker=AAPL&start_date=2024-01-01&end_date=2024-01-15

HTML Output
Example Query: &output=html&ticker=AAPL&start_date=2024-01-01&end_date=2024-01-15

Example Output
    """
    logging.info(f"Fetching stock prices for {ticker} from {start_date} to {end_date}.")

    # Simulate fetching data
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    data = []

    while current <= end:
        price = round(random.uniform(100, 500), 2)  # Simulated stock price
        data.append({"date": current.strftime("%Y-%m-%d"), "ticker": ticker, "price": price})
        current += timedelta(days=1)

    df = pd.DataFrame(data)
    logging.info(f"Generated {len(df)} rows of data for {ticker}.")
    return df
