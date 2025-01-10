# --------------------------------------------------
#    Imports
# --------------------------------------------------
import configparser
import datetime
import os
import random
import time
import pandas as pd
from DataHub.cache import cacheable


# --------------------------------------------------
#    CONFIG FILE
# --------------------------------------------------
CONFIG_FILE = '/etc/datamodule_example.conf'
CONFIG = None
if os.path.isfile(CONFIG_FILE):
    CONFIG = configparser.ConfigParser()
    CONFIG.read(CONFIG_FILE)


# --------------------------------------------------
#    Queries
# --------------------------------------------------
def random_data(rows, cols):
    """
Generate a DataFrame with random data.

This function creates a DataFrame with the specified number of rows and columns,
filled with random integers. Additional columns include the process ID (`pid`)
and the current timestamp (`time`).

Params:
    rows - (int) The number of rows of random data to generate.
    cols - (int) The number of columns of random data to generate.

CSV Output:
Example Query: &output=csv&rows=5&cols=3

HTML Output:
Example Query: &output=html&rows=5&cols=3

Example Output:
       ,0,1,2,pid,time
    0,486,268,900,1710093848.8563957
    1,598,423,345,1710093848.8563957
    2,93,759,981,1710093848.8563957
    3,102,487,346,1710093848.8563957
    4,317,592,478,1710093848.8563957

Additional Notes:
    - Reads secrets from the configuration file if available.
    - Logs process ID to demonstrate parallel processing capability.
    """
    # read a secret example
    # example of secrets file at /etc/datamodule_example.conf
    #    ['Secrets']
    #        Test=abcd
    if CONFIG is not None:
        secret = CONFIG['Secrets']['Test']
        print(f'secret is {secret}')
    else:
        print(f'missing config file at {CONFIG_FILE}')

    cols = int(cols)
    rows = int(rows)
    data = []
    for _ in range(0, rows):
        r = []
        for _ in range(0, cols):
            r.append(random.randint(0, 1000))
        data.append(r)

    # convert to dataframe
    df = pd.DataFrame(data)

    # show process id to prove we are running in different processes
    df['pid'] = os.getpid()

    # pretend this takes a long time
    df['time'] = str(time.time())

    # success!
    return df


def random_data_date(start_date, end_date):
    """
Generate random data with dates.

This function creates a DataFrame containing random data along with the specified
start_date and end_date as additional columns.

Params:
    start_date - (str) The start of the date range. Should be in YYYY-MM-DD format.
    end_date - (str) The end of the date range. Should be in YYYY-MM-DD format.

CSV Output:
Example Query: &output=csv&start_date=2024-08-01&end_date=2024-08-10

HTML Output:
Example Query: &output=html&start_date=2024-08-01&end_date=2024-08-10

Example Output:
       ,0,1,2,pid,time,start_date,end_date
    0,780,103,720,579429,1736461744.2224698,2024-08-01,2024-08-10
    1,437,521,109,579429,1736461744.2224698,2024-08-01,2024-08-10
    2,153,943,313,579429,1736461744.2224698,2024-08-01,2024-08-10
    """
    df = random_data(3,3)
    df['start_date'] = pd.to_datetime(start_date)
    df['end_date'] = pd.to_datetime(end_date)
    return df


# add 7 hours becase Los Angeles is 7 hours behind UTC time, in reality the lag will be 1 day
@cacheable(cache_dir='/tmp', filename=__file__, lag_params=['start_date', 'end_date'], lag_from_utc_now=datetime.timedelta(days=1, hours=7))
def generate_date_dataframe(start_date, end_date):
    """
Generate a DataFrame with all dates between start_date and end_date.
Each row contains:
    - date
    - number of days since epoch

Params:
    start_date - (str) The start of the date range (inclusive). Should be in YYYY-MM-DD format.
    end_date - (str) The end of the date range (inclusive). Should be in YYYY-MM-DD format.

CSV Output:
Example Query: &output=csv&start_date=2024-08-01&end_date=2024-08-05

HTML Output:
Example Query: &output=html&start_date=2024-08-01&end_date=2024-08-05

Example Output:
                  date  days_since_epoch
      0 2024-08-01             19723
      1 2024-08-02             19724
      2 2024-08-03             19725
      3 2024-08-04             19726
      4 2024-08-05             19727
    """
    # Convert input strings to datetime objects
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Generate a range of dates
    date_range = pd.date_range(start=start_date, end=end_date)

    # Create the DataFrame
    epoch = datetime.datetime(1970, 1, 1)
    df = pd.DataFrame({
        'date': date_range,
        'days_since_epoch': [(date - epoch).days for date in date_range]
    })

    return df