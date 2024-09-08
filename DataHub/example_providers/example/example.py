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
return a dataframe with random data

Params:
    rows - number of rows of random data
    cols - number of columns of random data

CSV Output
Example Query: &output=csv&rows=5&cols=1

HTML Output
Example Query: &output=html&rows=5&cols=1

Example Output
    ,0,pid,time
    0,486,2689007,1710093848.8563957
    1,598,2689007,1710093848.8563957
    2,93,2689007,1710093848.8563957
    3,317,2689007,1710093848.8563957
    4,416,2689007,1710093848.8563957
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
random_data_date

CSV Output
Example Query: &output=csv&start_date=2024-08-01&end_date=2024-08-10

HTML Output
Example Query: &output=html&start_date'2024-08-01&end_date=2024-08-10    
    """
    df = random_data(3,3)
    df['start_date'] = pd.to_datetime(start_date)
    df['end_date'] = pd.to_datetime(end_date)
    return df


# add 7 hours becase Los Angeles is 7 hours behind UTC time, in reality the lag will be 1 day
@cacheable(cache_dir='/tmp', filename=__file__, lag_params=['start_date', 'end_date'], lag_from_utc_now=datetime.timedelta(days=1, hours=7))
def random_data_date_cached(start_date, end_date):
    """
random_data_date_cached

CSV Output
Example Query: &output=csv&start_date=2024-08-01&end_date=2024-08-10

HTML Output
Example Query: &output=html&start_date=2024-08-01&end_date=2024-08-10
    """
    return random_data_date(start_date, end_date)
