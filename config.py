import os

# Holds config constants and parameters.
class Config:
    BASE_URL = "https://fapi.binance.com"
    OI_UPPER_LIMIT = 12   # We need to get the open interest data at the beginning of a minute
    OI_LOWER_LIMIT = 39   # For example, we'll start getting the data between 14:02:39 and 14:03:12
    DB_WRITE_THRESHOLD = 6 # When we are not getting data, we will process the data and write it to our database
    API_LIMITER = 2000     # Max allowed API requests before pausing. We should not exceed the api weight limit
    GMT_OFFSET = 3          # GMT offset GMT+3, necessary for some string representations of time
    LOG_PATH = os.path.expanduser('~') + "/open_interest.log"
