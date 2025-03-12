print("Start webscraping")
print("1. API: ENTSO-E")
#
# ENTSO-E API 
# 
# Use private API_TOKEN for ENTSO-E web page
import os
API_TOKEN = os.getenv('ENTSOE_API_TOKEN')
# import entsoe module to use the python client for ENTSO-E API
# EntsoePandasClient returns data parsed as a Pandas Series or DataFrame
from entsoe import EntsoePandasClient
client_enstoe = EntsoePandasClient(api_key = API_TOKEN)

# define start and end date for API call
import pandas as pd
from datetime import datetime, timezone
from datetime import timedelta
# to get a full day of data, choose yesterday 
yesterday_original = datetime.now(timezone.utc).date() - timedelta(days = 1) 
yesterday = yesterday_original.strftime("%Y%m%d")
start = pd.Timestamp(yesterday, tz = "UTC")
end = pd.Timestamp(yesterday+'T2359', tz = "UTC")
# choose Germany
country_code = "DE"
# method returns wind and solar forecast
response_entsoe = client_enstoe.query_wind_and_solar_forecast(
    country_code = country_code,
    start = start, end = end,
    psr_type = None
)
# transform index to column
df_entsoe = response_entsoe.reset_index(names = "timestamp")
# transform data frame to long format
df_entsoe_long = pd.melt(df_entsoe,
                  id_vars = ["timestamp"], 
                  value_vars = df_entsoe.columns[1:])
df_entsoe["unit"] = "MW"
print(df_entsoe.head())
# load package
import h5py

def create_or_open_hdf5(filename):
    """
    Create a new HDF5 file if it doesn't exist, or open it in
    append mode if it does.
    
    Args:
    filename (str): The name of the HDF5 file
    
    Returns:
    h5py.File: The opened HDF5 file object
    """
    if not os.path.exists(filename):
        print(f"Creating new HDF5 file: {filename}")
        return h5py.File(filename, 'w')
    else:
        print(f"Opening existing HDF5 file: {filename}")
        return h5py.File(filename, 'a')
# use function
filename = "api_results.h5"
create_or_open_hdf5(filename)    
print("write ENTSOE data to HDF5 file")
# write dataframe to HDF5 file with the key entsoe
df_entsoe_long.to_hdf("api_results.h5", 
          key = f"/day{yesterday}/entsoe", 
          mode = "a")
# 
# DWD API
#
print("2. API: DWD")
import requests
product_code = "OBS_DEU_PT10M_RAD-G" # 10-min measurement global uv radiation in J/cm²
product_description = "Global UV Radiation"
location = 1975 # Hamburg
url = f"https://cdc.dwd.de/geoserver/CDC/ows?Service=WFS&Version=2.0.0&"
url += f"Request=GetFeature&TypeNames=CDC:{product_code}"
url += f"&resulttype=results&OutputFormat=application/json&Cql_filter=SDO_CODE={location}%20"
url += f"AND ZEITSTEMPEL%20DURING%20{yesterday_original}T00:00:00Z/P1D"

payload = {}
headers = {}
response_dwd = requests.request("GET", 
                            url, 
                            headers = headers, 
                            data = payload)

data_dict_dwd = response_dwd.json()
data_features_dwd = data_dict_dwd["features"]
data_features_dwd = pd.json_normalize(data_features_dwd)
# Select only columns with timestamp and value and unit
df_dwd = data_features_dwd[['properties.ZEITSTEMPEL',
                    'properties.WERT', 
                    'properties.EINHEIT'
                    ]]
df_dwd = df_dwd.rename(columns = {'properties.ZEITSTEMPEL':'timestamp', 
                    'properties.WERT':'value', 
                    'properties.EINHEIT':'unit'})
df_dwd["variable"] = product_description
# transform column timestamp to datetime format
df_dwd["timestamp"] = pd.to_datetime(df_dwd["timestamp"])

# append df to HDF5 file
print("Write DWD data to HDF5 file.")
print(df_dwd.head())
df_dwd.to_hdf("api_results.h5", 
           key = f"/day{yesterday}/dwd", 
           mode = "a")

#
# open-meteo API
#
print("3. API: open-meteo")
import openmeteo_requests
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
client_openmeteo = openmeteo_requests.Client(session = retry_session)
# location coordinates of Sylt, Germany
lat_sylt = 54.8833
long_sylt = 8.35
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": lat_sylt,
	"longitude": long_sylt,
	"hourly": ["wind_speed_10m"],
	"timezone": "UTC",
	"past_days": 1,
	"forecast_days": 0
}
responses_openmeteo = client_openmeteo.weather_api(url, 
                                         params = params)
response_openmeteo = responses_openmeteo[0]

# Process hourly data.
hourly = response_openmeteo.Hourly()
hourly_wind_speed_10m = hourly.Variables(0).ValuesAsNumpy()

hourly_data_openmeteo = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data_openmeteo["wind_speed_10m"] = hourly_wind_speed_10m
df_openmeteo = pd.DataFrame(data = hourly_data_openmeteo).rename(
    columns = {"date": "timestamp",
             "wind_speed_10m": "value"})
df_openmeteo["unit"] = "km/h"
df_openmeteo["variable"] = "Windspeed 10 m"
df_openmeteo["timestamp"] = pd.to_datetime(df_openmeteo["timestamp"], utc = True)

# append df to HDF5 file
print("Write open-meteo data to HDF5 file.")
print(df_openmeteo.head())
df_openmeteo.to_hdf("api_results.h5", 
           key = f"/day{yesterday}/open_meteo", 
           mode = "a")
#
# check all keys and groups
#
print("Check all groups and datasets:")
def print_all_items(name, obj):
    if isinstance(obj, h5py.Group):
        print(name)
        print("  (Group)")

with h5py.File('api_results.h5', 'r') as f:
    f.visititems(print_all_items)

