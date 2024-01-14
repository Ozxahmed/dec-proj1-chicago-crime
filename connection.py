import requests
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
APP_TOKEN = os.environ.get("X-App-Token")

# initialize parameters
start_date = '2023-11-06T00:00:00.000'
end_date = '2023-11-19T23:59:59.999'
soql_date = f"where=date_of_occurrence between '{start_date}' and '{end_date}'" #9937 records
limit = 1000

response_data = []
i = 0
while True:
    offset = i * limit # offset = 0, 1000, 2000, etc.
    response = requests.get(f"https://data.cityofchicago.org/resource/x2n5-8w5q.json?"
                            f"$$app_token={APP_TOKEN}"
                            f"&$order=:id"  
                            f"&${soql_date}"
                            f"&$limit={limit}"
                            f"&$offset={offset}"
                            f"&$select=:*,*") #include metadata field info

    if not response.status_code==200:
        raise Exception

    if response.json() == []:
        break

    response_data.extend(response.json())
    i += 1

crime_df = pd.json_normalize(data=response_data)

# drop unnecessary columns
crime_df = crime_df.drop(columns=[':@computed_region_awaf_s7ux', ':@computed_region_6mkv_f3dw',
       ':@computed_region_vrxf_vc4k', ':@computed_region_bdys_3d7i',
       ':@computed_region_43wa_7qmu', ':@computed_region_rpca_8um6'])

# create a dataframe for specific dates with holiday column 

begin_date = "2023-01-01"
end_date = "2024-12-31" 
# ^ parameters to be set by yaml config later

date_df = pd.DataFrame({'Date':pd.date_range(start=begin_date, end=end_date)})

date_df['Day'] = date_df['Date'].dt.day
date_df['Month'] = date_df['Date'].dt.month
date_df['MonthName'] = date_df['Date'].dt.month_name()
date_df['Year'] = date_df['Date'].dt.year
date_df['DayOfWeek'] = date_df['Date'].dt.dayofweek
date_df['DayOfWeekName'] = date_df['Date'].dt.day_name()

holidays_df = pd.concat(map(pd.read_csv, ['raw_data/holidays/2023.csv', 'raw_data/holidays/2024.csv']))
# ^ change code above to run as for loop for each file inside the holidays folder

holidays_df = holidays_df.rename(columns={'Name': 'HolidayName'})
holidays_df['Date'] = pd.to_datetime(holidays_df['Date'])

date_merge_df = pd.merge(left=date_df, right=holidays_df, on=["Date"], how="left")