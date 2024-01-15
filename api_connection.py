import requests
import pandas as pd
from dotenv import load_dotenv
import os

def extract_api_data(APP_TOKEN:str, start_date:str, end_date:str, limit:int) -> pd.DataFrame:
    """
    Extract data from API
    """
    soql_date = f"where=date_of_occurrence between '{start_date}' and '{end_date}'" #9937 records
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

        if i >= 1000:
            raise Exception

        if response.json() == []:
            break

        response_data.extend(response.json())
        i += 1

    crime_df = pd.json_normalize(data=response_data)

    # drop unnecessary columns
    crime_df = crime_df.drop(columns=[
        ':@computed_region_awaf_s7ux', 
        ':@computed_region_6mkv_f3dw',
        ':@computed_region_vrxf_vc4k', 
        ':@computed_region_bdys_3d7i',
        ':@computed_region_43wa_7qmu', 
        ':@computed_region_rpca_8um6'])
    
    return crime_df

def load_data_to_parquet(start_date:str, end_date:str, crime_df:pd.DataFrame) -> None:
    """ 
    Save crime data as parquet file (include start_date and end_date in filename)
    """
    start_date_str = start_date.replace(":","-").replace(".","-")
    end_date_str = end_date.replace(":","-").replace(".","-")
    crime_df.to_parquet(f"data/crime_{start_date_str}_{end_date_str}.parquet", index=False)

def generate_date_df(begin_date:str, end_date:str, csv_file_paths: list[str]) -> pd.DataFrame:
    """
    Create a dataframe for specific dates with holiday column
    """ 
    # ^ parameters to be set by yaml config later

    date_df = pd.DataFrame({'Date':pd.date_range(start=begin_date, end=end_date)})

    date_df['Day'] = date_df['Date'].dt.day
    date_df['Month'] = date_df['Date'].dt.month
    date_df['MonthName'] = date_df['Date'].dt.month_name()
    date_df['Year'] = date_df['Date'].dt.year
    date_df['DayOfWeek'] = date_df['Date'].dt.dayofweek
    date_df['DayOfWeekName'] = date_df['Date'].dt.day_name()

    holidays_df = pd.concat(map(pd.read_csv, csv_file_paths))

    holidays_df = holidays_df.rename(columns={'Name': 'HolidayName'})
    holidays_df['Date'] = pd.to_datetime(holidays_df['Date'])

    return pd.merge(left=date_df, right=holidays_df, on=["Date"], how="left")

# Initialize parameters
load_dotenv()
APP_TOKEN = os.environ.get("X-App-Token")
start_date = '2023-11-06T00:00:00.000'
end_date = '2023-11-19T23:59:59.999'
limit = 1000
holidays_begin_date = "2023-01-01"
holidays_end_date = "2024-12-31" 
holidays_data_path = ['raw_data/holidays/2023.csv', 'raw_data/holidays/2024.csv']

crime_df = extract_api_data(APP_TOKEN=APP_TOKEN, start_date=start_date, end_date=end_date, limit=limit)
load_data_to_parquet(start_date=start_date, end_date=end_date, crime_df=crime_df)
dates_df = generate_date_df(begin_date=holidays_begin_date, end_date=holidays_end_date, csv_file_paths=holidays_data_path)

