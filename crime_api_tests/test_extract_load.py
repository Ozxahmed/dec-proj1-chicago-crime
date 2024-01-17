
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import datetime

# Initialize parameters
load_dotenv()
APP_TOKEN = os.environ.get("X-App-Token")
#start_date = '2023-11-06T00:00:00.000'
#end_date = '2023-11-19T23:59:59.999'
limit = 1000



def get_startend_dates(APP_TOKEN:str):
    """ 
    Get the inital start date for the back fill with min(date_of_occurance) 
    create first end date with dateoffset of 2 weeks (14 days)

    """
    response = requests.get(f"https://data.cityofchicago.org/resource/x2n5-8w5q.json?"
                            f"$$app_token={APP_TOKEN}&"
                            f"&$select=min(date_of_occurrence)"
                            )

    # print the message
    min_response = response.json()
    min_date = min_response[0]['min_date_of_occurrence']
    #print(type(min_date))
    
    start_date = pd.to_datetime(min_date)
    end_date = start_date + pd.DateOffset(days=14)

    assert response.status_code == 200

    #Adds T back into datetime to match request from api
    start_date = str(start_date.date()) +"T" + str(start_date.timetz())
    end_date = str(end_date.date()) +"T" + str(end_date.timetz())

    return start_date, end_date


def update_startend_dates(last_end_date:str):
    """ 
    Update the inital start date for the back fill with provided start date 
    update end date with dateoffset of 2 weeks (14 days)
    Used to iterate through full dataset 

    """
    
    start_date = pd.to_datetime(last_end_date)
    end_date = start_date + pd.DateOffset(days=14)
    
    #Adds T back into datetime to match request from api
    start_date = str(start_date.date()) +"T" + str(start_date.timetz())
    end_date = str(end_date.date()) +"T" + str(end_date.timetz()) 

    return start_date, end_date



def extract_api_data(APP_TOKEN:str, start_date:str, end_date:str, limit:int) -> pd.DataFrame:
    """
    Extract data from API
    """
    soql_date = f"where=date_of_occurrence between '{start_date}' and '{end_date}'" #9937 records
    print(soql_date)
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
            print("More than 1000")
            #raise Exception

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


#Get the initial start and end dates to begin backfill process
start_date, end_date = get_startend_dates(APP_TOKEN=APP_TOKEN)
print("First staring date: ", start_date)
print("First ending date: ", end_date)

# returns current date and time
now = pd.Timestamp.now() 
today_date = str(now.date()) +"T" + str(now.timetz()) 

def backfill_database(start_date, end_date, today_date):
    while end_date < today_date:
        """
        Extract the first set of crimes with initial start and end dates
        """
        crime_df = extract_api_data(APP_TOKEN=APP_TOKEN, start_date=start_date, end_date=end_date, limit=limit)
        print(len(crime_df))

        """
        While end date is smaller than today, keep updating the start and end dates
        """
        start_date, end_date = update_startend_dates(end_date)
        print(start_date, ", ", end_date)

        #load to parquet 

    return 

success = backfill_database(start_date, end_date, today_date)

#crime_df = extract_api_data(APP_TOKEN=APP_TOKEN, start_date=start_date, end_date=end_date, limit=limit)
#print(crime_df.head())
#load_data_to_parquet(start_date=start_date, end_date=end_date, crime_df=crime_df)
#dates_df = generate_date_df(begin_date=holidays_begin_date, end_date=holidays_end_date, csv_file_paths=holidays_data_path)

