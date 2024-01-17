
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

# returns current date and time -- used to increment start and end date
now = pd.Timestamp.now() 
today_date = str(now.date()) +"T" + str(now.timetz()) 

#imports used to for postgres load
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable 
from sqlalchemy import inspect


DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SERVER_NAME = os.environ.get("SERVER_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
PORT = os.environ.get("PORT")


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



def connect_to_postgres():   
# create connection to database 
    connection_url = URL.create(
        drivername = "postgresql+pg8000", 
        username = DB_USERNAME,
        password = DB_PASSWORD,
        host = SERVER_NAME, 
        port = PORT,
        database = DATABASE_NAME, 
    )

    engine = create_engine(connection_url)
    return engine


def load_crime_data(engine, crime_df):
    """
    Load data from API into postgres
    """
    meta = MetaData()
    test_crime_table = Table(
        "raw_crimes", meta, 
        Column(':id', String, primary_key=True), 
        Column(':created_at', String), 
        Column(':updated_at', String), 
        Column(':version', String), 
        Column('case_', String),
        Column('date_of_occurrence', String),
        Column('block', String),
        Column('_iucr', String),
        Column('_primary_decsription', String),
        Column('_secondary_description', String),
        Column('_location_description', String), 
        Column('arrest', String), 
        Column('domestic', String),
        Column('beat', String),
        Column('ward', String), 
        Column('fbi_cd', String),
        Column('x_coordinate', String),
        Column('y_coordinate', String), 
        Column('latitude', String),
        Column('longitude', String),
        Column('location.latitude', String), 
        Column('location.longitude', String),
        Column('location.human_address', String)
    )
    meta.create_all(engine) # creates table if it does not exist

    chunksize = 1000

    data = crime_df.to_dict(orient='records')
    max_length = len(data)

    key_columns = [pk_column.name for pk_column in test_crime_table.primary_key.columns.values()]

    for i in range(0, max_length, chunksize):
        if i + chunksize >= max_length:
            lower_bound = i
            upper_bound = max_length
        else:
            lower_bound = i
            upper_bound = i + chunksize
        insert_statement = postgresql.insert(test_crime_table).values(
            data[lower_bound:upper_bound]
        )
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        engine.execute(upsert_statement)


engine = connect_to_postgres()
print("connected to postgres")
inspector = inspect(engine)
print("check to see if empty")
# Get table information and check to see if database is empty
# if "raw_crimes" not in inspector.get_table_names():
#     print("Table doesn't exist")
    
# else:
#     print(inspector.get_table_names())

#Get the initial start and end dates to begin backfill process
start_date, end_date = get_startend_dates(APP_TOKEN=APP_TOKEN)
print("First staring date: ", start_date)
print("First ending date: ", end_date)


#def backfill_database(start_date, end_date, today_date):
while end_date < today_date:
    """
    Extract the first set of crimes with initial start and end dates
    """
    crime_df = extract_api_data(APP_TOKEN=APP_TOKEN, start_date=start_date, end_date=end_date, limit=limit)
    print(len(crime_df))

    load_crime_data(engine, crime_df)
    """
    While end date is smaller than today, keep updating the start and end dates
    """
    start_date, end_date = update_startend_dates(end_date)
    print(start_date, ", ", end_date)

    #load to parquet 

#return 

#success = backfill_database(start_date, end_date, today_date)

#crime_df = extract_api_data(APP_TOKEN=APP_TOKEN, start_date=start_date, end_date=end_date, limit=limit)
#print(crime_df.head())
#load_data_to_parquet(start_date=start_date, end_date=end_date, crime_df=crime_df)
#dates_df = generate_date_df(begin_date=holidays_begin_date, end_date=holidays_end_date, csv_file_paths=holidays_data_path)

