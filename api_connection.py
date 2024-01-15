# %%
import requests
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable 

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
        ':@computed_region_rpca_8um6',
        'location.latitude',
        'location.longitude',
        'location.human_address'])
    
    crime_df = crime_df.rename(columns={
        ":id": "id",
        ":created_at": "created_at",
        ":updated_at": "updated_at",
        ":version": "version",
        "case_": "case",
        "_iucr": "iucr",
        "_primary_decsription": "primary_description",
        "_secondary_description": "secondary_description",
        "_location_description": "location_description"
    })
    
    return crime_df

def load_data_to_parquet(start_date:str, end_date:str, crime_df:pd.DataFrame) -> None:
    """ 
    Save crime data as parquet file (include start_date and end_date in filename)
    """
    start_date_str = start_date.replace(":","-").replace(".","-")
    end_date_str = end_date.replace(":","-").replace(".","-")
    crime_df.to_parquet(f"data/crime_{start_date_str}_{end_date_str}.parquet", index=False)

def generate_date_df(begin_date:str, end_date:str, holidays_data_path:list[str]) -> pd.DataFrame:
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

    holidays_df = pd.concat(map(pd.read_csv, holidays_data_path))

    holidays_df = holidays_df.rename(columns={'Name': 'HolidayName'})
    holidays_df['Date'] = pd.to_datetime(holidays_df['Date'])

    return pd.merge(left=date_df, right=holidays_df, on=["Date"], how="left")

# Initialize environment variables and parameters
load_dotenv()
APP_TOKEN = os.environ.get("APP_TOKEN")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SERVER_NAME = os.environ.get("SERVER_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
PORT = os.environ.get("PORT")

start_date = '2023-11-06T00:00:00.000'
end_date = '2023-11-19T23:59:59.999'
limit = 1000
holidays_begin_date = "2023-01-01"
holidays_end_date = "2024-12-31" 
holidays_data_path = ['raw_data/holidays/2023.csv', 'raw_data/holidays/2024.csv']

crime_df = extract_api_data(APP_TOKEN=APP_TOKEN, start_date=start_date, end_date=end_date, limit=limit)
load_data_to_parquet(start_date=start_date, end_date=end_date, crime_df=crime_df)
dates_df = generate_date_df(begin_date=holidays_begin_date, end_date=holidays_end_date, holidays_data_path=holidays_data_path)

connection_url = URL.create(
    drivername = "postgresql+pg8000", 
    username = DB_USERNAME,
    password = DB_PASSWORD,
    host = SERVER_NAME, 
    port = 5432,
    database = DATABASE_NAME, 
)
engine = create_engine(connection_url)

meta = MetaData()
crime_table = Table(
    "crimes", meta, 
    Column('id',String,primary_key=True),
    Column('created_at',String),
    Column('updated_at',String),
    Column('version',String),
    Column('case',String),
    Column('date_of_occurrence',String),
    Column('block',String),
    Column('iucr',String),
    Column('primary_description',String),
    Column('secondary_description',String),
    Column('location_description',String),
    Column('arrest',String),
    Column('domestic',String),
    Column('beat',String),
    Column('ward',String),
    Column('fbi_cd',String),
    Column('x_coordinate',String),
    Column('y_coordinate',String),
    Column('latitude',String),
    Column('longitude',String)
)
meta.create_all(bind=engine)

chunksize = 1000
data = crime_df.to_dict(orient='records') # type list[dict]
max_length = len(data)

key_columns = [pk_column.name for pk_column in crime_table.primary_key.columns.values()]

for i in range(0, max_length, chunksize):
    if i + chunksize >= max_length:
        lower_bound = i
        upper_bound = max_length
    else:
        lower_bound = i
        upper_bound = i + chunksize
    insert_statement = postgresql.insert(crime_table).values(
        data[lower_bound:upper_bound]
    )
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={
            c.key: c for c in insert_statement.excluded if c.key not in key_columns
        },
    )
    engine.execute(upsert_statement)
