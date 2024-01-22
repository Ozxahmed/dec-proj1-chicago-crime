import requests
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Table, Column, String, Integer, Float, DateTime, Date, MetaData, inspect
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine.base import Engine
from datetime import datetime, timedelta, timezone
import schedule
import time
import logging

class PipelineLogging:
    """
    Creates logging object with specific format and file name to log pipeline run. 

    Usage example:
        PipelineLogging(pipeline_name="Chicago Crime ETL", log_folder_path="./logs")

    Args:
        pipeline_name: provide a str indicating preferred name for pipeline.
        log_folder_path: provide a str indicating the path of the folder to which log files will be written.
    """
    def __init__(self, pipeline_name: str, log_folder_path: str):
        self.pipeline_name = pipeline_name
        self.log_folder_path = log_folder_path
        logger = logging.getLogger(pipeline_name)
        logger.setLevel(logging.INFO)
        self.file_path = (
            f"{self.log_folder_path}/{time.time()}.log" 
        )
        file_handler = logging.FileHandler(self.file_path)
        file_handler.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        self.logger = logger

    def get_logs(self) -> str:
        """
        Returns contents of log file as str object.
        """
        with open(self.file_path, "r") as file:
            return "".join(file.readlines())

def _generate_date_ranges(start_date:str, end_date:str, days_delta:int) -> list[dict[str, str]]:
    """
    Generates a list of date ranges with start and end dates included.

    Usage example:
        _generate_date_ranges(start_date="2023-10-01T00:00:00.000", end_date="2023-10-14T23:59:59.999", days_delta=7)

    Returns:
        A list of dictionaries with date ranges in str format with the following structure:
        ```
            [
                {'start_time': '2023-10-01T00:00:00.000', 'end_time': '2023-10-07T23:59:59.999'},
                {'start_time': '2023-10-08T00:00:00.000', 'end_time': '2023-10-14T23:59:59.999'}
            ]
        ```

    Args:
        start_date: provide a str with the format "yyyy-mm-ddThh:mm:ss.sss".
        end_date: provide a str with the format "yyyy-mm-ddThh:mm:ss.sss".
        days_delta: provide an int for number of days to separate date ranges. 
    """

    date_ranges = []
    raw_start_time = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S.%f')
    raw_end_time = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S.%f')
    
    while raw_start_time < raw_end_time:
        if raw_start_time + timedelta(days=days_delta) >= raw_end_time:
            date_ranges.append({
                'start_time': raw_start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
                'end_time': raw_end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
            })
        else:
            date_ranges.append({
                'start_time': raw_start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
                'end_time': (raw_start_time + timedelta(days=days_delta) - timedelta(milliseconds=1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] # ensures end_time does not overlap with next start_time
            })
        raw_start_time += timedelta(days=days_delta) 

    return date_ranges

def get_min_date_crime_api(APP_TOKEN:str) -> str:
    """
    Retrieves the minimum value of the date_of_occurence field in the Chicago crimes dataset.

    Usage example:
        get_min_date_crime_data(APP_TOKEN="abc123")

    Returns:
        A str object with the date written in 'yyyy-mm-ddThh:mm:ss.sss' format. 

    Args:
        APP_TOKEN: provide a str with generated App Token credentials.
    """
    response = requests.get(f"https://data.cityofchicago.org/resource/x2n5-8w5q.json?"
                            f"$$app_token={APP_TOKEN}"
                            f"&$select=min(date_of_occurrence)")
    return response.json()[0].get('min_date_of_occurrence')

def get_max_date_crime_api(APP_TOKEN:str) -> str:
    """
    Retrieves the maximum value of the date_of_occurence field in the Chicago crimes dataset.

    Usage example:
        get_max_date_crime_data(APP_TOKEN="abc123")

    Returns:
        A str object with the date written in 'yyyy-mm-ddThh:mm:ss.sss' format. 

    Args:
        APP_TOKEN: provide a str with generated App Token credentials.
    """
    response = requests.get(f"https://data.cityofchicago.org/resource/x2n5-8w5q.json?"
                            f"$$app_token={APP_TOKEN}"
                            f"&$select=max(date_of_occurrence)")
    return response.json()[0].get('max_date_of_occurrence')

def get_max_update_time_crime_api(APP_TOKEN:str) -> str:
    """
    Retrieves the maximum value of the :updated_at field in the Chicago crimes dataset.

    Usage example:
        get_max_update_time_crime_data(APP_TOKEN="abc123")

    Returns:
        A str object with the date written in 'yyyy-mm-ddThh:mm:ss.sssZ' format. 

    Args:
        APP_TOKEN: provide a str with generated App Token credentials.
    """
    response = requests.get(f"https://data.cityofchicago.org/resource/x2n5-8w5q.json?"
                            f"$$app_token={APP_TOKEN}"
                            f"&$select=max(:updated_at)")
    return response.json()[0].get('max_updated_at')

def get_max_update_time_crime_table(crime_table_name:str, engine: Engine) -> datetime:
    """
    Returns maximum of value of the updated_at field from the Chicago crimes table in datetime format (UTC-adjusted).
    """
    select_max_update_query = f"select max(updated_at) from {crime_table_name}"
    max_update = [dict(row) for row in engine.execute(select_max_update_query).all()][0].get("max")
    return max_update.astimezone(timezone.utc).replace(tzinfo=None)

def extract_crime_api(APP_TOKEN:str, column_name:str, start_time:str, end_time:str, limit:int) -> pd.DataFrame:
    """
    Extracts Chicago crimes data from API endpoint for a given date range.

    Usage example:
        extract_crime_data(
            APP_TOKEN="abc123",
            column_name="date_of_occurrence", 
            start_time="2023-11-14T00:00:00.000", 
            end_time="2023-11-19T23:59:59.999", 
            limit=100
        )

    Returns:
        pd.DataFrame object encapsulating crimes data with following structure:   
        ```
        -------------------------------------------------------------------------------------
        | id	             |  created_at	             |  updated_at               |	...	|
        -------------------------------------------------------------------------------------
        | row-n56f.jhj4-rhq4 |	2023-11-14T11:02:01.256Z |	2023-11-14T11:02:11.508Z |	...	|
        -------------------------------------------------------------------------------------
        | row-p4xb~y53j-fuek |	2023-11-14T11:02:01.256Z |	2023-11-14T11:02:11.508Z |	...	|
        -------------------------------------------------------------------------------------
        ```

    Args:
        APP_TOKEN: provide a str with generated App Token credentials.
        column_name: provide a str ('date_of_occurrence' or ':updated_at') of needed column to filter data.
        start_time: provide a str with the format "yyyy-mm-ddThh:mm:ss.SSS".
        end_time: provide a str with the format "yyyy-mm-ddThh:mm:ss.SSS".
        limit: provide an int for maximum records retrieved per each API call.

    Raises:
        Exception when HTTP response code is not 200.
        Exception when paging over API endpoint for more than 1000 times (stuck in while loop). 
    """
    soql_date = f"where={column_name} between '{start_time}' and '{end_time}'" 
    response_data = []
    i = 0
    
    while True:
        offset = i * limit # if limit = 1000 -> offset = 0, 1000, 2000, etc.
        response = requests.get(f"https://data.cityofchicago.org/resource/x2n5-8w5q.json?"
                                f"$$app_token={APP_TOKEN}"
                                f"&$order=:id"  
                                f"&${soql_date}"
                                f"&$limit={limit}"
                                f"&$offset={offset}"
                                f"&$select=:*,*") # include metadata field info

        if not response.status_code==200:
            raise Exception

        if i >= 1000:
            raise Exception

        if response.json() == []:
            break

        response_data.extend(response.json())
        i += 1

    crime_df = pd.json_normalize(data=response_data)

    return crime_df

def transform_crime_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform data transformations on the input DataFrame.

    Parameters:
    - df (pd.DataFrame): Input DataFrame to be transformed.

    Returns:
    - pd.DataFrame: Transformed DataFrame.

    Transformations:
    1. Drop specified columns:
        - ':@computed_region_awaf_s7ux', 
        - ':@computed_region_6mkv_f3dw',
        - ':@computed_region_vrxf_vc4k', 
        - ':@computed_region_bdys_3d7i',
        - ':@computed_region_43wa_7qmu', 
        - ':@computed_region_rpca_8um6',
        - 'location.latitude',
        - 'location.longitude',
        - 'location.human_address'

    2. Rename columns based on the specified mapping:
        - ':id' -> 'id'
        - ':created_at' -> 'created_at'
        - ':updated_at' -> 'updated_at'
        - ':version' -> 'version'
        - 'case_' -> 'case'
        - '_iucr' -> 'iucr'
        - '_primary_decsription' -> 'primary_description'
        - '_secondary_description' -> 'secondary_description'
        - '_location_description' -> 'location_description'
    """
    # Transformation 1: Drop columns
    cols_to_drop = [
        ':@computed_region_awaf_s7ux', 
        ':@computed_region_6mkv_f3dw',
        ':@computed_region_vrxf_vc4k', 
        ':@computed_region_bdys_3d7i',
        ':@computed_region_43wa_7qmu', 
        ':@computed_region_rpca_8um6',
        'location.latitude',
        'location.longitude',
        'location.human_address'
    ]
    df = df.drop(columns=cols_to_drop)

    # Transformation 2: Rename columns
    col_mapping = {
        ':id':'crime_id',
        ':created_at':'created_at',
        ':updated_at':'updated_at',
        ':version':'version',
        'case_':'case',
        '_iucr':'iucr',
        '_primary_decsription':'primary_description',
        '_secondary_description':'secondary_description',
        '_location_description':'location_description',
    }
    df = df.rename(columns=col_mapping)

    return df

def generate_date_df(begin_date:str, end_date:str, holidays_data_path:list[str]) -> pd.DataFrame:
    """
    Creates a pd.DataFrame object for a date range with an additional holiday field.

    Usage example:
        generate_date_df(begin_date="2023-01-01", end_date="2024-12-12", holidays_data_path=["data/holidays/2023.csv"])

    Returns:
        pd.DataFrame object encapsulating dates data and corresponding holiday names with following structure:   
        ```
        -------------------------------------------------------------
        | date	     |  day_of_week_name  |  holiday_name   |   ...	|
        -------------------------------------------------------------
        | 2023-01-01 |	Sunday            |	 NaN            |   ...	|
        -------------------------------------------------------------
        | 2023-01-02 |	Monday            |	 New Year's Day |   ...	|
        -------------------------------------------------------------
        ```

    Args:
        begin_date: provide a str with the format "yyyy-mm-dd".
        end_date: provide a str with the format "yyyy-mm-dd".
        holidays_data_path: provide a list of str values pointing to location of CSV files containing holidays data.

    Raises:
        Exception when end_date is less than begin_date.
        Exception when holidays_data_path does not exist or has been corrupted in format.
    """ 
    date_df = pd.DataFrame({'date':pd.date_range(start=begin_date, end=end_date)})

    date_df['day'] = date_df['date'].dt.day
    date_df['month'] = date_df['date'].dt.month
    date_df['month_name'] = date_df['date'].dt.month_name()
    date_df['year'] = date_df['date'].dt.year
    date_df['day_of_week'] = date_df['date'].dt.dayofweek
    date_df['day_of_week_name'] = date_df['date'].dt.day_name()

    holidays_df = pd.concat(map(pd.read_csv, holidays_data_path))

    holidays_df = holidays_df.rename(columns={'Name': 'holiday_name'})
    holidays_df['date'] = pd.to_datetime(holidays_df['Date'])
    holidays_df = holidays_df.drop(['Date'], axis=1)

    return pd.merge(left=date_df, right=holidays_df, on=["date"], how="left")

def extract_csv(csv_file_path:str) -> pd.DataFrame:
    """
    Return a dataframe object with transformed columns based on a given CSV file path.
    """
    df = pd.read_csv(csv_file_path)
    df.columns = [column.lower().replace(" ","_") for column in df.columns]
    return df

def create_postgres_connection(username:str, password:str, host:str, port:int, database:str) -> Engine:
    """
    Connect to postgres server using provided pgAdmin credentials.
    """
    connection_url = URL.create(
        drivername = "postgresql+pg8000", 
        username = username,
        password = password,
        host = host, 
        port = port,
        database = database)

    return create_engine(connection_url)

def create_crime_table(engine:Engine) -> Table:
    """
    Create table for crimes data with applicable column names. 
    """
    meta = MetaData()
    table = Table(
        "crime_data", meta, 
        Column("crime_id", String, primary_key=True),
        Column("created_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True)),
        Column("version", String),
        Column("case", String),
        Column("date_of_occurrence", DateTime(timezone="US/Central")),
        Column("block", String),
        Column("iucr", String),
        Column("primary_description", String),
        Column("secondary_description", String),
        Column("location_description", String),
        Column("arrest", String),
        Column("domestic", String),
        Column("beat", Integer),
        Column("ward", Integer),
        Column("fbi_cd", String),
        Column("x_coordinate", Integer),
        Column("y_coordinate", Integer),
        Column("latitude", Float),
        Column("longitude", Float)
    )
    meta.create_all(bind=engine, checkfirst=True) # does not re-create table if it already exists
    return table

def create_date_table(engine:Engine) -> Table:
    """
    Create table for 2023 and 2024 dates and holiday data. 
    """
    meta = MetaData()
    table = Table(
        "date", meta, 
        Column('date',Date,primary_key=True),
        Column('day',Integer),
        Column('month',Integer),
        Column('month_name',String),
        Column('year',Integer),
        Column('day_of_week',Integer),
        Column('day_of_week_name',String),
        Column('holiday_name',String)
    )
    meta.create_all(bind=engine)
    return table

def create_police_table(engine:Engine) -> Table:
    """
    Create table for Chicago police district data. 
    """
    meta = MetaData()
    table = Table(
        "police_stations", meta,
        Column('district',String,primary_key=True),
        Column('district_name',String),
        Column('address',String),
        Column('city',String),
        Column('state',String),
        Column('zip',Integer),
        Column('website',String),
        Column('phone',String),
        Column('fax',String),
        Column('tty',String),
        Column('x_coordinate',Float),
        Column('y_coordinate',Float),
        Column('latitude',Float),
        Column('longitude',Float),
        Column('location',String)
    )
    meta.create_all(bind=engine)
    return table

def create_ward_table(engine:Engine) -> Table:
    """
    Create table for Chicago legislative districts (wards) data. 
    """
    meta = MetaData()
    table = Table(
        "ward_offices", meta,
        Column('ward',Integer,primary_key=True),
        Column('alderman',String),
        Column('address',String),
        Column('city',String),
        Column('state',String),
        Column('zipcode',String),
        Column('ward_phone',String),
        Column('ward_fax',String),
        Column('email',String),
        Column('website',String),
        Column('location',String),
        Column('city_hall_address',String),
        Column('city_hall_city',String),
        Column('city_hall_state',String),
        Column('city_hall_zipcode',Integer),
        Column('city_hall_phone',String),
        Column('photo_link',String)
    )
    meta.create_all(bind=engine)
    return table

def load_data_to_postgres(chunksize:int, data:list[dict], table:Table, engine:Engine) -> None:
    """
    Upsert data incrementally (chunking) into specific postgres table. 
    """
    max_length = len(data)
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]

    for i in range(0, max_length, chunksize):
        if i + chunksize >= max_length:
            lower_bound = i
            upper_bound = max_length
        else:
            lower_bound = i
            upper_bound = i + chunksize
        insert_statement = postgresql.insert(table).values(
            data[lower_bound:upper_bound]
        )
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        engine.execute(upsert_statement)

if __name__ == "__main__":
    # Initializing parameters
    days_delta = 7
    limit = 1000
    holidays_begin_date = "2023-01-01"
    holidays_end_date = "2024-12-31" 
    holidays_data_path = ['etl_project/data/holidays/2023.csv', 'etl_project/data/holidays/2024.csv']
    chunksize = 1000
    sql_folder_path = "etl_project/sql" 
    log_folder_path = "etl_project/logs"
    pipeline_name = "Chicago Crime ETL"
    log_table_name = "pipeline_logs"

    # Instantiating logger for pipeline run
    pipeline_logging = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=log_folder_path)

    # Try-except to log any errors during pipeline run
    try:
        pipeline_start_time = time.time()
        pipeline_logging.logger.info("Pipeline start")

        pipeline_logging.logger.info("Initializing environment variables")
        load_dotenv()
        APP_TOKEN = os.environ.get("APP_TOKEN")
        DB_USERNAME = os.environ.get("DB_USERNAME")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        SERVER_NAME = os.environ.get("SERVER_NAME")
        DATABASE_NAME = os.environ.get("DATABASE_NAME")
        PORT = os.environ.get("PORT")

        # Connecting to postgres and creating database tables
        pipeline_logging.logger.info("Connecting to pgAdmin")
        engine = create_postgres_connection(
            username=DB_USERNAME, 
            password=DB_PASSWORD, 
            host=SERVER_NAME, 
            port=PORT, 
            database=DATABASE_NAME)
        
        # Checking what tables exist in database
        pipeline_logging.logger.info("Inspecting database tables")
        inspector = inspect(engine)
        
        # Checking if ward table exists inside of database
        if 'ward' not in inspector.get_table_names():
            pipeline_logging.logger.info("Extracting ward data")
            ward_df = extract_csv(csv_file_path="etl_project/data/Ward_Offices.csv")

            pipeline_logging.logger.info("Creating ward table")
            ward_table = create_ward_table(engine=engine)

            pipeline_logging.logger.info("Inserting data records to ward table") 
            ward_data = ward_df.where(pd.notnull(ward_df), None).to_dict(orient='records')
            load_data_to_postgres(chunksize=chunksize, data=ward_data, table=ward_table, engine=engine)

        # Checking if police table exists inside of database
        if 'police' not in inspector.get_table_names():
            pipeline_logging.logger.info("Extracting police data")
            police_df = extract_csv(csv_file_path="etl_project/data/Police_Stations.csv")

            pipeline_logging.logger.info("Creating police table")
            police_table = create_police_table(engine=engine)

            pipeline_logging.logger.info("Inserting data records to police table")
            police_data = police_df.where(pd.notnull(police_df), None).to_dict(orient="records")
            load_data_to_postgres(chunksize=chunksize, data=police_data, table=police_table, engine=engine)

        # Checking if date table exists inside of database
        if 'date' not in inspector.get_table_names():
            pipeline_logging.logger.info("Generating date data")
            date_df = generate_date_df(begin_date=holidays_begin_date, end_date=holidays_end_date, holidays_data_path=holidays_data_path)

            pipeline_logging.logger.info("Creating date table")
            date_table = create_date_table(engine=engine)

            pipeline_logging.logger.info("Inserting data records to date table")
            date_data = date_df.where(pd.notnull(date_df), None).to_dict(orient='records')
            load_data_to_postgres(chunksize=chunksize, data=date_data, table=date_table, engine=engine)

        # Checking if crime table exists inside of database
        if 'crime' not in inspector.get_table_names():
            pipeline_logging.logger.info("Creating crime table")
            crime_table = create_crime_table(engine=engine)

            # Extracting crime data from beginning
            start_date = get_min_date_crime_api(APP_TOKEN=APP_TOKEN)
            end_date = get_max_date_crime_api(APP_TOKEN=APP_TOKEN)
            date_ranges = _generate_date_ranges(start_date=start_date, end_date=end_date, days_delta=days_delta)
            for date_range in date_ranges:
                start_time = date_range['start_time']
                end_time = date_range['end_time']

                pipeline_logging.logger.info(f"Extracting API data - {start_time} - {end_time}")
                crime_df = extract_crime_api(
                    APP_TOKEN=APP_TOKEN, 
                    column_name="date_of_occurrence",
                    start_time=start_time, 
                    end_time=end_time, 
                    limit=limit
                )

                pipeline_logging.logger.info(f"Transforming API data - {start_time} - {end_time}")
                crime_df = transform_crime_data(df=crime_df)

                pipeline_logging.logger.info(f"Loading API data - {start_time} - {end_time}")
                crime_data = crime_df.where(pd.notnull(crime_df), None).to_dict(orient='records')
                load_data_to_postgres(chunksize=chunksize, data=crime_data, table=crime_table, engine=engine)
        else:
            pipeline_logging.logger.info("Crime table exists - Checking for new API updates")
            max_api_str = get_max_update_time_crime_api(APP_TOKEN=APP_TOKEN)
            max_table = get_max_update_time_crime_table(crime_table_name="crime_data", engine=engine)
            max_api = datetime.strptime(max_api_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            
            if max_api > max_table:
                pipeline_logging.logger.info("New updates exist - Retrieving updated records from API")

                # Configuring parameters in correct format
                min_updated_at_val = max_table + timedelta(milliseconds=1) # ensure that new data does not overlap with current data
                start_time = min_updated_at_val.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                end_time = max_api_str[:-1]

                pipeline_logging.logger.info(f"Extracting API data - {start_time} - {end_time}")
                crime_df = extract_crime_api(
                    APP_TOKEN=APP_TOKEN, 
                    column_name=":updated_at",
                    start_time=start_time, 
                    end_time=end_time, 
                    limit=limit
                )

                pipeline_logging.logger.info(f"Transforming API data - {start_time} - {end_time}")
                crime_df = transform_crime_data(df=crime_df)
                
                pipeline_logging.logger.info("Upserting new updates to crime table")
                crime_data = crime_df.where(pd.notnull(crime_df), None).to_dict(orient='records')
                crime_table = create_crime_table(engine=engine) # does not re-create crime table in this case but returns table information
                load_data_to_postgres(chunksize=chunksize, data=crime_data, table=crime_table, engine=engine)
            else:
                pipeline_logging.logger.info("No new records to upsert")
            
        # Checking what views exist in database
        pipeline_logging.logger.info("Inspecting database views")
        inspector = inspect(engine)
        
        for sql_file in os.listdir(sql_folder_path):
            view = sql_file.split(".")[0] # name of view to match the name of the sql file

            if view not in inspector.get_view_names():
                pipeline_logging.logger.info(f"View {view} does not exist - Creating view")
                with open(f'{sql_folder_path}/{sql_file}', 'r') as f:
                    sql_query = f.read()
                    engine.execute(f"create view {view} as {sql_query};")
                    pipeline_logging.logger.info(f"Successfully created view {view}")
            else:
                pipeline_logging.logger.info(f"View {view} already exists in database")\

        pipeline_end_time = time.time()
        pipeline_run_time = pipeline_end_time - pipeline_start_time
        pipeline_logging.logger.info(f"Pipeline finished in {pipeline_run_time} seconds")
        pipeline_logging.logger.info("Successful pipeline run")
        
        # Ensure logger handlers are cleared
        pipeline_logging.logger.handlers.clear()

    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline failed with exception {e}")
        pipeline_logging.logger.handlers.clear()