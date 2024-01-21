from dotenv import load_dotenv
from etl_project.api_connection import extract_crime_api
import os
import pytest
from datetime import datetime


@pytest.fixture
def setup():
    load_dotenv()


def test_chicago_crimes_api_connection(setup):
    APP_TOKEN = os.environ.get("APP_TOKEN")
    column_names = ["date_of_occurrence", ":updated_at"]
    start_time = '2024-01-01T00:00:00.000'
    end_time = '2024-01-03T23:59:59.999'
    limit = 1000
    
    data = extract_crime_api(
        APP_TOKEN=APP_TOKEN, 
        column_name=column_names[0],
        start_time=start_time, 
        end_time=end_time, 
        limit=limit
    )
    
    assert len(data) > 0 # asserts data records are retrieved
    assert list(data.columns) == [ # asserts column names are correct
        'crime_id',
        'created_at',
        'updated_at',
        'version',
        'case',
        'date_of_occurrence',
        'block',
        'iucr',
        'primary_description',
        'secondary_description',
        'location_description',
        'arrest',
        'domestic',
        'beat',
        'ward',
        'fbi_cd',
        'x_coordinate',
        'y_coordinate',
        'latitude',
        'longitude']
    
    # asserts date_of_occurence of data extracted is between given date ranges
    assert datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%f') <= datetime.strptime(min(data['date_of_occurrence']), '%Y-%m-%dT%H:%M:%S.%f')
    assert datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S.%f') >= datetime.strptime(max(data['date_of_occurrence']), '%Y-%m-%dT%H:%M:%S.%f')

    data = extract_crime_api(
        APP_TOKEN=APP_TOKEN, 
        column_name=column_names[1],
        start_time=start_time, 
        end_time=end_time, 
        limit=limit
    )

    assert len(data) > 0 # asserts data records are retrieved if extraction filtered by :updated_at column 