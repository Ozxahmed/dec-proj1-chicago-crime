from elt_project.api_connection import _generate_date_ranges, load_crime_data_to_parquet, generate_date_df, extract_csv
import pandas as pd
import os
import pytest

@pytest.fixture
def setup_start_end_times():
    start_time = "2023-10-01T00:00:00.000"
    end_time = "2023-10-14T23:59:59.999"
    return start_time, end_time

def test_generate_dates(setup_start_end_times):
    start_time, end_time = setup_start_end_times
    expected = [
        {'start_time': '2023-10-01T00:00:00.000', 'end_time': '2023-10-07T23:59:59.999'},
        {'start_time': '2023-10-08T00:00:00.000', 'end_time': '2023-10-14T23:59:59.999'}
    ]
    result = _generate_date_ranges(start_date=start_time, end_date=end_time, days_delta=7)
    assert type(result) == list
    for list_obj in result:
        for key, value in list_obj.items():
            assert type(key) == str
            assert type(value) == str
        assert type(list_obj) == dict
    assert len(result) == 2
    assert result == expected

@pytest.fixture
def setup_input_df():
    return pd.DataFrame(
        [
            {
                "date": pd.to_datetime("2023-01-01"),
                "day": 1,
                "month": 1,
                "month_name": "January",
                "year": 2023,
                "day_of_week": 6,
                "day_of_week_name": "Sunday",
                "holiday_name": float('NaN')
            },
            {
                "date": pd.to_datetime("2023-01-02"),
                "day": 2,
                "month": 1,
                "month_name": "January",
                "year": 2023,
                "day_of_week": 0,
                "day_of_week_name": "Monday",
                "holiday_name": "New Year's Day"
            },
            {
                "date": pd.to_datetime("2023-01-03"),
                "day": 3,
                "month": 1,
                "month_name": "January",
                "year": 2023,
                "day_of_week": 1,
                "day_of_week_name": "Tuesday",
                "holiday_name": float('NaN')
            }
        ]
    )

def test_load_data_to_parquet(setup_start_end_times, setup_input_df):
    start_time, end_time = setup_start_end_times
    df = setup_input_df
    load_crime_data_to_parquet(start_time=start_time, end_time=end_time, crime_df=df)

    start_time_str = start_time.replace(":","-").replace(".","-")
    end_time_str = end_time.replace(":","-").replace(".","-")
    file_name = f'elt_project/data/crime_{start_time_str}_{end_time_str}.parquet'
    assert os.path.isfile(file_name) # assert parquet file successfully loaded
    pd.testing.assert_frame_equal(left=pd.read_parquet(file_name),right=df)
    
    os.remove(file_name) # remove file from directory after testing

def test_generate_date_df(setup_input_df):
    expected = setup_input_df
    df = generate_date_df("2023-01-01", "2023-01-03", ['elt_project/raw_data/holidays/2023.csv'])
    pd.testing.assert_frame_equal(left=expected, right=df)

def test_extract_csv():
    file_path = "elt_project_tests/data/Police_Stations.csv"
    df = extract_csv(csv_file_path=file_path)
    assert len(df) > 0
    for column_name in df.columns:
        assert column_name == column_name.lower() # assert no uppercase letters in column names
        assert " " not in column_name # assert column names do not contain spaces