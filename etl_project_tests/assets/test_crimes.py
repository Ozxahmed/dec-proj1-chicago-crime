from etl_project.api_connection import _generate_date_ranges, generate_date_df, extract_csv, transform_crime_data
import pandas as pd
import pytest

def test_extract_csv():
    file_path = "etl_project_tests/data/Police_Stations.csv"
    df = extract_csv(csv_file_path=file_path)
    assert len(df) > 0
    for column_name in df.columns:
        assert column_name == column_name.lower() # assert no uppercase letters in column names
        assert " " not in column_name # assert column names do not contain spaces

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
def setup_input_date_df():
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

def test_generate_date_df(setup_input_date_df):
    expected = setup_input_date_df
    df = generate_date_df("2023-01-01", "2023-01-03", ['etl_project/data/holidays/2023.csv'])
    pd.testing.assert_frame_equal(left=expected, right=df)

@pytest.fixture
def setup_input_crime_df():
    return pd.DataFrame(
        [
            {
                ":id":"row-6nmm_trd2~z4v7",
                ":created_at":"2023-10-09T10:02:17.438Z",
                ":updated_at":"2023-10-09T10:02:32.402Z",
                ":version":"rv-hu9i-h33m.mx5k",
                ":@computed_region_awaf_s7ux":"17",
                ":@computed_region_6mkv_f3dw":"21559",
                ":@computed_region_vrxf_vc4k":"66",
                ":@computed_region_bdys_3d7i":"410",
                ":@computed_region_43wa_7qmu":"32",
                ":@computed_region_rpca_8um6":"11",
                "case_":"JG446391",
                "date_of_occurrence":"2023-10-01T00:00:00.000",
                "block":"070XX S MORGAN ST",
                "_iucr":"1310",
                "_primary_decsription":"CRIMINAL DAMAGE",
                "_secondary_description":"TO PROPERTY",
                "_location_description":"APARTMENT",
                "arrest":"N",
                "domestic":"N",
                "beat":"733",
                "ward":"16",
                "fbi_cd":"14",
                "x_coordinate":"1170859",
                "y_coordinate":"1858203",
                "latitude":"41.76638357",
                "longitude":"-87.649296327",
                "location.latitude":"41.76638357",
                "location.longitude":"-87.649296327",
                "location.human_address":"none"
            }
        ]
    )

def test_transform_crime_data(setup_input_crime_df):
    input_df = setup_input_crime_df
    df = transform_crime_data(input_df)
    assert list(df.columns) == [
        'crime_id', 'created_at', 'updated_at', 'version', 'case','date_of_occurrence', 
        'block', 'iucr', 'primary_description','secondary_description', 'location_description', 
        'arrest', 'domestic', 'beat', 'ward', 'fbi_cd', 'x_coordinate', 'y_coordinate', 'latitude','longitude'
        ]