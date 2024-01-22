#Need to add schedule library to requirements??
#pip install schedule

from dotenv import load_dotenv

import requests
import os
import pytest
from datetime import datetime
import schedule
import time

APP_TOKEN = os.environ.get("APP_TOKEN")

@pytest.fixture
def get_max_date_crime_data(APP_TOKEN:str) -> str:
    """
    Retrieves the maximum value of the date_of_occurence field in the Chicago crimes dataset.

    Usage example:
        get_max_date_crime_data(APP_TOKEN="abc123")

    Returns:
        A str object with the date written in 'yyyy-mm-dd' format. 

    Args:
        APP_TOKEN: provide a str with generated App Token credentials.
    """
    response = requests.get(f"https://data.cityofchicago.org/resource/x2n5-8w5q.json?"
                            f"$$app_token={APP_TOKEN}"
                            f"&$select=max(date_of_occurrence)")
    return response.json()[0].get('max_date_of_occurrence')


def test_schedule_pipeline(job=get_max_date_crime_data):
    
    #schedule.every().day.do(test_get_max_date_crime_data)
    schedule.every(1).minutes.do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

#https://schedule.readthedocs.io/en/stable/


