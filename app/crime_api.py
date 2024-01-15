import requests
import pandas as pd 
#from secrets_config import api_key
from sodapy import Socrata


client = Socrata("data.cityofchicago.org",
                 "TuDyOSbRSTPWADojwlVd69hq0") #this is the apptoken

#Function to get all crime data - run once
def extractAll(client):
    results = client.get_all("x2n5-8w5q")  # the "x2n5-8w5q" is the dataset identifier

    allresults_df = pd.DataFrame.from_dict(results)

    print(allresults_df.head)
    print(len(allresults_df))

    return allresults_df


def incrementalExtract(client):
    results = client.get("x2n5-8w5q",  order="date_of_occurrence DESC", where="date_of_occurrence between '2024-01-01' and '2024-08-01")
    
    print(results)
    resultsPartial_df = pd.DataFrame.from_dict(results)
    print(resultsPartial_df)

    return resultsPartial_df