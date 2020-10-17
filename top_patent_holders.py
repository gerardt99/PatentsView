# -*- coding: utf-8 -*-
"""
Created on Oct 16, 2020

This script uses PatentViews API to query for the top patent holders since 1980.
It queries the API for the patent and assignee data, parses the json response
into a Pandas dataframe and saves the dataframe as a Stata (dta) file.

For info about the PatentsView API see:
https://api.patentsview.org/doc.html

@author: gtorres
"""
# import python libraries
import requests # to make API request
import json #to retrieve API data
import pandas as pd #for data manipulation
import os #to get current working directory
from requests.packages.urllib3.exceptions import InsecureRequestWarning # to deal with PatentsView API issue


#Global variables - can be edited if needed
DIR = os.getcwd() #Directory location where this python file is saved
STATA_FILE = r'patents_assignees.dta' #Name of the stata file created and saved in same directory

#API settings - most adhere to API documentation noted above
URL = r'https://api.patentsview.org/patents/query?q='
q = r'{"_gte":{"patent_date":"1979-12-31"}}'
f = r'["patent_number","patent_date","patent_type"]'
s = r'[{"patent_date":"asc"}]'
QUERY = URL + q + "&f=" + f + "&s=" + s
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#Checks that the API is working properly. Will send error message if not.
def check_api_status():
    response = requests.get(QUERY, verify = False)
    status = response.status_code
    print('API Status Code', status)
    if status != 200:
        print("Error: not able to process PatentsView API request.")
    else:
        print("Connected to PatentsView API and downloading data.")
    return

#Retrives data from PatentsViewAPI
def get_data():
    response = requests.get(QUERY, verify = False)
    print(json.dumps(response.json(), sort_keys=True, indent=4))
    return response

def main():
    check_api_status()
    data = get_data()
    return


if __name__ == '__main__':
    main()
