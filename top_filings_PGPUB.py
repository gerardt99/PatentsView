# -*- coding: utf-8 -*-
"""
Created on Mon Oct  5 23:31:22 2020

This script uses PatentViews PG-Pub data to query for the top patent filers over
the last 5 years.

Download and unzips application and rawassignee to directory where python
file is saved.

Data tables from:
https://www.patentsview.org/download/pregrantpublications.html
Applications and RawAssignee tables

Joins applications and rawassignes by application number
Cleans data and counts applications by assignee sorted by descending count.

@author: gtorres
"""
#Import python libararies
import os #for changing directory
from io import BytesIO #for unzipping files
from urllib.request import urlopen #for downloading data
from zipfile import ZipFile #for unzipping files
import pandas as pd #for working with data

#Global variables - can be edited if needed
DIR = os.getcwd() #Directory location where python file is saved
#URL to PG-PUB Application data
URL1 = r"http://data.patentsview.org/pregrant_publications/application.tsv.zip"
#URL to PG-PUB Raw Assignee data
URL2 = r"http://data.patentsview.org/pregrant_publications/rawassignee.tsv.zip"

#This function downloads and unzips data into the directory
def getData():
    urls = [URL1, URL2]
    for url in urls:
        with urlopen(url) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                if url == URL1:
                    zfile.extractall("applications")
                else:
                    zfile.extractall("assignees")
    return

def joinData():
    path = DIR + "/applications/"
    os.chdir(path)
    df1 = pd.read_csv(r"application.tsv", delimiter="\t")
    print(df1.head)
    path = DIR + "/assignees/"
    os.chdir(path)
    df2 = pd.read_csv(r"assignee.tsv", delimiter="\t")
    print(df2.head)
    return

#main function to run script
def main():
    print("Beginning data download ...")
    #getData();
    print("Data Downloaded. Now joining the data...")
    joinData()
    
    return

#Calls the main function above
if __name__ == "__main__":
    main()
    
