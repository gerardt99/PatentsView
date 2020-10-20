# -*- coding: utf-8 -*-
"""
Created on Oct 17 2020

This script uses PatentViews data to query for the top patent assignees.

Download and unzips application data directory where python
file is saved.

Data tables from:
https://www.patentsview.org/download/
Application, Assignee, and Location tables.
Also uses patent_assignee table to merge the tables

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
#URL to PG-PUB Publication data
URL1 = r"http://data.patentsview.org/20200630/download/application.tsv.zip"
#URL to PG-PUB Raw Assignee data
URL2 = r"http://data.patentsview.org/20200630/download/assignee.tsv.zip"
#Cross Walk for applications and assignees
URL3 = r"http://data.patentsview.org/20200630/download/patent_assignee.tsv.zip"
#Assignee location data
URL4 = r"http://data.patentsview.org/20200630/download/location.tsv.zip"
#Directory name for publication data
DIR1 = "applications"
#Directory name for assignee data
DIR2 = "assignees"
#Directory for cross walk
DIR3 = "crosswalk"
#Directory for location
DIR4 = "locations"
#Publication File
F1 = r"application.tsv"
#Assignee File
F2 = r"assignee.tsv"
#Cross Walk file
F3 = r"patent_assignee.tsv"
#Location File
F4 = r"location.tsv"
#File name to save data to same directory as python file
SAVE = r"application_assignee_data.csv"

#This function downloads and unzips data into the directory
def getData():
    print("Beginning data download ...")
    urls = [URL1, URL2, URL3, URL4]
    for url in urls:
        with urlopen(url) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                if url == URL1:
                    zfile.extractall(DIR1)
                elif url == URL2:
                    zfile.extractall(DIR2)
                elif url == URL3:
                    zfile.extractall(DIR3)
                else:
                    zfile.extractall(DIR4)
    return

def joinData():
    print("Now joining the data...")
    #Read application data
    path = DIR + "/" + DIR1 + "/"
    os.chdir(path)
    df1 = pd.read_csv(F1, delimiter="\t", low_memory=False)
    df1 = df1[['patent_id','date']]
    df1 = df1.rename(columns={'patent_id':'patent_id', 'date':'filing_date'})
    print("Number of records in application table: ", len(df1))
    
    #Read assignee data
    path = DIR + "/" + DIR2 + "/"
    os.chdir(path)
    df2 = pd.read_csv(F2, delimiter="\t", low_memory=False)
    df2 = df2[['id','type','organization']]
    cols = {'id':'assignee_id', 'type':'assignee_type','organization':'assignee_name'}
    df2 = df2.rename(columns=cols)
    print("Number of records in assignee table: ", len(df2))
    
    #Read cross walk data
    path = DIR + "/" + DIR3 + "/"
    os.chdir(path)
    df3 = pd.read_csv(F3, delimiter="\t", low_memory=False)
    print("Number of records in cross-walk table: ", len(df3))

    #Read location data
    path = DIR + "/" + DIR4 + "/"
    os.chdir(path)
    df4 = pd.read_csv(F4, delimiter="\t", low_memory=False)
    df4 = df4[['id','country']]
    cols = {'id':'location_id','country':'assignee_country'}
    df4 = df4.rename(columns=cols)
    print("Number of records in location table: ", len(df4))
    
    #join data sets
    df5 = pd.merge(df1, df3, how='left', on='patent_id')
    df5 = df5.merge(df2, how='left', on='assignee_id')
    df5 = df5.merge(df4, how='left', on='location_id')
    print(df5.describe())
    print(df5[:10])
    return df4

#save data as csv file to same directory as python file
def saveData(data):
    print('Saving Data to directory')
    os.chdir(DIR)
    data.to_csv(SAVE)
    return

#main function to run script
def main():
    getData(); #comment out after downloading data to avoid downloading again
    data = joinData() #joins the tables together
    saveData(data) #save data to csv file
    return

#Calls the main function above
if __name__ == "__main__":
    main()
