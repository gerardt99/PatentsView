# -*- coding: utf-8 -*-
"""
Created on Mon Oct  5 23:31:22 2020

This script uses PatentViews PG-Pub data to query for the top patent filers since 2005.

Download and unzips application and rawassignee to directory where python
file is saved.

Data tables from:
https://www.patentsview.org/download/pregrantpublications.html
Publication and RawAssignee tables

Joins publication and rawassigne tables by application number
Cleans data and counts publications by assignee sorted by descending count.

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
URL1 = r"http://data.patentsview.org/pregrant_publications/publication.tsv.zip"
#URL to PG-PUB Raw Assignee data
URL2 = r"http://data.patentsview.org/pregrant_publications/rawassignee.tsv.zip"
#Directory name for publication data
DIR1 = "publications"
#Directory name for assignee data
DIR2 = "assignees"
#Publication File
F1 = r"publication.tsv"
#Assignee File
F2 = r"rawassignee.tsv"
#File name to save data to same directory as python file
SAVE = r"application_assignee_data.csv"

#This function downloads and unzips data into the directory
def getData():
    print("Beginning data download ...")
    urls = [URL1, URL2]
    for url in urls:
        with urlopen(url) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                if url == URL1:
                    zfile.extractall(DIR1)
                else:
                    zfile.extractall(DIR2)
    return

def joinData():
    print("Now joining the data...")
    path = DIR + "/" + DIR1 + "/"
    os.chdir(path)
    col = ['document_number','date','country','kind','filing_type']
    df1 = pd.read_csv(F1, delimiter="\t", usecols = col)
    df1.rename(columns = {'country':'pub_country'}, inplace = True)
    #print(len(df1))
    path = DIR + "/" + DIR2 + "/"
    os.chdir(path)
    col = ['document_number','sequence', 'name_first', 'name_last',
       'organization', 'type', 'city', 'state', 'country']
    df2 = pd.read_csv(F2, delimiter="\t", usecols = col)
    df2.rename(columns = {'country':'assignee_country'}, inplace = True)
    #print(len(df2))
    df3 = df1.merge(df2, how = 'left', on = 'document_number')
    return df3

#Cleans the organization/assignee name
def cleanData(data):
    print("Cleaning data ...")
    #Remove all punctuation from organziation
    data['organization'] = data['organization'].str.replace(r'[^\w\s]+', '')
    #Make all organization names capitalized
    data['organization'] = data['organization'].str.upper()
    return data

#Counts applications by organization/assignee
def countApps(data):
    print('Number of filings per Assignee since 2005')
    data2 = data['organization'].value_counts()
    print(data2)
    return

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
    data = cleanData(data) #cleans organization names
    saveData(data) #save data to csv file
    countApps(data) # counts applications by filer
    return

#Calls the main function above
if __name__ == "__main__":
    main()
