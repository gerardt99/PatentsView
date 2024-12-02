# Databricks notebook source
import dataframe_comparator
import os
import pandas as pd

NEW = "/Volumes/oce_dev/bronze/patentsview_files/11_2024/"
OLD = "/Volumes/oce_dev/bronze/patentsview_files/old/"

# COMMAND ----------

#Get files from both releases as a dictionary
def release_dict(dir):
    file_dict = {}
    for file in os.listdir(dir):
        fname = file.split(".")[0]
        file_dict[fname] = file
    return(file_dict)
    
new_files = release_dict(NEW)
old_files = release_dict(OLD)
for file_name in new_files:
    new_df = pd.read_
    old_file = old_files[new_file]
# Initialize and use the comparator
#comparator = DataFrameComparator(df1, df2)
#comparator.export_metrics_to_excel()
