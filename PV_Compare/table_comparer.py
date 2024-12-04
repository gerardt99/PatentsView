# Databricks notebook source
from dataframe_comparator import DataFrameComparator
import os
import pandas as pd
import datetime

NEW = "/Volumes/oce_dev/bronze/patentsview_files/11_2024/"
OLD = "/Volumes/oce_dev/bronze/patentsview_files/old/"
OUTPUT = ["release_analysis", str(datetime.datetime.now().year), str(datetime.datetime.now().month)]
OUTPUT_DIR = "_".join(OUTPUT)
output_dir = os.path.join("/Volumes/oce_dev/bronze/patentsview_files/test_release/", OUTPUT_DIR)
if not os.path.exists(output_dir):
    os.mkdir(output_dir)

# COMMAND ----------

#Get files from both releases as a dictionary
def release_dict(dir):
    file_dict = {}
    for file in os.listdir(dir):
        fname = file.split(".")[0]
        file_dict[fname] = file
    return(file_dict)

def read_file(file):
    if file.endswith(".csv.gz"):
        df = pd.read_csv(file, compression = "gzip", low_memory=False)
    else:
        df = pd.read_csv(file, sep="\t", compression="zip", low_memory=False)
    return(df)
    
new_files = release_dict(NEW)
old_files = release_dict(OLD)
for file_name in new_files:
    print(file_name)
    if file_name in old_files:
        old_df = read_file(os.path.join(OLD, old_files[file_name]))
        new_df = read_file(os.path.join(NEW, new_files[file_name]))
        # Initialize and use the comparator
        comparator = DataFrameComparator(new_df, old_df)
        '''
        try:
            comparator = DataFrameComparator(new_df, old_df)
        except:
            continue
        '''
        output_file = os.path.join(output_dir, file_name + ".csv")
        comparator.export_metrics(output_file)
        #break
    else:
        print("\t", file_name, " is not in prior release.")
