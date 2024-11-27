# Databricks notebook source
import PatentsViewDownloader as pvd
from datetime import datetime
import os

# COMMAND ----------

# OCE Dev Volume
volume = "/Volumes/oce_dev/bronze/patentsview_files"

#Get current month and year
current_month = datetime.now().month
current_year = datetime.now().year

#Create new directory on OCE Dev Volume
release = str(current_month) + "_" + str(current_year)
print(release)
download_dir = os.path.join(volume, release)
print(download_dir)
#dbutils.fs.mkdirs(download_dir)

# COMMAND ----------

base_url = "https://patentsview.org/download/data-download-tables"
downloader = pvd.PatentsViewDownloader(base_url, download_dir)

downloaded_files = downloader.download_all()

for file in downloader.get_downloaded_files():
    print(f"Downloaded: {file}")
