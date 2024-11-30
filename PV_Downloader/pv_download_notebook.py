# Databricks notebook source
import PatentsViewDownloader as pvd
from datetime import datetime
import os

# COMMAND ----------

# OCE Dev Volume
volume = "/Volumes/oce_dev/bronze/patentsview_files"

#
grants = "https://patentsview.org/download/data-download-tables"
pgpubs = "https://patentsview.org/download/pg-download-tables"
urls = [grants, pgpubs]

#Get current month and year
current_month = datetime.now().month
current_year = datetime.now().year

#Create new directory on OCE Dev Volume
release = str(current_month) + "_" + str(current_year)
print(release)
download_dir = os.path.join(volume, release)
print(download_dir)

# COMMAND ----------

for url in urls:
    downloader = pvd.PatentsViewDownloader(url, download_dir)
    downloaded_files = downloader.download_all()

    for file in downloader.get_downloaded_files():
        print(f"Downloaded: {file}")
