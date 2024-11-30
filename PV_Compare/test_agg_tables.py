# Databricks notebook source
import test_agg_compare

NEW = "/Volumes/oce_dev/bronze/patentsview_files/11_2024/"
OLD = "/Volumes/oce_dev/bronze/patentsview_files/old/"

# COMMAND ----------

compare = test_agg_compare.Agg_Compare(NEW, OLD, old_release_ext='.csv.gz')
compare.run_all_tests()
