import os
import pandas as pd
import numpy as np

class Agg_Compare:
    """
    A class to compare PatentView releases at the aggregate(grant or pgpubs) level
    """
    
    def __init__(self, new_release_dir, old_release_dir, new_release_ext='.tsv.zip', old_release_ext='.tsv.zip'):
        """
        Initialize the object with the directories of the new and old releases
        Args:
            new_release_dir = directory where the new release files are stored
            old_release_dir = directory where the old release files are stored
            new_release_ext = file extensions in new release
            old_release_ext = file extensions in old release
        """
        self.new_dir = new_release_dir
        self.old_dir = old_release_dir
        self.new_ext = new_release_ext
        self.old_ext = old_release_ext
        return
    
    def count_tables(self, dir):
        count = {'total':0,'pgpub':0,'grant':0}
        for file in os.listdir(dir):
            count['total'] += 1
            if file.startswith("g_"):
                count['grant'] += 1
            elif file.startswith("pg_"):
                count['pgpub'] += 1
        return(count)
    
    def count_all_tables(self):
        new_counts = self.count_tables(self.new_dir)
        old_counts = self.count_tables(self.old_dir) 
        if new_counts == old_counts:
            print("New release has same number of tables as prior release: ", new_counts, "\n")
        else:
            print("New release has different number of tables then prior release:")
            print("\t New release table counts: ", new_counts)
            print("\t Old release table counts: ", old_counts, "\n")
        return
    
    def compare_table_names(self):

        def print_missing_tables(tables):
            for table in tables:
                print("\t\t", table)
            return

        new_tables = set([file.split(".")[0] for file in os.listdir(self.new_dir)])
        old_tables = set([file.split(".")[0] for file in os.listdir(self.old_dir)])
        if new_tables == old_tables:
            print("New release contains same table names as prior release. \n")
        else:
            print("New release has different table names than prior release:")
            print("\t Tables in new release, but not in prior release: ")
            print_missing_tables(new_tables - old_tables)
            print("\t Tables in prior release, but not in new release: ")
            print_missing_tables(old_tables - new_tables)
            print("\n")
        return
    
    def compare_data_types(self):
        bad_cols = []
        bad_dtypes = []

        def read_file(file):
            if file.endswith(".csv.gz"):
                df = pd.read_csv(file, compression = "gzip", low_memory=False)
            else:
                df = pd.read_csv(file, sep="\t", compression="zip", low_memory=False)
            return(df)

        for file in os.listdir(self.new_dir):
            print(file)
            old_df = 0
            new_fname = os.path.join(self.new_dir, file)
            new_df = read_file(new_fname)
            old_name = file.split(".")[0]
            temp = [self.old_dir, old_name, self.old_ext]
            old_fname = "".join(temp)
            try:
                old_df = read_file(old_fname)
            except:
                pass
            else:
                new_cols = new_df.columns.to_list()
                old_cols = old_df.columns.to_list()
                if new_cols != old_cols:
                    bad_cols.append(old_name)
                else:
                    if (new_df.dtypes == old_df.dtypes).all():
                        pass
                    else:
                        bad_dtypes.append(old_name)
            del new_df, old_df
        if not bad_cols and not bad_dtypes:
            print("All tables have the same columns and data types between releases.")
        else:
            if len(bad_cols) > 0:
                print("The following tables have different column names between releases:")
                for table in bad_cols:
                    print(table)
            if len(bad_dtypes) > 0:
                print("The following tables have different data types:")
                for table in bad_dtypes:
                    print(table)
        return

    def run_all_tests(self):
        """
        Runs all the tests above
        """
        self.count_all_tables()
        self.compare_table_names()
        self.compare_data_types()
        return