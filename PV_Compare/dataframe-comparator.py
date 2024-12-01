import pandas as pd
import numpy as np

class DataFrameComparator:
    def __init__(self, df1, df2):
        """
        Initialize the DataFrameComparator with two pandas DataFrames
        
        Args:
            df1 (pd.DataFrame): First DataFrame to compare
            df2 (pd.DataFrame): Second DataFrame to compare
        """
        self.df1 = df1.copy()
        self.df2 = df2.copy()
        
        # Validate column consistency
        self._validate_columns()
    
    def _validate_columns(self):
        """
        Check if both DataFrames have the same columns
        
        Raises:
            ValueError: If columns are not identical
        """
        # Get column sets
        cols1 = set(self.df1.columns)
        cols2 = set(self.df2.columns)
        
        # Check column consistency
        if cols1 != cols2:
            # Find differences
            missing_in_df1 = cols2 - cols1
            missing_in_df2 = cols1 - cols2
            
            error_msg = "DataFrames have different columns:\n"
            if missing_in_df1:
                error_msg += f"Columns missing in first DataFrame: {missing_in_df1}\n"
            if missing_in_df2:
                error_msg += f"Columns missing in second DataFrame: {missing_in_df2}"
            
            raise ValueError(error_msg)
    
    def _calculate_column_metrics(self):
        """
        Calculate metrics for each column
        
        Returns:
            pd.DataFrame: A DataFrame with column metrics
        """
        metrics = []
        
        for col in self.df1.columns:
            # Total records
            total_records_df1 = len(self.df1)
            total_records_df2 = len(self.df2)
            
            # Unique values
            unique_df1 = self.df1[col].nunique()
            unique_df2 = self.df2[col].nunique()
            
            # Missing values
            missing_df1 = self.df1[col].isna().sum()
            missing_df2 = self.df2[col].isna().sum()
            
            # Values in both DataFrames
            common_values = len(set(self.df1[col].dropna()) & set(self.df2[col].dropna()))
            
            # Data type
            dtype_df1 = str(self.df1[col].dtype)
            dtype_df2 = str(self.df2[col].dtype)
            
            metrics.append({
                'Column': col,
                'DataFrame1_Total_Records': total_records_df1,
                'DataFrame2_Total_Records': total_records_df2,
                'DataFrame1_Unique_Values': unique_df1,
                'DataFrame2_Unique_Values': unique_df2,
                'DataFrame1_Missing_Values': missing_df1,
                'DataFrame2_Missing_Values': missing_df2,
                'Common_Values': common_values,
                'DataFrame1_Data_Type': dtype_df1,
                'DataFrame2_Data_Type': dtype_df2
            })
        
        return pd.DataFrame(metrics)
    
    def export_metrics_to_excel(self, output_path='dataframe_comparison_metrics.xlsx'):
        """
        Calculate and export column metrics to an Excel file
        
        Args:
            output_path (str, optional): Path to save the Excel file. 
                                         Defaults to 'dataframe_comparison_metrics.xlsx'
        
        Returns:
            str: Path to the saved Excel file
        """
        # Calculate metrics
        metrics_df = self._calculate_column_metrics()
        
        # Export to Excel
        metrics_df.to_excel(output_path, index=False)
        
        print(f"Metrics exported to {output_path}")
        return output_path

# Example usage
def main():
    # Create sample DataFrames for demonstration
    df1 = pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie', 'David', np.nan],
        'Age': [25, 30, 35, 40, 45],
        'City': ['New York', 'London', 'Paris', 'Tokyo', 'Berlin']
    })
    
    df2 = pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie', 'Eve', np.nan],
        'Age': [25, 30, 35, 50, 55],
        'City': ['New York', 'London', 'Paris', 'Sydney', 'Rome']
    })
    
    # Initialize and use the comparator
    comparator = DataFrameComparator(df1, df2)
    comparator.export_metrics_to_excel()

if __name__ == "__main__":
    main()
