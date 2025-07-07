"""
Remove 'Unnamed: 0' columns from dataframes
This script removes the 'Unnamed: 0' columns from df, df2, and df3 dataframes.
"""

import pandas as pd
import sys
import os

# Add src directory to path for importing utilities
sys.path.append('.')

# Import utility functions
from data_utils import load_data_files

def remove_unnamed_columns():
    """
    Load data files and remove 'Unnamed: 0' columns from all dataframes.
    
    What: Removes automatically generated index columns ('Unnamed: 0') from all loaded DataFrames.
    Why: These columns are artifacts from pandas CSV saving and are not meaningful for analysis.
    How: Checks for column existence and drops if present. Only shape/columns are printed for privacy.
    Alternative: Could use index_col=0 on read_csv, but explicit removal is safer for mixed sources.
    """
    print("Loading data files...")
    df, df2, df3 = load_data_files()
    print(f"df: {df.shape}, columns: {list(df.columns)}")
    print(f"df2: {df2.shape}, columns: {list(df2.columns)}")
    print(f"df3: {df3.shape}, columns: {list(df3.columns)}")
    
    print("\nRemoving 'Unnamed: 0' columns from all dataframes...")
    
    # Check if 'Unnamed: 0' column exists in each dataframe before removing
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])
        print("✓ Removed 'Unnamed: 0' from df (customer data)")
    
    if 'Unnamed: 0' in df2.columns:
        df2 = df2.drop(columns=['Unnamed: 0'])
        print("✓ Removed 'Unnamed: 0' from df2 (inventory data)")
    
    if 'Unnamed: 0' in df3.columns:
        df3 = df3.drop(columns=['Unnamed: 0'])
        print("✓ Removed 'Unnamed: 0' from df3 (sales data)")
    
    print("\nUpdated DataFrame Information:")
    print(f"df: {df.shape}, columns: {list(df.columns)}")
    print(f"df2: {df2.shape}, columns: {list(df2.columns)}")
    print(f"df3: {df3.shape}, columns: {list(df3.columns)}")
    
    print("\n" + "="*50)
    print("COLUMN CLEANUP COMPLETED")
    print("="*50)
    
    return df, df2, df3

if __name__ == "__main__":
    # Change to the correct directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Remove unnamed columns
    df, df2, df3 = remove_unnamed_columns()
    
    # Save cleaned data
    print("\nSaving cleaned data...")
    df.to_csv('../data/cleaned_customer_data.csv', index=False)
    df2.to_csv('../data/cleaned_inventory_data.csv', index=False)
    df3.to_csv('../data/cleaned_sales_data.csv', index=False)
    
    print("✓ Cleaned data saved to data folder")
    print("\nData cleaning completed successfully!") 