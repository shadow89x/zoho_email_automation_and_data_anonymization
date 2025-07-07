"""
Split Inventory Item Categories
This script splits the Item column in df2 into hierarchical categories based on ':' separator.
"""

import pandas as pd
import sys
import os

# Add src directory to path for importing utilities
sys.path.append('.')

# Import utility functions
from data_utils import load_data_files

def split_inventory_categories():
    """
    Load data files and split the Item column into hierarchical categories.
    
    What: Loads inventory data and splits the 'Item' column into multiple hierarchical category columns based on ':' separator.
    Why: Many inventory systems encode category hierarchy in a single string; splitting enables easier analysis and filtering.
    How: Counts hierarchy depth, splits strings, pads missing levels, and adds new columns. Only shape/columns are printed for privacy.
    Alternative: Could use regular expressions or a parser for more complex structures, but split(':') is fast and sufficient for this format.
    """
    print("Loading data files...")
    df, df2, df3 = load_data_files()
    print(f"df2: {df2.shape}, columns: {list(df2.columns)}")
    
    # Remove 'Unnamed: 0' columns first
    print("\nRemoving 'Unnamed: 0' columns...")
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])
    if 'Unnamed: 0' in df2.columns:
        df2 = df2.drop(columns=['Unnamed: 0'])
    if 'Unnamed: 0' in df3.columns:
        df3 = df3.drop(columns=['Unnamed: 0'])
    
    print("=== INVENTORY ITEM CATEGORY ANALYSIS ===")
    
    # First, let's examine the Item column structure
    print("\n--- Item Column Structure Analysis ---")
    print(f"Total items: {len(df2)}")
    print(f"Unique items: {df2['Item'].nunique()}")
    
    # Sample some items to understand the structure
    print("\n--- Sample Items ---")
    sample_items = df2['Item'].dropna().head(10).tolist()
    for i, item in enumerate(sample_items, 1):
        print(f"{i}. {item}")
    
    # Analyze the depth of hierarchy (number of colons)
    print("\n--- Hierarchy Depth Analysis ---")
    colon_counts = df2['Item'].str.count(':').value_counts().sort_index()
    print("Number of colons (hierarchy levels):")
    for colons, count in colon_counts.items():
        print(f"  {colons} colons: {count:,} items")
    
    # Find the maximum depth
    max_depth = df2['Item'].str.count(':').max()
    print(f"\nMaximum hierarchy depth: {max_depth} levels")
    
    # Split the Item column into hierarchical categories
    print("\n--- Splitting Item Column ---")
    
    # Create a function to split item into categories
    def split_item_categories(item):
        """
        Split item string by ':' and return hierarchical categories
        """
        if pd.isna(item):
            return [None] * (max_depth + 1)
        
        parts = item.split(':')
        # Pad with None if not enough parts
        while len(parts) < max_depth + 1:
            parts.append(None)
        
        return parts[:max_depth + 1]
    
    # Apply the splitting function
    split_items = df2['Item'].apply(split_item_categories)
    
    # Create new columns for each level
    for i in range(max_depth + 1):
        col_name = f'Category_Level_{i+1}' if i < max_depth else 'Product_Code'
        df2[col_name] = split_items.apply(lambda x: x[i] if x else None)
    
    print("✓ Item column split into hierarchical categories")
    
    # Display the new structure
    print("\n--- New DataFrame Structure ---")
    print(f"Original columns: {list(df2.columns)}")
    print(f"New shape: {df2.shape}")
    
    # Show sample of the split data
    print("\n--- Sample Split Data ---")
    sample_cols = ['Item'] + [f'Category_Level_{i+1}' for i in range(max_depth)] + ['Product_Code']
    print(df2[sample_cols].head(10).to_string())
    
    # Analyze category distribution
    print("\n--- Category Distribution ---")
    for i in range(max_depth):
        col_name = f'Category_Level_{i+1}'
        unique_categories = df2[col_name].nunique()
        print(f"{col_name}: {unique_categories:,} unique categories")
    
    # Show some examples of the hierarchy
    print("\n--- Hierarchy Examples ---")
    for i in range(min(5, len(df2))):
        item = df2.iloc[i]['Item']
        if pd.notna(item):
            print(f"\nItem: {item}")
            for j in range(max_depth):
                col_name = f'Category_Level_{j+1}'
                value = df2.iloc[i][col_name]
                if pd.notna(value):
                    print(f"  Level {j+1}: {value}")
            product_code = df2.iloc[i]['Product_Code']
            if pd.notna(product_code):
                print(f"  Product Code: {product_code}")
    
    print("\n" + "="*50)
    print("INVENTORY ITEM CATEGORY ANALYSIS COMPLETED")
    print("="*50)
    
    print(f"df2: {df2.shape}, columns: {list(df2.columns)}")
    return df, df2, df3

if __name__ == "__main__":
    # Change to the correct directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Split inventory categories
    df, df2, df3 = split_inventory_categories()
    
    # Save the processed inventory data
    print("\nSaving processed inventory data...")
    df2.to_csv('../data/processed_inventory_with_categories.csv', index=False)
    print("✓ Processed inventory data saved to '../data/processed_inventory_with_categories.csv'")
    
    print("\nInventory category splitting completed successfully!") 