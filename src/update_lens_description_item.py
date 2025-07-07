import pandas as pd
import numpy as np

def update_lens_description_item(inv_df):
    """
    When Category_Level_1 is 'lens',
    merge Category_Level_2 and Category_Level_3 to
    overwrite Description and Item columns
    """
    
    # Copy data
    df = inv_df.copy()
    
    # Filter only lens data
    lens_mask = df['Category_Level_1'] == 'lens'
    lens_df = df[lens_mask].copy()
    
    print(f"Lens data: {len(lens_df)} rows")
    
    # Merge Category_Level_2 and Category_Level_3
    def merge_cat2_cat3(row):
        cat2 = str(row['Category_Level_2']) if pd.notna(row['Category_Level_2']) else ''
        cat3 = str(row['Category_Level_3']) if pd.notna(row['Category_Level_3']) else ''
        merged = (cat2 + ' ' + cat3).strip()
        return merged
    
    # Generate merged string
    merged_values = lens_df.apply(merge_cat2_cat3, axis=1)
    
    # Overwrite Description and Item columns
    df.loc[lens_mask, 'Description'] = merged_values.values
    df.loc[lens_mask, 'Item'] = merged_values.values
    
    print("Lens data Description and Item columns update completed")
    
    # Check results
    print("\n=== Update Result Sample ===")
    updated_lens = df[lens_mask][['item_code', 'Item', 'Description', 'Category_Level_1', 'Category_Level_2', 'Category_Level_3']].head(10)
    print(updated_lens)
    
    return df

def main():
    # Load data (modified to current file path)
    try:
        # First check if cleaned file exists
        inv_df = pd.read_csv('data/inventory_final_anonymous_cleaned.csv')
        print("Cleaned file loaded successfully")
    except:
        # If not, load original file
        inv_df = pd.read_csv('data/inventory_final_anonymous.csv')
        print("Original file loaded successfully")
    
    print(f"Total data: {len(inv_df)} rows")
    print(f"Columns: {list(inv_df.columns)}")
    
    # Check lens data
    lens_count = len(inv_df[inv_df['Category_Level_1'] == 'lens'])
    print(f"Lens data: {lens_count} rows")
    
    # Execute update
    updated_df = update_lens_description_item(inv_df)
    
    # Save results
    output_file = 'data/inventory_final_anonymous_updated.csv'
    updated_df.to_csv(output_file, index=False)
    print(f"\nUpdated data saved successfully: {output_file}")
    
    # Save lens data separately
    lens_df = updated_df[updated_df['Category_Level_1'] == 'lens']
    lens_output_file = 'data/lens_data_updated.csv'
    lens_df.to_csv(lens_output_file, index=False)
    print(f"Lens data saved successfully: {lens_output_file}")

if __name__ == "__main__":
    main() 