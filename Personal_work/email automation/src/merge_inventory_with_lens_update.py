import pandas as pd
import numpy as np

def merge_inventory_with_lens_update():
    """
    Merge existing inventory data with new lens data
    Overwrite only lens data with new data
    """
    
    print("=== Starting Inventory Merge Process ===")
    
    # 1. Load existing inventory files
    print("Loading existing inventory files...")
    
    # Original inventory file (select from multiple options)
    original_files = [
        'data/inventory_final_anonymous.csv',
        'data/inventory_final_cleaned.csv',
        'data/processed_inventory_data_with_item_code.csv',
        'data/inventory.CSV'
    ]
    
    original_df = None
    for file_path in original_files:
        try:
            original_df = pd.read_csv(file_path)
            print(f"Existing file loaded successfully: {file_path}")
            print(f"Existing data: {len(original_df)} rows, columns: {list(original_df.columns)}")
            break
        except Exception as e:
            print(f"File load failed: {file_path} - {e}")
            continue
    
    if original_df is None:
        print("Cannot find existing inventory file.")
        return None
    
    # 2. Load new lens data
    print("\nLoading new lens data...")
    try:
        new_lens_df = pd.read_csv('data/lens_data_updated.csv')
        print(f"New lens data: {len(new_lens_df)} rows")
    except Exception as e:
        print(f"New lens data load failed: {e}")
        return None
    
    # 3. Remove lens data from existing data
    print("\nRemoving lens data from existing data...")
    
    # Find Category_Level_1 column
    level1_col = None
    for col in original_df.columns:
        if 'category' in col.lower() and ('level_1' in col.lower() or 'level1' in col.lower()):
            level1_col = col
            break
    
    if level1_col is None:
        print("Cannot find Category_Level_1 column.")
        print(f"Available columns: {list(original_df.columns)}")
        return None
    
    # Filter only non-lens data
    lens_keywords = ['lens', 'lenses']
    original_df['temp_category_lower'] = original_df[level1_col].astype(str).str.lower()
    non_lens_mask = ~original_df['temp_category_lower'].str.contains('|'.join(lens_keywords), na=False)
    non_lens_df = original_df[non_lens_mask].copy()
    non_lens_df = non_lens_df.drop('temp_category_lower', axis=1)
    
    print(f"Existing non-lens data: {len(non_lens_df)} rows")
    
    # 4. Merge with new lens data
    print("\nMerging with new lens data...")
    
    # Align column structure
    common_columns = list(set(non_lens_df.columns) & set(new_lens_df.columns))
    print(f"Common columns: {common_columns}")
    
    # Merge using only common columns
    merged_df = pd.concat([
        non_lens_df[common_columns],
        new_lens_df[common_columns]
    ], ignore_index=True)
    
    print(f"Merge completed: {len(merged_df)} rows")
    print(f"Non-lens: {len(non_lens_df)} rows")
    print(f"Lens: {len(new_lens_df)} rows")
    
    # 5. Verify results
    print("\n=== Merge Result Verification ===")
    lens_count = len(merged_df[merged_df[level1_col] == 'lens'])
    print(f"Final lens data: {lens_count} rows")
    
    # Check lens data sample
    lens_sample = merged_df[merged_df[level1_col] == 'lens'][['item_code', 'Item', 'Description', level1_col]].head(5)
    print("\nLens data sample:")
    print(lens_sample)
    
    # 6. Save results
    output_file = 'data/inventory_merged_with_lens_update.csv'
    merged_df.to_csv(output_file, index=False)
    print(f"\nMerged data saved successfully: {output_file}")
    
    # Save lens data separately
    lens_only_df = merged_df[merged_df[level1_col] == 'lens']
    lens_output_file = 'data/inventory_lens_only_merged.csv'
    lens_only_df.to_csv(lens_output_file, index=False)
    print(f"Lens data saved separately: {lens_output_file}")
    
    return merged_df

def main():
    # Execute merge
    merged_df = merge_inventory_with_lens_update()
    
    if merged_df is not None:
        print("\n=== Merge Process Completed ===")
        print(f"Final data: {len(merged_df)} rows")
        print("Files saved successfully.")
    else:
        print("\nMerge process failed.")

if __name__ == "__main__":
    main() 