import pandas as pd
import re
import numpy as np

def extract_business_info(account_no):
    """
    Extract base business number and account type from account number.
    
    What: Parses account numbers to extract the numeric base, optional suffix, and maps to account type.
    Why: Business/account IDs are often encoded in a single string; splitting them enables grouping and analysis.
    How: Uses regex to split number and suffix, then maps suffix to type.
    Alternative: Could use more advanced parsing for non-standard formats, but regex covers most cases here.
    """
    if pd.isna(account_no):
        return None, None, None
    
    account_str = str(account_no)
    
    # Find alphabetic suffix pattern
    match = re.match(r'(\d+)([A-Za-z]*)$', account_str)
    
    if match:
        base_number = match.group(1)  # Base number (e.g., 1341)
        suffix = match.group(2).upper() if match.group(2) else ''  # Suffix (e.g., A, F, K, S, E)
        
        # Account type classification
        account_type_map = {
            'A': 'Accessory',
            'F': 'Frame', 
            'K': 'Surface',
            'S': 'Brand Lens',
            'E': 'Edging',
            '': 'Lens'  # No suffix means lens
        }
        
        account_type = account_type_map.get(suffix, 'Other')
        
        return base_number, suffix, account_type
    
    return account_str, '', 'Unknown'

def clean_customer_name(customer_name):
    """
    Clean customer name by removing account number suffixes.
    
    What: Removes trailing account numbers (e.g., '#1341') from customer names.
    Why: Enables matching and deduplication by standardizing names.
    How: Uses regex to remove trailing patterns.
    Alternative: For more complex patterns, Named Entity Recognition could be used, but regex is fast and interpretable.
    """
    if pd.isna(customer_name):
        return None
    
    # Remove "#number" pattern (e.g., "1001 OPTICAL #1341" -> "1001 OPTICAL")
    clean_name = re.sub(r'\s*#\d+[A-Za-z]*$', '', str(customer_name))
    return clean_name.strip()

def process_all_dataframes():
    """
    Add cleaned customer names and business_id to all DataFrames.
    
    What: Loads customer and sales data, cleans names, extracts business/account info, and maps business_id across datasets.
    Why: Standardizes customer naming and enables unified business/account analysis.
    How: Loads, cleans, maps, and prints only shape/columns for privacy.
    Alternative: Could use fuzzy matching for partial matches, but exact match is transparent and fast for initial checks.
    """
    print("Loading CSV files...")
    df = pd.read_csv('cc (1).csv')
    df3 = pd.read_csv('s_by_c.CSV')
    print(f"df: {df.shape}, columns: {list(df.columns)}")
    print(f"df3: {df3.shape}, columns: {list(df3.columns)}")
    
    # df processing
    print("\n=== Processing df... ===")
    
    # 1. Add clean customer name
    df['clean_customer_name'] = df['Customer'].apply(clean_customer_name)
    
    # 2. Process account numbers
    account_info = df['clean_customer_name'].apply(lambda x: re.search(r'(\d+)([A-Za-z]*)$', str(x)) if pd.notna(x) else None)
    df['account_number'] = account_info.apply(lambda x: x.group(1) if x else None)
    df['suffix'] = account_info.apply(lambda x: x.group(2).upper() if x and x.group(2) else '')
    
    # 3. Generate unique business ID
    df['business_id'] = df['account_number'].fillna('unknown')

    # 4. Additional columns for data analysis
    df['is_main_account'] = df['suffix'] == ''  # Main account flag (lens)
    df['account_type'] = df['suffix'].map({'A': 'Frame', 'F': 'Frame', 'K': 'Frame', 'S': 'Frame', 'E': 'Frame', '': 'Lens'})
    
    print(f"df unique businesses: {df['business_id'].nunique()}")
    
    # df3 processing
    print("\n=== Processing df3... ===")
    
    # 1. Add clean customer name
    df3['clean_customer_name'] = df3['Name'].apply(clean_customer_name)
    
    # 2. Create mapping from df's clean customer name and business_id
    customer_business_map = df[['clean_customer_name', 'business_id']].drop_duplicates()
    
    # 3. Apply mapping
    df3 = df3.merge(customer_business_map, 
                   left_on='clean_customer_name', 
                   right_on='clean_customer_name', 
                   how='left')
    
    # Check mapping results
    unmapped_count = df3['business_id'].isna().sum()
    total_count = len(df3)
    
    print(f"=== df3 Mapping Results ===")
    print(f"Total records: {total_count}")
    print(f"Mapped records: {total_count - unmapped_count}")
    print(f"Unmapped records: {unmapped_count}")
    print(f"Mapping success rate: {((total_count - unmapped_count) / total_count * 100):.2f}%")
    
    # Check unmapped customer names
    if unmapped_count > 0:
        unmapped_customers = df3[df3['business_id'].isna()]['clean_customer_name'].unique()
        print(f"\nUnmapped customer names (top 10):")
        for customer in unmapped_customers[:10]:
            print(f"  - {customer}")
    
    print(f"df: {df.shape}, columns: {list(df.columns)}")
    print(f"df3: {df3.shape}, columns: {list(df3.columns)}")
    return df, df3

def verify_clean_names(df, df3):
    """
    Verify that clean customer name processing was done correctly
    """
    print("\n=== Clean Customer Name Processing Verification ===")
    
    # df verification
    print("=== df Verification ===")
    print(f"Original Customer column unique values: {df['Customer'].nunique()}")
    print(f"Clean customer name unique values: {df['clean_customer_name'].nunique()}")
    
    # Sample comparison
    print("\n=== df Customer Name Transformation Sample ===")
    sample_df = df[['Customer', 'clean_customer_name', 'Account No.', 'business_id']].head(10)
    for idx, row in sample_df.iterrows():
        print(f"Original: {row['Customer']}")
        print(f"Clean name: {row['clean_customer_name']}")
        print(f"Account number: {row['Account No.']}")
        print(f"Business ID: {row['business_id']}")
        print("-" * 50)
    
    # df3 verification
    print("\n=== df3 Verification ===")
    print(f"Original Name column unique values: {df3['Name'].nunique()}")
    print(f"Clean customer name unique values: {df3['clean_customer_name'].nunique()}")
    
    # Sample comparison
    print("\n=== df3 Customer Name Transformation Sample ===")
    sample_df3 = df3[['Name', 'clean_customer_name', 'business_id']].head(10)
    for idx, row in sample_df3.iterrows():
        print(f"Original: {row['Name']}")
        print(f"Clean name: {row['clean_customer_name']}")
        print(f"Business ID: {row['business_id']}")
        print("-" * 50)
    
    # Mapping success rate
    df3_mapped_records = df3['business_id'].notna().sum()
    df3_total_records = len(df3)
    success_rate = (df3_mapped_records / df3_total_records) * 100
    
    print(f"\n=== Final Mapping Success Rate ===")
    print(f"df3 mapping success rate: {success_rate:.2f}%")
    
    return success_rate > 80

if __name__ == "__main__":
    # Process all DataFrames
    df_processed, df3_processed = process_all_dataframes()
    
    # Verification
    mapping_success = verify_clean_names(df_processed, df3_processed)
    
    if mapping_success:
        print("\n✅ All processing completed successfully!")
    else:
        print("\n⚠️ There are some issues with mapping. Please check unmapped records.")
    
    # Save results
    df_processed.to_csv('processed_df_with_clean_names.csv', index=False)
    df3_processed.to_csv('processed_df3_with_clean_names.csv', index=False)
    
    print("\n=== Processed Files Saved ===")
    print("- processed_df_with_clean_names.csv: df with clean customer names added")
    print("- processed_df3_with_clean_names.csv: df3 with clean customer names and business_id added")
    
    # Final statistics
    print("\n=== Final Statistics ===")
    print(f"df shape: {df_processed.shape}")
    print(f"df3 shape: {df3_processed.shape}")
    print(f"df unique businesses: {df_processed['business_id'].nunique()}")
    print(f"df3 mapped unique businesses: {df3_processed['business_id'].dropna().nunique()}")
    print(f"df clean customer name unique values: {df_processed['clean_customer_name'].nunique()}")
    print(f"df3 clean customer name unique values: {df3_processed['clean_customer_name'].nunique()}")
    
    # Multiple accounts for same business example
    print("\n=== Multiple Accounts for Same Business Example ===")
    business_groups = df_processed.groupby('business_id').agg({
        'clean_customer_name': 'first',
        'Customer': list,
        'Account No.': list,
        'account_type': list
    }).head(5)

    for idx, row in business_groups.iterrows():
        print(f"\nBusiness ID: {idx}")
        print(f"Clean customer name: {row['clean_customer_name']}")
        print(f"Original customer names: {row['Customer']}")
        print(f"Accounts: {row['Account No.']}")
        print(f"Account types: {row['account_type']}") 