# Code to run in Jupyter notebook
# Copy this code to a new cell and run it

import pandas as pd
import re

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

# Add business information to df (skip this part if already processed)
if 'business_id' not in df.columns:
    print("=== Processing df account numbers... ===")
    
    # Extract business information from Account No.
    df[['base_account', 'suffix', 'account_type']] = df['Account No.'].apply(
        lambda x: pd.Series(extract_business_info(x))
    )
    
    # Generate unique business ID
    unique_base_accounts = df['base_account'].dropna().unique()
    business_id_map = {base: f"BUS_{i+1:04d}" for i, base in enumerate(sorted(unique_base_accounts))}
    
    # Add business ID
    df['business_id'] = df['base_account'].map(business_id_map)
    
    # Additional columns for data analysis
    df['is_main_account'] = df['suffix'] == ''  # Main account flag (lens)
    df['has_multiple_accounts'] = df.groupby('business_id')['business_id'].transform('count') > 1

print(f"df unique businesses: {df['business_id'].nunique()}")
print(f"df original Customer unique values: {df['Customer'].nunique()}")
print(f"df clean customer name unique values: {df['clean_customer_name'].nunique()}")

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

print(f"df3 original Name unique values: {df3['Name'].nunique()}")
print(f"df3 clean customer name unique values: {df3['clean_customer_name'].nunique()}")

# Check unmapped customer names
if unmapped_count > 0:
    unmapped_customers = df3[df3['business_id'].isna()]['clean_customer_name'].unique()
    print(f"\nUnmapped customer names (top 10):")
    for customer in unmapped_customers[:10]:
        print(f"  - {customer}")

# Verification
print("\n=== Clean Customer Name Processing Verification ===")

# df verification
print("=== df Customer Name Transformation Sample ===")
sample_df = df[['Customer', 'clean_customer_name', 'Account No.', 'business_id']].head(10)
for idx, row in sample_df.iterrows():
    print(f"Original: {row['Customer']}")
    print(f"Clean name: {row['clean_customer_name']}")
    print(f"Account number: {row['Account No.']}")
    print(f"Business ID: {row['business_id']}")
    print("-" * 50)

# df3 verification
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

if success_rate > 80:
    print("\n✅ All processing completed successfully!")
else:
    print("\n⚠️ There are some issues with mapping. Please check unmapped records.")

# Final statistics
print("\n=== Final Statistics ===")
print(f"df shape: {df.shape}")
print(f"df3 shape: {df3.shape}")
print(f"df unique businesses: {df['business_id'].nunique()}")
print(f"df3 mapped unique businesses: {df3['business_id'].dropna().nunique()}")
print(f"df clean customer name unique values: {df['clean_customer_name'].nunique()}")
print(f"df3 clean customer name unique values: {df3['clean_customer_name'].nunique()}")

# Multiple accounts for same business example
print("\n=== Multiple Accounts for Same Business Example ===")
business_groups = df.groupby('business_id').agg({
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

# Processed data preview
print("\n=== df Processed Data Preview ===")
df_display_columns = ['Customer', 'clean_customer_name', 'Account No.', 'business_id', 'account_type', 'is_main_account']
print(df[df_display_columns].head(15))

print("\n=== df3 Processed Data Preview ===")
df3_display_columns = ['Name', 'clean_customer_name', 'business_id', 'Type', 'Date', 'Amount']
print(df3[df3_display_columns].head(15))

# Business record count distribution (df3)
print("\n=== df3 Business Record Count Distribution (Top 10) ===")
business_record_counts = df3.groupby('business_id').size().sort_values(ascending=False).head(10)
for business_id, count in business_record_counts.items():
    customer_name = df3[df3['business_id'] == business_id]['clean_customer_name'].iloc[0]
    print(f"{business_id} ({customer_name}): {count} records") 