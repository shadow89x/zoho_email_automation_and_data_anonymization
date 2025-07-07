# Code to run in Jupyter notebook
# Copy this code to a new cell and run it

import pandas as pd
import re

def clean_customer_name(name):
    """
    Clean customer name by removing patterns and extracting account information
    """
    if pd.isna(name):
        return name
    
    name = str(name).strip()
    
    # Remove "#number" pattern (e.g., "1001 OPTICAL #1341" -> "1001 OPTICAL")
    name = re.sub(r'#\d+', '', name).strip()
    
    # Remove extra spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def extract_account_info(customer_name):
    """
    Extract account number and suffix from customer name
    """
    if pd.isna(customer_name):
        return None, None, None
    
    customer_name = str(customer_name).strip()
    
    # Find alphabetic suffix pattern
    pattern = r'(\d+)([A-Za-z]*)$'
    match = re.search(pattern, customer_name)
    
    if match:
        base_number = match.group(1)  # Base number (e.g., 1341)
        suffix = match.group(2).upper() if match.group(2) else ''  # Suffix (e.g., A, F, K, S, E)
        
        # Account type classification
        account_type_map = {
            'A': 'Frame',
            'F': 'Frame', 
            'K': 'Frame',
            'S': 'Frame',
            'E': 'Frame',
            '': 'Lens'  # No suffix means lens
        }
        
        account_type = account_type_map.get(suffix, 'Unknown')
        return base_number, suffix, account_type
    
    return None, None, None

# Example usage
print("=== Customer Name Cleaning Example ===")
sample_names = [
    "1001 OPTICAL #1341",
    "1001 OPTICAL A",
    "1001 OPTICAL F",
    "1001 OPTICAL K",
    "1001 OPTICAL S",
    "1001 OPTICAL E",
    "1001 OPTICAL"
]

for name in sample_names:
    clean_name = clean_customer_name(name)
    account_info = extract_account_info(clean_name)
    print(f"Original: {name}")
    print(f"Clean: {clean_name}")
    print(f"Account Info: {account_info}")
    print("-" * 40)

# df processing
print("=== Processing df... ===")

# 1. Add clean customer name
df['customer_name_clean'] = df['Customer'].apply(lambda x: str(x).strip() if pd.notna(x) else x)

# 2. Process account numbers (skip if already processed)
if 'business_id' not in df.columns:
    df[['base_account', 'suffix', 'account_type']] = df['Account No.'].apply(
        lambda x: pd.Series(extract_account_info(x))
    )

# 3. Generate unique business ID
df['business_id'] = df['base_account'].fillna('unknown')

# 4. Additional columns for data analysis
df['is_main_account'] = df['suffix'] == ''  # Main account flag (lens)
df['account_type'] = df['suffix'].map({'A': 'Frame', 'F': 'Frame', 'K': 'Frame', 'S': 'Frame', 'E': 'Frame', '': 'Lens'})

print(f"df unique businesses: {df['business_id'].nunique()}")
print(f"df original Customer unique values: {df['Customer'].nunique()}")
print(f"df clean customer name unique values: {df['customer_name_clean'].nunique()}")

# df3 processing
print("\n=== Processing df3... ===")

# 1. Add clean customer name
df3['customer_name_clean'] = df3['Name'].apply(lambda x: str(x).strip() if pd.notna(x) else x)

# 2. Create mapping from df's clean customer name and business_id
customer_business_map = df[['customer_name_clean', 'business_id']].drop_duplicates()

# 3. Apply mapping
df3 = df3.merge(customer_business_map, 
               left_on='customer_name_clean', 
               right_on='customer_name_clean', 
               how='left')

# Check mapping results
mapped_count = df3['business_id'].notna().sum()
total_count = len(df3)
print(f"df3 mapping results:")
print(f"   Successfully mapped: {mapped_count:,}/{total_count:,} ({mapped_count/total_count*100:.1f}%)")

# Check unmapped customer names
unmapped_count = df3['business_id'].isna().sum()
if unmapped_count > 0:
    unmapped_customers = df3[df3['business_id'].isna()]['customer_name_clean'].unique()
    print(f"\nUnmapped customer names (top 10):")
    for customer in unmapped_customers[:10]:
        print(f"  - {customer}")

# Verification
print("\n=== df Verification ===")
print(f"Original Customer column unique values: {df['Customer'].nunique()}")
print(f"Clean customer name unique values: {df['customer_name_clean'].nunique()}")

# Sample comparison
print("\n=== df Customer Name Transformation Sample ===")
sample_df = df[['Customer', 'customer_name_clean', 'Account No.', 'business_id']].head(10)
for idx, row in sample_df.iterrows():
    print(f"Original: {row['Customer']}")
    print(f"Clean name: {row['customer_name_clean']}")
    print(f"Account number: {row['Account No.']}")
    print(f"Business ID: {row['business_id']}")
    print("-" * 50)

print("\n=== df3 Verification ===")
print(f"Original Name column unique values: {df3['Name'].nunique()}")
print(f"Clean customer name unique values: {df3['customer_name_clean'].nunique()}")

# Sample comparison
print("\n=== df3 Customer Name Transformation Sample ===")
sample_df3 = df3[['Name', 'customer_name_clean', 'business_id']].head(10)
for idx, row in sample_df3.iterrows():
    print(f"Original: {row['Name']}")
    print(f"Clean name: {row['customer_name_clean']}")
    print(f"Business ID: {row['business_id']}")
    print("-" * 50)

# Final summary
print("\n=== Final Summary ===")
print(f"df unique businesses: {df['business_id'].nunique()}")
print(f"df3 mapped unique businesses: {df3['business_id'].dropna().nunique()}")
print(f"df clean customer name unique values: {df['customer_name_clean'].nunique()}")
print(f"df3 clean customer name unique values: {df3['customer_name_clean'].nunique()}")

# Multiple accounts for same business example
print("\n=== Multiple Accounts for Same Business Example ===")
business_groups = df.groupby('business_id').agg({
    'customer_name_clean': 'first',
    'Customer': list,
    'Account No.': list,
    'suffix': list
}).head(5)

for idx, row in business_groups.iterrows():
    print(f"\nBusiness ID: {idx}")
    print(f"Clean customer name: {row['customer_name_clean']}")
    print(f"Original customer names: {row['Customer']}")
    print(f"Accounts: {row['Account No.']}")
    print(f"Suffixes: {row['suffix']}")
    print("-" * 50)

# df processing
print("=== Processing df... ===")

# 1. Add clean customer name
df['customer_name_clean'] = df['Customer'].apply(clean_customer_name)

# 2. Process account numbers (skip if already processed)
if 'business_id' not in df.columns:
    df[['base_account', 'suffix', 'account_type']] = df['Account No.'].apply(
        lambda x: pd.Series(extract_account_info(x))
    )

    # 3. Generate unique business ID
    unique_base_accounts = df['base_account'].dropna().unique()
    business_id_map = {base: f"BUS_{i+1:04d}" for i, base in enumerate(sorted(unique_base_accounts))}

    # 4. Add business ID
    df['business_id'] = df['base_account'].map(business_id_map)

    # 5. Additional columns for data analysis
    df['is_main_account'] = df['suffix'] == ''  # Main account flag (lens)
    df['has_multiple_accounts'] = df.groupby('business_id')['business_id'].transform('count') > 1

print(f"df unique businesses: {df['business_id'].nunique()}")
print(f"df original Customer unique values: {df['Customer'].nunique()}")
print(f"df clean customer name unique values: {df['customer_name_clean'].nunique()}")

# df3 processing
print("\n=== Processing df3... ===")

# 1. Add clean customer name
df3['customer_name_clean'] = df3['Name'].apply(clean_customer_name)

# 2. Create mapping from df's clean customer name and business_id
customer_business_map = df[['customer_name_clean', 'business_id']].drop_duplicates()

# 3. Apply mapping
df3 = df3.merge(customer_business_map, 
               left_on='customer_name_clean', 
               right_on='customer_name_clean', 
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
print(f"df3 clean customer name unique values: {df3['customer_name_clean'].nunique()}")

# Check unmapped customer names
if unmapped_count > 0:
    unmapped_customers = df3[df3['business_id'].isna()]['customer_name_clean'].unique()
    print(f"\nUnmapped customer names (top 10):")
    for customer in unmapped_customers[:10]:
        print(f"  - {customer}")

# Verification
print("\n=== Clean Customer Name Processing Verification ===")

# df verification
print("=== df Customer Name Transformation Sample ===")
sample_df = df[['Customer', 'customer_name_clean', 'Account No.', 'business_id']].head(10)
for idx, row in sample_df.iterrows():
    print(f"Original: {row['Customer']}")
    print(f"Clean name: {row['customer_name_clean']}")
    print(f"Account number: {row['Account No.']}")
    print(f"Business ID: {row['business_id']}")
    print("-" * 50)

# df3 verification
print("\n=== df3 Customer Name Transformation Sample ===")
sample_df3 = df3[['Name', 'customer_name_clean', 'business_id']].head(10)
for idx, row in sample_df3.iterrows():
    print(f"Original: {row['Name']}")
    print(f"Clean name: {row['customer_name_clean']}")
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
print(f"df clean customer name unique values: {df['customer_name_clean'].nunique()}")
print(f"df3 clean customer name unique values: {df3['customer_name_clean'].nunique()}")

# Multiple accounts for same business example
print("\n=== Multiple Accounts for Same Business Example ===")
business_groups = df.groupby('business_id').agg({
    'customer_name_clean': 'first',
    'Customer': list,
    'Account No.': list,
    'account_type': list
}).head(5)

for idx, row in business_groups.iterrows():
    print(f"\nBusiness ID: {idx}")
    print(f"Clean customer name: {row['customer_name_clean']}")
    print(f"Original customer names: {row['Customer']}")
    print(f"Accounts: {row['Account No.']}")
    print(f"Account types: {row['account_type']}")

# Processed data preview
print("\n=== df Processed Data Preview ===")
df_display_columns = ['Customer', 'customer_name_clean', 'Account No.', 'business_id', 'account_type', 'is_main_account']
print(df[df_display_columns].head(15))

print("\n=== df3 Processed Data Preview ===")
df3_display_columns = ['Name', 'customer_name_clean', 'business_id', 'Type', 'Date', 'Amount']
print(df3[df3_display_columns].head(15))

# Business record count distribution (df3)
print("\n=== df3 Business Record Count Distribution (Top 10) ===")
business_record_counts = df3.groupby('business_id').size().sort_values(ascending=False).head(10)
for business_id, count in business_record_counts.items():
    customer_name = df3[df3['business_id'] == business_id]['customer_name_clean'].iloc[0]
    print(f"{business_id} ({customer_name}): {count} records") 