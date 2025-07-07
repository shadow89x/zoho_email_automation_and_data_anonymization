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

# Add business information to df
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

# Account type distribution
print("\n=== Account Type Distribution ===")
account_type_counts = df['account_type'].value_counts()
for account_type, count in account_type_counts.items():
    print(f"{account_type}: {count} accounts")

# Business analysis
print("\n=== Business Analysis ===")
print(f"Total unique businesses: {df['business_id'].nunique()}")
print(f"Businesses with multiple accounts: {df['has_multiple_accounts'].sum()}")

# Sample business data
print("\n=== Sample Business Data ===")
sample_businesses = df[['Customer', 'Account No.', 'business_id', 'account_type', 'is_main_account']].head(10)
for idx, row in sample_businesses.iterrows():
    print(f"Customer: {row['Customer']}")
    print(f"Account: {row['Account No.']}")
    print(f"Business ID: {row['business_id']}")
    print(f"Account Type: {row['account_type']}")
    print(f"Main Account: {row['is_main_account']}")
    print("-" * 50)

# Multiple accounts analysis
print("\n=== Multiple Accounts Analysis ===")
businesses_with_multiple = df[df['has_multiple_accounts']].groupby('business_id').agg({
    'Customer': list,
    'Account No.': list,
    'account_type': list
}).head(5)

for idx, row in businesses_with_multiple.iterrows():
    print(f"\nBusiness ID: {idx}")
    print(f"Customers: {row['Customer']}")
    print(f"Accounts: {row['Account No.']}")
    print(f"Account Types: {row['account_type']}")

# Account type summary
print("\n=== Account Type Summary ===")
for account_type in df['account_type'].unique():
    if pd.notna(account_type):
        count = len(df[df['account_type'] == account_type])
        percentage = (count / len(df)) * 100
        print(f"{account_type}: {count} accounts ({percentage:.1f}%)")

# Main account analysis
print("\n=== Main Account Analysis ===")
main_accounts = df[df['is_main_account']]
print(f"Main accounts (lens): {len(main_accounts)}")
print(f"Sub accounts (frames/accessories): {len(df) - len(main_accounts)}")

# Business ID format verification
print("\n=== Business ID Format Verification ===")
business_ids = df['business_id'].unique()
print(f"Business ID format: BUS_XXXX")
print(f"Sample business IDs: {business_ids[:5]}")

# Data quality check
print("\n=== Data Quality Check ===")
null_accounts = df['Account No.'].isna().sum()
null_business_ids = df['business_id'].isna().sum()
print(f"Null account numbers: {null_accounts}")
print(f"Null business IDs: {null_business_ids}")

if null_accounts == 0 and null_business_ids == 0:
    print("✅ Data quality is good - no null values in key fields")
else:
    print("⚠️ Some null values found - please investigate")

# Final summary
print("\n=== Final Summary ===")
print(f"Total records: {len(df)}")
print(f"Unique businesses: {df['business_id'].nunique()}")
print(f"Unique customers: {df['Customer'].nunique()}")
print(f"Account types: {df['account_type'].nunique()}")
print(f"Main accounts: {df['is_main_account'].sum()}")
print(f"Multiple account businesses: {df['has_multiple_accounts'].sum()}") 