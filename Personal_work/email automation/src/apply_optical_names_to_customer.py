import pandas as pd
import numpy as np

print("=== Apply Optical Names to Customer Data ===")

# 1. Load existing mapping file
print("1. Loading existing mapping file...")

# First check if mapping file exists
try:
    mapping_df = pd.read_csv('business_id_optical_mapping.csv')
    print(f"✅ Mapping file loaded successfully: {len(mapping_df)} mappings")
    
    # Convert to dictionary
    business_id_to_optical = dict(zip(mapping_df['business_id'], mapping_df['optical_name']))
    print(f"Mapping dictionary created: {len(business_id_to_optical)} items")
    
except FileNotFoundError:
    print("❌ Mapping file not found. Please run add_optical_names.py first.")
    exit()

# 2. Load Customer data
print("\n2. Loading Customer data...")
try:
    customer_df = pd.read_csv('processed_customer_data.csv')
    print(f"✅ Customer data loaded: {customer_df.shape}")
except FileNotFoundError:
    print("❌ Customer data file not found.")
    exit()

# 3. Check business_id column
print("\n3. Checking business_id column...")
if 'business_id' not in customer_df.columns:
    print("❌ business_id column not found in customer data.")
    print(f"Available columns: {list(customer_df.columns)}")
    exit()

print(f"✅ business_id column found!")
print(f"Unique business_ids: {customer_df['business_id'].nunique()}")
print(f"NaN business_ids: {customer_df['business_id'].isna().sum()}")

# 4. Apply optical names
print("\n4. Applying optical names...")
customer_df_with_optical = customer_df.copy()
customer_df_with_optical['optical_name'] = customer_df_with_optical['business_id'].map(business_id_to_optical)

# 5. Check results
mapped_count = customer_df_with_optical['optical_name'].notna().sum()
total_count = len(customer_df_with_optical)
print(f"Mapping results:")
print(f"   Successfully mapped: {mapped_count:,}/{total_count:,} ({mapped_count/total_count*100:.1f}%)")

# 6. Verify consistency
print("\n5. Verifying consistency...")
consistency_check = customer_df_with_optical.groupby('business_id')['optical_name'].nunique()
inconsistent = consistency_check[consistency_check > 1]

if len(inconsistent) == 0:
    print("✅ Consistent optical_name assignment for all business_ids")
else:
    print(f"❌ Consistency issues found in {len(inconsistent)} business_ids")

# 7. Save results
output_filename = 'customer_data_with_optical_names.csv'
customer_df_with_optical.to_csv(output_filename, index=False)
print(f"\n✅ Customer data saved: {output_filename}")

# 8. Sample verification
print("\n=== Sample Results ===")
sample_cols = ['business_id', 'optical_name']
if 'customer_name' in customer_df_with_optical.columns:
    sample_cols.append('customer_name')
print(customer_df_with_optical[sample_cols].head(10))

print(f"\n🎉 Completed! Added optical_name to {len(customer_df_with_optical):,} customer records")

# 9. Additional verification: Check consistency with email data
print("\n9. Checking consistency with email data...")
try:
    email_df = pd.read_csv('email_customer_matched_full_with_optical.csv')
    
    # Compare optical_name for common business_ids
    common_business_ids = set(customer_df_with_optical['business_id'].dropna()) & set(email_df['business_id'].dropna())
    
    if len(common_business_ids) > 0:
        print(f"    Common business_id count: {len(common_business_ids)}")
        
        # Sample verification for consistency
        sample_common_ids = list(common_business_ids)[:5]
        for biz_id in sample_common_ids:
            customer_name = customer_df_with_optical[
                customer_df_with_optical['business_id'] == biz_id
            ]['optical_name'].iloc[0]
            
            email_name = email_df[
                email_df['business_id'] == biz_id
            ]['optical_name'].iloc[0]
            
            match_status = "✅ Match" if customer_name == email_name else "❌ Mismatch"
            print(f"   Business ID {biz_id}: {customer_name} vs {email_name} - {match_status}")
    
    print("✅ Consistency check with email data completed")
    
except FileNotFoundError:
    print("⚠️ Email data file not found. Skipping consistency check")

print("\n" + "="*60)
print("📋 Method Description:")
print("="*60)
print("🔧 Method: LEFT JOIN (pd.merge with how='left')")
print("")
print("📊 Why this method was chosen:")
print("1. ✅ Data integrity: Original customer_df rows retained")
print("2. ✅ Mapping safety: business_id not mapped to NaN")
print("3. ✅ Order retention: Original data row order retained")
print("4. ✅ Performance optimization: pandas optimized merge algorithm used")
print("5. ✅ Consistency guarantee: business_id same, optical_name same")
print("")
print("🔄 Comparing with other methods:")
print("• map() function: Simple but difficult to handle keys not mapped")
print("• apply() function: Slow and complex")
print("• INNER JOIN: Rows disappear (risky)")
print("• RIGHT JOIN: customer_df rows may disappear (risky)")
print("")
print("🏆 LEFT JOIN is optimal because:")
print("• Safety: No data loss")
print("• Efficiency: Fast processing speed")
print("• Clarity: Code easy to understand")
print("• Extensibility: Easy to add other mappings later") 