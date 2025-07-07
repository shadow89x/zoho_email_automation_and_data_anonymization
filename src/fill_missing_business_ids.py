import pandas as pd
import numpy as np
import random

def fill_missing_business_ids():
    """
    Fill missing business_ids in email data using customer data
    """
    print("=== Fill Missing Business IDs Process ===")
    
    # 1. Load data
    print("1. Loading data...")
    try:
        email_df = pd.read_csv('email_customer_matched_full.csv')
        customer_df = pd.read_csv('processed_customer_data.csv')
        print(f"✅ Email data loaded: {email_df.shape}")
        print(f"✅ Customer data loaded: {customer_df.shape}")
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return None
    
    # 2. Check business_id status
    print("\n2. Checking business_id status...")
    email_na_count = email_df['business_id'].isna().sum()
    email_total = len(email_df)
    customer_na_count = customer_df['business_id'].isna().sum()
    customer_total = len(customer_df)
    
    print(f"Email DataFrame:")
    print(f"   Total rows: {email_total:,}")
    print(f"   NaN business_id: {email_na_count:,} ({email_na_count/email_total*100:.1f}%)")
    print(f"   Valid business_id: {email_total - email_na_count:,}")
    
    print(f"\nCustomer DataFrame:")
    print(f"   Total rows: {customer_total:,}")
    print(f"   NaN business_id: {customer_na_count:,} ({customer_na_count/customer_total*100:.1f}%)")
    print(f"   Valid business_id: {customer_total - customer_na_count:,}")
    
    # 3. Get available business_ids from customer data
    print("\n3. Getting available business_ids from customer data...")
    available_business_ids = customer_df['business_id'].dropna().unique()
    used_business_ids = email_df['business_id'].dropna().unique()
    
    print(f"Available business_ids in customer data: {len(available_business_ids):,}")
    print(f"Already used business_ids in email data: {len(used_business_ids):,}")
    
    # 4. Find unused business_ids
    unused_business_ids = set(available_business_ids) - set(used_business_ids)
    print(f"Unused business_ids available: {len(unused_business_ids):,}")
    
    if len(unused_business_ids) == 0:
        print("❌ No unused business_ids available for filling")
        return email_df
    
    # 5. Fill missing business_ids
    print("\n4. Filling missing business_ids...")
    email_df_filled = email_df.copy()
    
    # Get rows with missing business_id
    missing_mask = email_df_filled['business_id'].isna()
    missing_count = missing_mask.sum()
    
    print(f"Rows with missing business_id: {missing_count:,}")
    
    if missing_count > len(unused_business_ids):
        print(f"⚠️ Warning: More missing rows ({missing_count:,}) than available business_ids ({len(unused_business_ids):,})")
        print("Will reuse business_ids if necessary")
    
    # Convert to list for random selection
    unused_list = list(unused_business_ids)
    
    # Set seed for reproducible results
    np.random.seed(42)
    random.seed(42)
    
    # Fill missing business_ids
    filled_count = 0
    for idx in email_df_filled[missing_mask].index:
        if len(unused_list) > 0:
            # Select random unused business_id
            selected_business_id = np.random.choice(unused_list)
            unused_list.remove(selected_business_id)
        else:
            # If no unused business_ids left, reuse from available pool
            selected_business_id = np.random.choice(list(available_business_ids))
        
        email_df_filled.loc[idx, 'business_id'] = selected_business_id
        filled_count += 1
    
    print(f"✅ Filled {filled_count:,} missing business_ids")
    
    # 6. Verify results
    print("\n5. Verifying results...")
    final_na_count = email_df_filled['business_id'].isna().sum()
    final_total = len(email_df_filled)
    
    print(f"Final status:")
    print(f"   Total rows: {final_total:,}")
    print(f"   NaN business_id: {final_na_count:,} ({final_na_count/final_total*100:.1f}%)")
    print(f"   Valid business_id: {final_total - final_na_count:,}")
    
    if final_na_count == 0:
        print("✅ All missing business_ids successfully filled!")
    else:
        print(f"⚠️ Still {final_na_count:,} rows with missing business_id")
    
    # 7. Check business_id distribution
    print("\n6. Checking business_id distribution...")
    business_id_counts = email_df_filled['business_id'].value_counts()
    print(f"Business_id distribution:")
    print(f"   Unique business_ids: {len(business_id_counts)}")
    print(f"   Average emails per business_id: {business_id_counts.mean():.1f}")
    print(f"   Max emails per business_id: {business_id_counts.max()}")
    print(f"   Min emails per business_id: {business_id_counts.min()}")
    
    # 8. Save results
    output_filename = 'email_customer_matched_full_filled.csv'
    email_df_filled.to_csv(output_filename, index=False)
    print(f"\n✅ Filled data saved: {output_filename}")
    
    return email_df_filled

def main():
    # Execute the filling process
    filled_df = fill_missing_business_ids()
    
    if filled_df is not None:
        print("\n=== Process Completed ===")
        print(f"Final data shape: {filled_df.shape}")
        print("All missing business_ids have been filled successfully!")
    else:
        print("\nProcess failed. Please check the error messages above.")

if __name__ == "__main__":
    main() 