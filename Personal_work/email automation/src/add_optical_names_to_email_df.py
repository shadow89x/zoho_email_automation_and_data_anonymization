#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to add business_id-based Optical names to Email DataFrame

This script generates consistent random optical names for each business_id in email_df
and adds them as a new 'optical_name' column.

Features:
- Same business_id always gets the same optical name
- Guaranteed identical results on re-execution (seed fixed)
- Generates unique names without duplicates
"""

import pandas as pd
import numpy as np
import random
from tqdm import tqdm
import os
import warnings
warnings.filterwarnings('ignore')

def load_email_dataframe():
    """Load email_df from multiple possible paths"""
    possible_paths = [
        'G:/VSCode/Personal_work/email automation/data/processed_email_data_with_customer_info.csv',
        'notebooks/email_df_anon.csv',
        'email_df_anon.csv',
        'G:/VSCode/Personal_work/email automation/notebooks/email_df_anon.csv',
        'data/processed_email_data_with_customer_info.csv'
    ]
    
    print("=== Loading Data ===")
    
    for path in possible_paths:
        try:
            if os.path.exists(path):
                email_df = pd.read_csv(path)
                print(f"✅ Data loaded successfully: {path}")
                return email_df, path
            else:
                print(f"❌ File not found: {path}")
        except Exception as e:
            print(f"❌ Load error {path}: {e}")
    
    # Manual input
    print("\n⚠️ Cannot find file automatically.")
    manual_path = input("Enter email_df CSV file path: ")
    email_df = pd.read_csv(manual_path)
    return email_df, manual_path

def generate_optical_names():
    """Word lists for generating Optical names"""
    adjectives = [
        'Bright', 'Clear', 'Crystal', 'Diamond', 'Elite', 'Golden', 'Grand', 'Happy', 
        'Perfect', 'Prime', 'Pure', 'Royal', 'Sharp', 'Smart', 'Sunny', 'Supreme',
        'Ultra', 'Vision', 'Vivid', 'Wonder', 'Alpha', 'Beta', 'Gamma', 'Delta',
        'Omega', 'Apex', 'Peak', 'Summit', 'Star', 'Nova', 'Cosmic', 'Stellar',
        'Radiant', 'Brilliant', 'Luminous', 'Glowing', 'Shining', 'Gleaming',
        'Sparkling', 'Dazzling', 'Magnificent', 'Excellent', 'Superior', 'Premium',
        'Luxury', 'Elegant', 'Refined', 'Sophisticated', 'Advanced', 'Modern',
        'Future', 'Next', 'Pro', 'Master', 'Expert', 'Precision', 'Focus',
        'Clarity', 'Insight', 'Panorama', 'Spectrum', 'Rainbow', 'Prism',
        'Lens', 'Zoom', 'Wide', 'Macro', 'Micro', 'Mega', 'Super', 'Hyper',
        'Turbo', 'Speed', 'Quick', 'Swift', 'Rapid', 'Fast', 'Instant',
        'Magic', 'Wonder', 'Dream', 'Fantasy', 'Miracle', 'Amazing', 'Awesome',
        'Fantastic', 'Incredible', 'Marvelous', 'Spectacular', 'Stunning',
        'Beautiful', 'Gorgeous', 'Stylish', 'Chic', 'Trendy', 'Fashion',
        'Creative', 'Unique', 'Special', 'Exclusive', 'Limited', 'Rare', 'Quality'
    ]
    
    nouns = [
        'Optical', 'Vision', 'Eye', 'Sight', 'View', 'Focus', 'Lens', 'Frame',
        'Glass', 'Spec', 'Optic', 'Visual', 'See', 'Look', 'Watch', 'Observe',
        'Center', 'Shop', 'Store', 'House', 'Place', 'Zone', 'Point', 'Spot',
        'Hub', 'Base', 'Station', 'Studio', 'Lab', 'Clinic', 'Care', 'Service',
        'Solution', 'System', 'Tech', 'Pro', 'Plus', 'Max', 'Ultra', 'Super',
        'World', 'Land', 'Kingdom', 'Empire', 'Realm', 'Domain', 'Space',
        'Galaxy', 'Universe', 'Cosmos', 'Star', 'Moon', 'Sun', 'Light',
        'Beam', 'Ray', 'Glow', 'Shine', 'Spark', 'Flash', 'Bright', 'Clear'
    ]
    
    return adjectives, nouns

def create_business_id_to_optical_mapping(unique_business_ids, adjectives, nouns):
    """Create consistent optical name mapping for each business_id"""
    print(f"=== Generating Optical Names by business_id ===")
    print(f"Number of unique business_ids to process: {len(unique_business_ids)}")
    
    # Set seed for reproducible results
    np.random.seed(42)
    random.seed(42)
    
    business_id_to_optical = {}
    used_names = set()  # Prevent duplicates
    
    print("Generating Optical names...")
    for i, business_id in enumerate(tqdm(unique_business_ids)):
        # Use unique seed for each business_id for consistent results
        business_seed = hash(str(business_id)) % 2147483647
        np.random.seed(business_seed)
        random.seed(business_seed)
        
        # Generate non-duplicate name
        max_attempts = 100
        for attempt in range(max_attempts):
            adj = np.random.choice(adjectives)
            noun = np.random.choice(nouns)
            optical_name = f"{adj} {noun}"
            
            if optical_name not in used_names:
                used_names.add(optical_name)
                business_id_to_optical[business_id] = optical_name
                break
        else:
            # If still duplicate after 100 attempts, add number
            optical_name = f"{adj} {noun} {i+1}"
            business_id_to_optical[business_id] = optical_name
            used_names.add(optical_name)
    
    print(f"✅ Generated {len(business_id_to_optical)} unique Optical names")
    print(f"Number of non-duplicate names: {len(used_names)}")
    
    return business_id_to_optical

def verify_consistency(email_df_with_optical):
    """Verify consistency"""
    print("=== Consistency Verification ===")
    
    # Check if same business_id has same optical_name
    consistency_check = email_df_with_optical.groupby('business_id')['optical_name'].nunique()
    inconsistent_business_ids = consistency_check[consistency_check > 1]
    
    if len(inconsistent_business_ids) == 0:
        print("✅ Consistent optical_name assignment completed for all business_ids")
    else:
        print(f"❌ Consistency issues found in {len(inconsistent_business_ids)} business_ids:")
        print(inconsistent_business_ids)
    
    # Sample verification
    print("\n=== Sample Verification ===")
    sample_business_ids = email_df_with_optical['business_id'].dropna().unique()[:5]
    
    for business_id in sample_business_ids:
        business_rows = email_df_with_optical[email_df_with_optical['business_id'] == business_id]
        unique_optical_names = business_rows['optical_name'].unique()
        
        print(f"Business ID {business_id}:")
        print(f"  Rows: {len(business_rows)}")
        print(f"  Optical name: {unique_optical_names}")
        print(f"  Consistency: {'✅ OK' if len(unique_optical_names) == 1 else '❌ Issue'}")
        print()

def main():
    """Main function"""
    print("🔧 Starting to add business_id-based Optical names to Email DataFrame")
    print("=" * 60)
    
    # 1. Load data
    email_df, used_path = load_email_dataframe()
    
    print(f"\n=== Data Information ===")
    print(f"DataFrame shape: {email_df.shape}")
    print(f"Columns: {list(email_df.columns)}")
    
    # Check business_id column
    if 'business_id' not in email_df.columns:
        print(f"\n❌ business_id column not found.")
        print(f"Available columns: {list(email_df.columns)}")
        raise ValueError("business_id column is required.")
    
    print(f"\n✅ business_id column found!")
    print(f"Unique business_ids: {email_df['business_id'].nunique()}")
    print(f"NaN business_ids: {email_df['business_id'].isna().sum()}")
    
    # 2. Generate word lists
    adjectives, nouns = generate_optical_names()
    print(f"\nPrepared {len(adjectives)} adjectives, {len(nouns)} nouns")
    print(f"Total possible combinations: {len(adjectives) * len(nouns):,}")
    
    # 3. Create business_id mapping
    unique_business_ids = email_df['business_id'].dropna().unique()
    business_id_to_optical = create_business_id_to_optical_mapping(
        unique_business_ids, adjectives, nouns
    )
    
    # Output sample of generated names
    print("\n=== Sample of Generated Optical Names ===")
    sample_items = list(business_id_to_optical.items())[:10]
    for business_id, optical_name in sample_items:
        print(f"Business ID {business_id}: {optical_name}")
    
    # 4. Add optical_name column to email_df
    print("\n=== Adding optical_name column to email_df ===")
    email_df_with_optical = email_df.copy()
    
    print("Mapping Optical names...")
    email_df_with_optical['optical_name'] = email_df_with_optical['business_id'].map(business_id_to_optical)
    
    # Check results
    mapped_count = email_df_with_optical['optical_name'].notna().sum()
    total_count = len(email_df_with_optical)
    nan_business_id_count = email_df_with_optical['business_id'].isna().sum()
    
    print(f"\n=== Mapping Results ===")
    print(f"Total rows: {total_count:,}")
    print(f"Optical name mapping success: {mapped_count:,}")
    print(f"Mapping failure (NaN business_id): {nan_business_id_count:,}")
    print(f"Mapping failure (Other): {total_count - mapped_count - nan_business_id_count:,}")
    
    # Mapping success rate
    success_rate = (mapped_count / total_count) * 100
    print(f"Mapping success rate: {success_rate:.1f}%")
    
    # 5. Verify consistency
    verify_consistency(email_df_with_optical)
    
    # 6. Final result verification and saving
    print("\n=== Final Result Verification ===")
    print(f"Original DataFrame column count: {len(email_df.columns)}")
    print(f"New DataFrame column count: {len(email_df_with_optical.columns)}")
    print(f"Added column: optical_name")
    
    # optical_name column statistics
    print(f"\n=== optical_name column statistics ===")
    print(f"Number of unique optical_name: {email_df_with_optical['optical_name'].nunique()}")
    print(f"Most frequently used optical_name:")
    top_optical_names = email_df_with_optical['optical_name'].value_counts().head()
    print(top_optical_names)
    
    # Output sample data
    print("\n=== Sample Data ===")
    sample_columns = ['business_id', 'optical_name']
    if 'customer_name' in email_df_with_optical.columns:
        sample_columns.append('customer_name')
    if 'subject' in email_df_with_optical.columns:
        sample_columns.append('subject')
    
    print(email_df_with_optical[sample_columns].head(10))
    
    # 7. Save file
    output_filename = 'email_df_with_optical_names.csv'
    print(f"\n=== Saving File ===")
    print(f"Saving: {output_filename}")
    
    email_df_with_optical.to_csv(output_filename, index=False)
    print(f"✅ Save completed: {output_filename}")
    
    # Save mapping table separately
    mapping_df = pd.DataFrame(list(business_id_to_optical.items()), 
                             columns=['business_id', 'optical_name'])
    mapping_filename = 'business_id_to_optical_name_mapping.csv'
    mapping_df.to_csv(mapping_filename, index=False)
    print(f"✅ Mapping table saved: {mapping_filename}")
    
    print(f"\n�� Task completed!")
    print(f"📊 Total {len(email_df_with_optical):,} rows with optical_name column added")
    print(f"🏢 {len(unique_business_ids):,} unique business_ids with consistent name assignment")
    print(f"📁 Saved files: {output_filename}, {mapping_filename}")
    
    return email_df_with_optical, business_id_to_optical

if __name__ == "__main__":
    email_df_with_optical, mapping = main() 