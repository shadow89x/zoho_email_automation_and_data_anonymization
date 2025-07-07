import pandas as pd
import numpy as np
import random

# 1. Load data
print("Loading data...")
df = pd.read_csv('email_customer_matched_full.csv')
print(f"Data loaded successfully: {df.shape}")

# 2. Check business_id
print(f"Unique business_id count: {df['business_id'].nunique()}")

# 3. Words for generating Optical names
optical_words = [
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

# 4. Generate consistent optical names by business_id
print("Generating Optical names...")

# Set seed for reproducible results
np.random.seed(42)
random.seed(42)

# List of unique business_ids
unique_business_ids = df['business_id'].dropna().unique()

# Create business_id to optical name mapping
business_id_to_optical = {}
used_names = set()

for i, business_id in enumerate(unique_business_ids):
    # Use unique seed for each business_id for consistent results
    business_seed = hash(str(business_id)) % 2147483647
    np.random.seed(business_seed)
    random.seed(business_seed)
    
    # Generate non-duplicate name
    for attempt in range(100):
        word = np.random.choice(optical_words)
        optical_name = f"{word} Optical"
        
        if optical_name not in used_names:
            used_names.add(optical_name)
            business_id_to_optical[business_id] = optical_name
            break
    else:
        # If still duplicate after 100 attempts, add number
        optical_name = f"{word} Optical {i+1}"
        business_id_to_optical[business_id] = optical_name
        used_names.add(optical_name)

print(f"✅ Generated {len(business_id_to_optical)} unique Optical names")

# 5. Add new column to DataFrame
print("Adding new column...")
df['optical_name'] = df['business_id'].map(business_id_to_optical)

# 6. Check results
mapped_count = df['optical_name'].notna().sum()
total_count = len(df)
print(f"Mapping completed: {mapped_count}/{total_count} ({mapped_count/total_count*100:.1f}%)")

# 7. Check sample
print("\n=== Sample Results ===")
sample_cols = ['business_id', 'optical_name']
if 'customer_name' in df.columns:
    sample_cols.append('customer_name')
print(df[sample_cols].head(10))

# 8. Verify consistency
print("\n=== Consistency Verification ===")
consistency_check = df.groupby('business_id')['optical_name'].nunique()
inconsistent = consistency_check[consistency_check > 1]
if len(inconsistent) == 0:
    print("✅ Consistent optical_name assignment completed for all business_ids")
else:
    print(f"❌ Consistency issues found in {len(inconsistent)} business_ids")

# 9. Save
output_filename = 'email_customer_matched_full_with_optical.csv'
df.to_csv(output_filename, index=False)
print(f"\n✅ Save completed: {output_filename}")

# 10. Save mapping table
mapping_df = pd.DataFrame(list(business_id_to_optical.items()), 
                         columns=['business_id', 'optical_name'])
mapping_df.to_csv('business_id_optical_mapping.csv', index=False)
print(f"✅ Mapping table saved: business_id_optical_mapping.csv")

print(f"\n🎉 Completed! Added optical_name column to {len(df):,} rows") 