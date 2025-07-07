# Code to run in Jupyter notebook
# Copy this code to a new cell and run it

import pandas as pd
import re

def clean_customer_name(name):
    """
    Clean customer name by removing patterns
    """
    if pd.isna(name):
        return name
    
    name = str(name).strip()
    
    # Remove "#number" pattern (e.g., "1001 OPTICAL #1341" -> "1001 OPTICAL")
    name = re.sub(r'#\d+', '', name).strip()
    
    # Remove extra spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

# Clean customer names (skip if already processed)
if 'clean_customer_name' not in df.columns:
    df['clean_customer_name'] = df['Customer'].apply(clean_customer_name)

if 'clean_customer_name' not in df3.columns:
    df3['clean_customer_name'] = df3['Name'].apply(clean_customer_name)

# Extract unique customer names
df_customers = set(df['clean_customer_name'].dropna().unique())
df3_customers = set(df3['clean_customer_name'].dropna().unique())

print("=== Customer Name Statistics ===")
print(f"df unique customer names: {len(df_customers)}")
print(f"df3 unique customer names: {len(df3_customers)}")

# Find matching customer names
matching_customers = df_customers & df3_customers
df_only_customers = df_customers - df3_customers
df3_only_customers = df3_customers - df_customers

print(f"\n=== Matching Results ===")
print(f"Matching customer names: {len(matching_customers)}")
print(f"df only customer names: {len(df_only_customers)}")
print(f"df3 only customer names: {len(df3_only_customers)}")

# Show sample matching customers
print(f"\n=== Sample Matching Customers (Top 10) ===")
for customer in sorted(list(matching_customers))[:10]:
    print(f"  - {customer}")

# Show sample df-only customers
print(f"\n=== Sample df-Only Customers (Top 10) ===")
for customer in sorted(list(df_only_customers))[:10]:
    print(f"  - {customer}")

# Show sample df3-only customers
print(f"\n=== Sample df3-Only Customers (Top 10) ===")
for customer in sorted(list(df3_only_customers))[:10]:
    print(f"  - {customer}")

# Calculate matching statistics
total_customers = len(df_customers | df3_customers)
matching_rate = len(matching_customers) / total_customers * 100

print(f"\n=== Matching Statistics ===")
print(f"Total unique customers: {total_customers}")
print(f"Matching rate: {matching_rate:.2f}%")

if matching_rate > 50:
    print("✅ Good matching rate!")
elif matching_rate > 20:
    print("⚠️ Moderate matching rate")
else:
    print("❌ Low matching rate - need to investigate")

# Detailed analysis
print(f"\n=== Detailed Analysis ===")

# Check for similar names (fuzzy matching)
print("Checking for similar names...")
similar_pairs = []

for df_customer in list(df_only_customers)[:50]:  # Check first 50 for performance
    for df3_customer in list(df3_only_customers)[:50]:
        # Simple similarity check (can be improved with fuzzy matching)
        if (df_customer.lower() in df3_customer.lower() or 
            df3_customer.lower() in df_customer.lower()):
            similar_pairs.append((df_customer, df3_customer))

if similar_pairs:
    print(f"Found {len(similar_pairs)} potentially similar name pairs:")
    for pair in similar_pairs[:10]:
        print(f"  {pair[0]} <-> {pair[1]}")
else:
    print("No obvious similar names found")

# Data quality check
print(f"\n=== Data Quality Check ===")

# Check for null values
df_null_count = df['clean_customer_name'].isna().sum()
df3_null_count = df3['clean_customer_name'].isna().sum()

print(f"df null customer names: {df_null_count}")
print(f"df3 null customer names: {df3_null_count}")

# Check for empty strings
df_empty_count = (df['clean_customer_name'] == '').sum()
df3_empty_count = (df3['clean_customer_name'] == '').sum()

print(f"df empty customer names: {df_empty_count}")
print(f"df3 empty customer names: {df3_empty_count}")

# Summary
print(f"\n=== Summary ===")
print(f"df total records: {len(df)}")
print(f"df3 total records: {len(df3)}")
print(f"df valid customer names: {len(df) - df_null_count - df_empty_count}")
print(f"df3 valid customer names: {len(df3) - df3_null_count - df3_empty_count}")
print(f"Overall matching rate: {matching_rate:.2f}%") 