import pandas as pd

# Load each CSV file as an individual DataFrame
cc_path = 'data/cc (1).csv'
fi_path = 'data/fi.CSV'
s_by_c_path = 'data/s_by_c.CSV'

df = pd.read_csv(cc_path)
df2 = pd.read_csv(fi_path)
df3 = pd.read_csv(s_by_c_path)

print("=== DataFrame Information ===")
print(f"df (cc (1).csv): {df.shape} - columns: {list(df.columns)}")
print(f"df2 (fi.CSV): {df2.shape} - columns: {list(df2.columns)}")
print(f"df3 (s_by_c.CSV): {df3.shape} - columns: {list(df3.columns)}")

print("\n=== First 5 rows of df (cc (1).csv) ===")
print(df.head())

print("\n=== First 5 rows of df2 (fi.CSV) ===")
print(df2.head())

print("\n=== First 5 rows of df3 (s_by_c.CSV) ===")
print(df3.head())

# Basic info for each DataFrame
print("\n=== Basic DataFrame Info ===")
print(df.info())
print(df2.info())
print(df3.info())

# Check data types
print("\n=== Data Types ===")
print(df.dtypes)
print(df2.dtypes)
print(df3.dtypes)

# Check for missing values
print("\n=== Missing Value Counts ===")
print("df missing values:")
print(df.isnull().sum())
print("\ndf2 missing values:")
print(df2.isnull().sum())
print("\ndf3 missing values:")
print(df3.isnull().sum()) 