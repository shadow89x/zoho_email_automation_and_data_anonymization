import pandas as pd
import os
import glob

# Find all CSV files in current directory
csv_files = glob.glob('*.csv')
print(f"Found CSV files: {csv_files}")

# Find all XLSX files in current directory
xlsx_files = glob.glob('*.xlsx')
print(f"Found XLSX files: {xlsx_files}")

# Store CSV files in dictionary
csv_data = {}
for file in csv_files:
    try:
        df = pd.read_csv(file)
        csv_data[file] = df
        print(f"{file} loaded successfully - shape: {df.shape}")
    except Exception as e:
        print(f"{file} load failed: {e}")

print(f"\nTotal {len(csv_data)} CSV files loaded successfully")

# Store XLSX files in dictionary
xlsx_data = {}
for file in xlsx_files:
    try:
        # Read all sheets in Excel file
        excel_file = pd.ExcelFile(file)
        for sheet in excel_file.sheet_names:
            df = pd.read_excel(file, sheet_name=sheet)
            xlsx_data[f"{file}_{sheet}"] = df
            print(f"{file} - {sheet} loaded successfully - shape: {df.shape}")
    except Exception as e:
        print(f"{file} load failed: {e}")

print(f"\nTotal {len(xlsx_data)} XLSX sheets loaded successfully")

# Display summary
print("\n=== Data Summary ===")
print(f"CSV files: {len(csv_data)}")
print(f"XLSX sheets: {len(xlsx_data)}")
print(f"Total datasets: {len(csv_data) + len(xlsx_data)}")

# Show available datasets
print("\n=== Available Datasets ===")
print("CSV files:")
for name, df in csv_data.items():
    print(f"  {name}: {df.shape}")

print("\nXLSX sheets:")
for name, df in xlsx_data.items():
    print(f"  {name}: {df.shape}")

# Check all data
print("\n=== CSV File Data ===")
for file, df in csv_data.items():
    print(f"\nFile: {file}")
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  First 5 rows:")
    print(df.head())

print("\n=== XLSX File Data ===")
for file, df in xlsx_data.items():
    print(f"\nFile: {file}")
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  First 5 rows:")
    print(df.head())

# Data access examples
print("\n=== Data Access Examples ===")
if csv_data:
    first_csv_file = list(csv_data.keys())[0]
    print(f"First CSV file ({first_csv_file}) first 3 rows:")
    print(csv_data[first_csv_file].head(3))

if xlsx_data:
    first_xlsx_file = list(xlsx_data.keys())[0]
    print(f"\nFirst XLSX file ({first_xlsx_file}) first 3 rows:")
    print(xlsx_data[first_xlsx_file].head(3)) 