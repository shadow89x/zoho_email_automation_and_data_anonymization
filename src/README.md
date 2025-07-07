# src/ - Core Processing Scripts

This folder contains all core Python scripts for data preprocessing, customer-email matching, business ID mapping, and utility functions. Each script is modular and can be used independently or as part of the full pipeline.

## Purpose
- Clean and preprocess raw customer, email, and inventory data
- Map and match customer information across datasets
- Assign unique business IDs and handle account types
- Provide utility functions for date, name, and data cleaning

## Main Scripts
- `data_utils.py`: Central utility functions for data loading, cleaning, and processing
- `account_processing.py`: Extracts and processes business/account information from customer data
- `business_id_mapping.py`: Assigns unique business IDs and links related accounts
- `check_customer_matching.py`: Analyzes and validates customer matching across datasets
- `clean_customer_names.py`: Cleans and standardizes customer names for matching
- `convert_dates.py`: Converts and standardizes date/time formats
- `split_date_time.py`: Splits date and time columns for easier analysis
- `email_customer_matching.py`: Matches emails to customer records using business logic
- `remove_unnamed_columns.py`: Removes unnecessary columns from dataframes
- `split_inventory_categories.py`: Splits inventory data into categories
- `load_all_data.py`, `load_email_data.py`, `load_individual_dataframes.py`: Data loading utilities

## Jupyter Scripts
Scripts prefixed with `jupyter_` are notebook-friendly versions for interactive analysis and step-by-step debugging.

## How to Use
- Import specific functions in your scripts or notebooks:
  ```python
  from src.data_utils import load_data_files, process_account_data
  df = load_data_files()
  df_processed = process_account_data(df)
  ```
- Run as standalone scripts for batch processing.

## Extensibility
- Add new scripts for additional data sources or processing steps
- Extend utility functions for new data types or business rules
- All scripts are designed for easy modification and integration 