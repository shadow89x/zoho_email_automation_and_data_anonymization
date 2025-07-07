"""
Comprehensive Data Processing Utilities

What: Common functions for data cleaning, processing, and analysis across the optical retail analytics pipeline
Why: Centralizes reusable data processing logic to ensure consistency and reduce code duplication
How: Provides modular functions for account processing, customer name cleaning, and data analysis
Alternative: Could use object-oriented design with classes, but functional approach is simpler for utility functions

Package Selection Rationale:
- pandas: Industry standard for data manipulation, excellent performance for medium datasets
- re: Built-in regex library, sufficient for pattern matching tasks
- json: Built-in JSON processing, no external dependencies needed
- datetime: Built-in date/time handling, better than third-party alternatives

Design Principles:
- Single responsibility: Each function has one clear purpose
- Privacy-first: Functions only display shape/columns, never raw data
- Error handling: Robust handling of missing/malformed data
- Documentation: Comprehensive docstrings with examples
"""

import pandas as pd
import re
import json
from datetime import datetime


def extract_business_info(account_no):
    """
    Extract base business number and account type from account number
    
    What: Parses optical industry account numbers to separate business ID from account type suffix
    Why: Optical businesses often have multiple account types (lens, frame, accessories) with same base ID
    How: Uses regex to match digits followed by optional letters, then maps letters to account types
    Alternative: Could use string parsing, but regex is more robust for edge cases
    
    Args:
        account_no (str/int): Account number string or number (e.g., "1341A", "1513F", "1659")
        
    Returns:
        tuple: (base_number, suffix, account_type)
        - base_number: Core business identifier (e.g., "1341")
        - suffix: Account type code (e.g., "A", "F", "")
        - account_type: Human-readable account type (e.g., "Accessory", "Frame", "Lens")
    
    Examples:
        extract_business_info("1341A") -> ("1341", "A", "Accessory")
        extract_business_info("1513") -> ("1513", "", "Lens")
        extract_business_info(None) -> (None, None, None)
    """
    if pd.isna(account_no):
        return None, None, None
    
    account_str = str(account_no)
    
    # Match pattern: digits followed by optional letters
    # Why regex: Handles various account number formats consistently
    match = re.match(r'(\d+)([A-Za-z]*)$', account_str)
    
    if match:
        base_number = match.group(1)  # Base number (e.g., 1341)
        suffix = match.group(2).upper() if match.group(2) else ''  # Suffix (e.g., A, F, K, S, E, '')
        
        # Account type classification based on optical industry standards
        # Why this mapping: Common optical industry account type conventions
        account_type_map = {
            'A': 'Accessory',    # Accessories (cases, cleaners, tools)
            'F': 'Frame',        # Eyeglass frames
            'K': 'Surface',      # Lens surface treatments
            'S': 'Brand Lens',   # Special/branded lenses
            'E': 'Edging',       # Lens edging services
            '': 'Lens'           # Default: prescription lenses (no suffix)
        }
        
        account_type = account_type_map.get(suffix, 'Other')
        
        return base_number, suffix, account_type
    
    return account_str, '', 'Unknown'


def clean_customer_name(customer_name):
    """
    Remove account number suffixes from customer names for standardization
    
    What: Removes trailing account numbers (e.g., "#1341") from customer names
    Why: Customer names often include account numbers that should be separated for analysis
    How: Uses regex to remove "#number" patterns from end of names
    Alternative: Could use string splitting, but regex handles edge cases better
    
    Args:
        customer_name (str): Raw customer name string
        
    Returns:
        str: Cleaned customer name without account numbers
        
    Examples:
        clean_customer_name("1001 OPTICAL #1341") -> "1001 OPTICAL"
        clean_customer_name("ABC VISION CENTER") -> "ABC VISION CENTER"
        clean_customer_name(None) -> None
    """
    if pd.isna(customer_name):
        return None
    
    # Remove "#number" pattern from end of customer names
    # Pattern explanation: \s* (optional whitespace) + # + \d+ (digits) + [A-Za-z]* (optional letters) + $ (end of string)
    clean_name = re.sub(r'\s*#\d+[A-Za-z]*$', '', str(customer_name))
    return clean_name.strip()


def process_account_data(df):
    """
    Process DataFrame account numbers to create unified business identifiers
    
    What: Transforms account numbers into anonymized business IDs while preserving relationships
    Why: Enables business-level analysis while protecting actual account numbers
    How: Extracts business info, creates sequential mapping, adds analytical columns
    Alternative: Could use UUID for business IDs, but sequential IDs are more readable
    
    Args:
        df (DataFrame): DataFrame with 'Account No.' column
        
    Returns:
        DataFrame: Enhanced DataFrame with business intelligence columns
        
    Added Columns:
        - base_account: Core business number
        - suffix: Account type suffix
        - account_type: Human-readable account type
        - business_id: Anonymized sequential business identifier
        - is_main_account: Boolean indicating main account (lens)
        - has_multiple_accounts: Boolean indicating if business has multiple account types
    """
    # Extract business information from account numbers
    # Why apply with pd.Series: Efficiently unpacks tuple results into multiple columns
    df[['base_account', 'suffix', 'account_type']] = df['Account No.'].apply(
        lambda x: pd.Series(extract_business_info(x))
    )

    # Create unique business ID mapping
    # Why sequential IDs: Easier to work with in analysis than random UUIDs
    unique_base_accounts = df['base_account'].dropna().unique()
    business_id_map = {base: f"{i+1:04d}" for i, base in enumerate(sorted(unique_base_accounts))}

    # Apply business ID mapping
    df['business_id'] = df['base_account'].map(business_id_map)

    # Add analytical columns for business intelligence
    df['is_main_account'] = df['suffix'] == ''  # Main account flag (lens account)
    df['has_multiple_accounts'] = df.groupby('business_id')['business_id'].transform('count') > 1
    
    return df


def analyze_business_data(df):
    """
    Analyze processed business data and generate privacy-safe statistics
    
    What: Provides comprehensive business intelligence insights from processed account data
    Why: Understand business patterns and data quality without exposing sensitive information
    How: Computes aggregated statistics and displays only anonymized sample data
    Alternative: Could use advanced analytics or visualization, but basic stats are sufficient
    
    Args:
        df (DataFrame): Processed DataFrame with business information
        
    Returns:
        DataFrame: Business groups summary for further analysis
    """
    print("=== Account Number Processing Results ===")
    print(f"Total unique businesses: {df['business_id'].nunique()}")
    print(f"Total accounts: {len(df)}")

    print("\n=== Account Type Distribution ===")
    account_type_counts = df['account_type'].value_counts()
    print(account_type_counts)

    print("\n=== Sample Data Structure (Privacy-Safe) ===")
    # Show only anonymized/processed columns for privacy
    sample_columns = ['business_id', 'base_account', 'suffix', 'account_type', 'is_main_account']
    print(f"Sample shape: {df[sample_columns].head(10).shape}")
    print(f"Sample columns: {sample_columns}")

    print("\n=== Business Account Analysis ===")
    # Group accounts with same business ID for relationship analysis
    business_groups = df.groupby('business_id').agg({
        'Customer': 'first',
        'Account No.': list,
        'account_type': list
    }).head(5)
    
    # Multi-account business analysis
    multi_account_businesses = df[df['has_multiple_accounts']]['business_id'].nunique()
    print(f"Businesses with multiple account types: {multi_account_businesses}")
    
    return business_groups


def load_data_files():
    """
    Load all CSV data files with comprehensive error handling and privacy protection
    
    What: Loads customer, inventory, and sales data from CSV files into pandas DataFrames
    Why: Centralizes data loading with consistent error handling and privacy protection
    How: Reads each file, removes unnecessary columns, displays only shape/columns for privacy
    Alternative: Could use glob or pathlib for dynamic file discovery, but explicit paths are clearer
    
    Returns:
        tuple: (customer_df, inventory_df, sales_df) - Three DataFrames with loaded data
        
    Privacy Note: Only displays shape and column information, never actual data content
    """
    print("=== Loading data files with privacy protection ===")
    
    try:
        # Load each CSV file as individual DataFrame
        # Why absolute paths: Ensures consistent file access regardless of working directory
        customer_df = pd.read_csv('G:/VSCode/Personal_work/email automation/data/customer.csv')
        inventory_df = pd.read_csv('G:/VSCode/Personal_work/email automation/data/inventory.CSV')
        sales_df = pd.read_csv('G:/VSCode/Personal_work/email automation/data/sales_by_customer.CSV')
    except FileNotFoundError as e:
        print(f"Error: Data files not found. Please check file paths. Details: {e}")
        return None, None, None
    except Exception as e:
        print(f"Error loading data files: {e}")
        return None, None, None

    # Remove 'Unnamed: 0' columns if they exist (common pandas artifact)
    # Why: These columns are usually index columns that got saved accidentally
    for df_name, df in [('customer', customer_df), ('inventory', inventory_df), ('sales', sales_df)]:
        if 'Unnamed: 0' in df.columns:
            df.drop(columns=['Unnamed: 0'], inplace=True)
            print(f"Removed 'Unnamed: 0' column from {df_name} data")

    # Privacy-safe information display
    print("=== Dataset Information (Privacy-Safe) ===")
    print(f"Customer data: {customer_df.shape}, columns: {len(customer_df.columns)}")
    print(f"Inventory data: {inventory_df.shape}, columns: {len(inventory_df.columns)}")
    print(f"Sales data: {sales_df.shape}, columns: {len(sales_df.columns)}")
    
    return customer_df, inventory_df, sales_df


def load_email_data():
    """
    Load and normalize email data from JSON file with comprehensive processing
    
    What: Loads email data from JSON file and normalizes nested structures into flat DataFrame
    Why: Email data is often nested JSON; normalization flattens it for easier analysis
    How: Uses pandas.json_normalize for efficient flattening with error handling
    Alternative: Could use manual JSON parsing, but pandas normalization is more robust
    
    Returns:
        DataFrame: Normalized email data with flattened structure
        
    Processing Steps:
    1. Load JSON file with proper encoding
    2. Validate data structure
    3. Normalize nested JSON to flat DataFrame
    4. Display privacy-safe summary statistics
    """
    print("=== Loading email data from JSON with normalization ===")
    
    try:
        # Read JSON file with UTF-8 encoding for international characters
        with open('G:/VSCode/Personal_work/email automation/data/zoho_emails.json', 'r', encoding='utf-8') as file:
            email_data = json.load(file)
    except FileNotFoundError:
        print("Error: zoho_emails.json not found. Please check file path.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format. Details: {e}")
        return None
    except Exception as e:
        print(f"Error loading email data: {e}")
        return None

    # Validate data structure
    print(f"JSON data type: {type(email_data)}")
    print(f"Total email records: {len(email_data)}")

    # Convert nested JSON data to flat DataFrame
    # Why json_normalize: Handles nested dictionaries and lists automatically
    try:
        email_df = pd.json_normalize(email_data)
    except Exception as e:
        print(f"Error normalizing JSON data: {e}")
        return None

    # Privacy-safe information display
    print(f"Normalized email DataFrame: {email_df.shape}, columns: {len(email_df.columns)}")
    
    return email_df


def process_email_dates(email_df):
    """
    Process email timestamps into multiple date/time components for analysis
    
    What: Converts millisecond timestamps to datetime objects and extracts date/time components
    Why: Email timestamps are often in milliseconds; separate components enable time-based analysis
    How: Uses pandas datetime conversion with component extraction
    Alternative: Could use datetime library directly, but pandas is more efficient for DataFrames
    
    Args:
        email_df (DataFrame): Email DataFrame with timestamp columns
        
    Returns:
        DataFrame: Enhanced DataFrame with processed date/time columns
        
    Added Columns:
        - sentDate/receivedDate: Full datetime objects
        - sent_date_only/received_date_only: Date components only
        - sent_time_only/received_time_only: Time components only
        - sent_hour/received_hour: Hour components for time analysis
        - sent_minute/received_minute: Minute components for detailed timing
    """
    print("=== Processing email timestamps into date/time components ===")
    
    try:
        # Convert millisecond timestamps to datetime objects
        # Why unit='ms': Email timestamps are typically in milliseconds since epoch
        email_df['sentDate'] = pd.to_datetime(email_df['sentDateInGMT'].astype(float), unit='ms')
        email_df['receivedDate'] = pd.to_datetime(email_df['receivedTime'].astype(float), unit='ms')

        # Extract date components (separate date and time for analysis)
        # Why separate components: Enables time-based analysis and filtering
        email_df['sent_date_only'] = email_df['sentDate'].dt.date  # Date only (e.g., 2025-06-27)
        email_df['sent_time_only'] = email_df['sentDate'].dt.time  # Time only (e.g., 08:33:48)
        email_df['sent_hour'] = email_df['sentDate'].dt.hour       # Hour only (e.g., 8)
        email_df['sent_minute'] = email_df['sentDate'].dt.minute   # Minute only (e.g., 33)

        # Process received time with same components
        email_df['received_date_only'] = email_df['receivedDate'].dt.date  # Date only
        email_df['received_time_only'] = email_df['receivedDate'].dt.time  # Time only
        email_df['received_hour'] = email_df['receivedDate'].dt.hour       # Hour only
        email_df['received_minute'] = email_df['receivedDate'].dt.minute   # Minute only
    except Exception as e:
        print(f"Error processing email dates: {e}")
        return email_df

    # Validation and summary
    print("=== Date/Time Processing Results ===")
    print("Successfully created date/time components:")
    print(f"- sent_date_only: {email_df['sent_date_only'].dtype}")
    print(f"- sent_time_only: {email_df['sent_time_only'].dtype}")
    print(f"- sent_hour: {email_df['sent_hour'].dtype}")
    print(f"- sent_minute: {email_df['sent_minute'].dtype}")

    # Privacy-safe sample display (only showing structure, not actual dates)
    print("\n=== Sample Date/Time Structure (Privacy-Safe) ===")
    print(f"Sample shape: {email_df[['sent_date_only', 'sent_time_only', 'sent_hour', 'sent_minute']].head().shape}")
    print("Date/time components successfully extracted")
    
    return email_df


def analyze_customer_matching(customer_df, sales_df):
    """
    Analyze customer name matching between datasets with comprehensive reporting
    
    What: Compares cleaned customer names between customer and sales datasets
    Why: Ensures data consistency and identifies potential integration issues
    How: Computes set intersections/differences with detailed statistics
    Alternative: Could use fuzzy matching for partial matches, but exact matching is clearer for initial assessment
    
    Args:
        customer_df (DataFrame): Customer DataFrame with customer names
        sales_df (DataFrame): Sales DataFrame with customer names
        
    Returns:
        dict: Comprehensive matching analysis results
        
    Analysis Components:
    - Exact name matches between datasets
    - Dataset-specific customer names
    - Matching ratios and quality assessment
    - Record-level matching statistics
    """
    print("=== Analyzing customer name matching between datasets ===")
    
    # Add clean customer names if not present
    if 'customer_name_clean' not in customer_df.columns:
        customer_df['customer_name_clean'] = customer_df['Customer'].apply(clean_customer_name)

    if 'customer_name_clean' not in sales_df.columns:
        sales_df['customer_name_clean'] = sales_df['Name'].apply(clean_customer_name)

    # Extract unique customer names from both datasets
    customer_names = set(customer_df['customer_name_clean'].dropna().unique())
    sales_names = set(sales_df['customer_name_clean'].dropna().unique())

    print("=== Customer Name Statistics ===")
    print(f"Customer dataset unique names: {len(customer_names)}")
    print(f"Sales dataset unique names: {len(sales_names)}")

    # Compute set operations for matching analysis
    matching_customers = customer_names.intersection(sales_names)
    customer_only_names = customer_names - sales_names
    sales_only_names = sales_names - customer_names

    print(f"\n=== Name Matching Results ===")
    print(f"Matching customer names: {len(matching_customers)}")
    print(f"Customer-only names: {len(customer_only_names)}")
    print(f"Sales-only names: {len(sales_only_names)}")

    # Calculate matching ratios
    customer_matching_ratio = len(matching_customers) / len(customer_names) * 100 if customer_names else 0
    sales_matching_ratio = len(matching_customers) / len(sales_names) * 100 if sales_names else 0

    print(f"\n=== Matching Ratios ===")
    print(f"Customer dataset matching ratio: {customer_matching_ratio:.2f}%")
    print(f"Sales dataset matching ratio: {sales_matching_ratio:.2f}%")

    # Record-level analysis (how many records have matching names)
    customer_matching_records = customer_df[customer_df['customer_name_clean'].isin(matching_customers)].shape[0]
    sales_matching_records = sales_df[sales_df['customer_name_clean'].isin(matching_customers)].shape[0]

    customer_record_matching_ratio = customer_matching_records / len(customer_df) * 100 if len(customer_df) > 0 else 0
    sales_record_matching_ratio = sales_matching_records / len(sales_df) * 100 if len(sales_df) > 0 else 0

    print(f"\n=== Record-Level Analysis ===")
    print(f"Customer records with matching names: {customer_matching_records}")
    print(f"Sales records with matching names: {sales_matching_records}")
    print(f"Customer dataset record matching ratio: {customer_record_matching_ratio:.2f}%")
    print(f"Sales dataset record matching ratio: {sales_record_matching_ratio:.2f}%")

    # Quality assessment based on matching ratios
    print(f"\n=== Data Quality Assessment ===")
    avg_matching_ratio = (customer_matching_ratio + sales_matching_ratio) / 2
    if avg_matching_ratio > 80:
        print("✅ Excellent data consistency - High overlap between datasets")
    elif avg_matching_ratio > 60:
        print("⚠️ Good data consistency - Moderate overlap with some gaps")
    elif avg_matching_ratio > 40:
        print("⚠️ Moderate data consistency - Significant gaps between datasets")
    else:
        print("❌ Poor data consistency - Major integration issues detected")

    return {
        'matching_customers': matching_customers,
        'customer_only_names': customer_only_names,
        'sales_only_names': sales_only_names,
        'customer_matching_ratio': customer_matching_ratio,
        'sales_matching_ratio': sales_matching_ratio,
        'customer_record_matching_ratio': customer_record_matching_ratio,
        'sales_record_matching_ratio': sales_record_matching_ratio,
        'avg_matching_ratio': avg_matching_ratio
    } 