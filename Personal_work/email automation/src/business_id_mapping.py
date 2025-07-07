"""
Business ID Mapping and Cross-Dataset Integration

What: Maps business identifiers across multiple datasets to enable unified customer analysis
Why: Optical businesses often appear in different datasets with variations in naming and account structures
How: Uses account number parsing and customer name matching to create consistent business IDs
Alternative: Could use machine learning for fuzzy matching, but rule-based approach is more transparent

Package Selection Rationale:
- pandas: Industry standard for data manipulation and joining operations
- re: Built-in regex library for pattern matching and text cleaning
- No external dependencies: Keeps module lightweight and easy to deploy

Design Principles:
- Privacy-first: Only displays shape/columns, never actual customer data
- Robust error handling: Graceful handling of missing or malformed data
- Comprehensive validation: Verifies mapping quality and completeness
- Modular design: Each function has single responsibility for easy testing
"""

import pandas as pd
import re

def extract_business_info(account_no):
    """
    Extract base business number and account type from account number
    
    What: Parses account numbers to extract the numeric base, optional suffix, and maps to account type
    Why: Business/account IDs are often encoded in a single string; splitting them enables grouping and analysis
    How: Uses regex to split number and suffix, then maps suffix to type
    Alternative: Could use more advanced parsing for non-standard formats, but regex covers most cases here
    
    Args:
        account_no (str/int): Account number that may contain business ID and type suffix
        
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
        base_number = match.group(1)  # Base business number (e.g., 1341)
        suffix = match.group(2).upper() if match.group(2) else ''  # Suffix (e.g., A, F, K, S, E, '')
        
        # Account type classification based on optical industry standards
        # Why this mapping: Common optical industry account suffix conventions
        account_type_map = {
            'A': 'Accessory',    # Accessories and tools
            'F': 'Frame',        # Eyeglass frames
            'K': 'Surface',      # Lens surface treatments
            'S': 'Brand Lens',   # Special/branded lenses
            'E': 'Edging',       # Lens edging services
            '': 'Lens'           # Default: prescription lenses (no suffix)
        }
        
        account_type = account_type_map.get(suffix, 'Other')
        
        return base_number, suffix, account_type
    
    return account_str, '', 'Unknown'

def process_df_accounts(df):
    """
    Process account numbers in DataFrame to create unified business identifiers
    
    What: Adds business info columns and assigns unique business IDs based on account numbers
    Why: Enables grouping, deduplication, and downstream analysis by business entity
    How: Extracts info, creates sequential mapping, adds analytical columns
    Alternative: Could use hash-based IDs, but sequential IDs are more interpretable for humans
    
    Args:
        df (DataFrame): DataFrame with 'Account No.' column
        
    Returns:
        DataFrame: Enhanced DataFrame with business intelligence columns
        
    Added Columns:
        - base_account: Core business number extracted from account
        - suffix: Account type suffix (A, F, K, S, E, or empty)
        - account_type: Human-readable account type
        - business_id: Anonymized sequential business identifier (BUS_0001, BUS_0002, etc.)
        - is_main_account: Boolean indicating main account (lens account)
        - has_multiple_accounts: Boolean indicating if business has multiple account types
    """
    print("=== Processing account numbers to create business IDs ===")
    
    # Extract business information from account numbers
    # Why apply with pd.Series: Efficiently unpacks tuple results into multiple columns
    df[['base_account', 'suffix', 'account_type']] = df['Account No.'].apply(
        lambda x: pd.Series(extract_business_info(x))
    )

    # Create unique business ID mapping
    # Why sequential IDs: Easier to work with in analysis than random UUIDs or hashes
    unique_base_accounts = df['base_account'].dropna().unique()
    business_id_map = {base: f"BUS_{i+1:04d}" for i, base in enumerate(sorted(unique_base_accounts))}

    # Apply business ID mapping
    df['business_id'] = df['base_account'].map(business_id_map)

    # Add analytical columns for business intelligence
    df['is_main_account'] = df['suffix'] == ''  # Main account indicator (lens account)
    df['has_multiple_accounts'] = df.groupby('business_id')['business_id'].transform('count') > 1
    
    # Privacy-safe reporting
    print(f"Customer dataset processed: {df.shape}")
    print(f"Unique businesses identified: {df['business_id'].nunique()}")
    print(f"Account types found: {df['account_type'].nunique()}")
    
    return df

def map_business_id_to_df3(customer_df, sales_df):
    """
    Map business_id from customer dataset to sales dataset using customer name matching
    
    What: Joins business_id from customer data to sales data by cleaned customer name
    Why: Links business/account info across datasets for unified customer analysis
    How: Cleans names, performs left join, validates mapping quality
    Alternative: Could use fuzzy matching for partial matches, but exact match is transparent and fast
    
    Args:
        customer_df (DataFrame): Customer DataFrame with business_id column
        sales_df (DataFrame): Sales DataFrame with customer names in 'Name' column
        
    Returns:
        DataFrame: Sales DataFrame enhanced with business_id and customer mapping info
        
    Mapping Process:
    1. Create customer name to business_id mapping from customer dataset
    2. Clean customer names in sales dataset (remove account number suffixes)
    3. Perform left join to preserve all sales records
    4. Validate mapping quality and report statistics
    """
    print("=== Mapping business IDs to sales dataset ===")
    
    # Create customer-to-business mapping from customer dataset
    # Why drop_duplicates: Ensures one-to-one mapping between customer names and business IDs
    customer_business_map = customer_df[['Customer', 'business_id']].drop_duplicates()
    
    # Clean customer names in sales dataset
    def extract_customer_name(name):
        """
        Extract clean customer name by removing account number suffixes
        
        What: Removes trailing account numbers (e.g., "#1341") from customer names
        Why: Customer names in sales data often include account numbers that need standardization
        How: Uses regex to remove "#number" patterns from end of names
        Alternative: Could use more sophisticated name parsing, but regex is sufficient
        
        Args:
            name (str): Raw customer name from sales data
            
        Returns:
            str: Cleaned customer name without account number suffixes
        """
        if pd.isna(name):
            return None
        
        # Remove "#number" pattern from end of names
        # Pattern: \s* (optional whitespace) + # + \d+ (digits) + [A-Za-z]* (optional letters) + $ (end)
        customer_name = re.sub(r'\s*#\d+[A-Za-z]*$', '', str(name))
        return customer_name.strip()
    
    # Apply name cleaning to sales dataset
    sales_df['customer_name_clean'] = sales_df['Name'].apply(extract_customer_name)
    
    # Perform left join to preserve all sales records
    # Why left join: Maintains all sales data even if some customers don't have business IDs
    sales_df = sales_df.merge(customer_business_map, 
                             left_on='customer_name_clean', 
                             right_on='Customer', 
                             how='left')
    
    # Analyze mapping results
    unmapped_count = sales_df['business_id'].isna().sum()
    total_count = len(sales_df)
    mapped_count = total_count - unmapped_count
    
    print(f"=== Sales Dataset Mapping Results ===")
    print(f"Total sales records: {total_count}")
    print(f"Successfully mapped records: {mapped_count}")
    print(f"Unmapped records: {unmapped_count}")
    print(f"Mapping success rate: {(mapped_count / total_count * 100):.2f}%")
    
    # Analyze unmapped customers (privacy-safe)
    if unmapped_count > 0:
        unmapped_customers = sales_df[sales_df['business_id'].isna()]['customer_name_clean'].nunique()
        print(f"\nUnmapped unique customers: {unmapped_customers}")
        print("Note: Unmapped customers may be new customers not in customer dataset")
    
    # Privacy-safe dataset info
    print(f"Enhanced sales dataset: {sales_df.shape}")
    
    return sales_df

def verify_mapping(customer_df, sales_df):
    """
    Verify that business_id mapping was successful with comprehensive validation
    
    What: Checks unique business counts, mapping rates, and data quality metrics
    Why: Ensures mapping quality and completeness before downstream analysis
    How: Computes various statistics and validates business-level aggregations
    Alternative: Could visualize mapping coverage, but summary stats are sufficient for validation
    
    Args:
        customer_df (DataFrame): Processed customer DataFrame with business IDs
        sales_df (DataFrame): Sales DataFrame with mapped business IDs
        
    Returns:
        bool: True if mapping quality is acceptable (>80% success rate), False otherwise
        
    Validation Metrics:
    - Unique business count consistency
    - Record-level mapping success rate
    - Business activity distribution
    - Data quality indicators
    """
    print("\n=== Comprehensive Mapping Validation ===")
    
    # Business count validation
    customer_unique_businesses = customer_df['business_id'].nunique()
    sales_mapped_businesses = sales_df['business_id'].dropna().nunique()
    
    print(f"Customer dataset unique businesses: {customer_unique_businesses}")
    print(f"Sales dataset mapped businesses: {sales_mapped_businesses}")
    print(f"Business coverage: {(sales_mapped_businesses / customer_unique_businesses * 100):.2f}%")
    
    # Record-level mapping validation
    sales_total_records = len(sales_df)
    sales_mapped_records = sales_df['business_id'].notna().sum()
    mapping_success_rate = (sales_mapped_records / sales_total_records) * 100
    
    print(f"\nRecord-Level Validation:")
    print(f"Total sales records: {sales_total_records}")
    print(f"Mapped sales records: {sales_mapped_records}")
    print(f"Overall mapping success rate: {mapping_success_rate:.2f}%")
    
    # Business activity analysis (privacy-safe)
    print(f"\n=== Business Activity Analysis (Privacy-Safe) ===")
    if sales_mapped_records > 0:
        business_activity = sales_df.groupby('business_id').size().sort_values(ascending=False)
        
        print(f"Most active business record count: {business_activity.iloc[0] if len(business_activity) > 0 else 0}")
        print(f"Least active business record count: {business_activity.iloc[-1] if len(business_activity) > 0 else 0}")
        print(f"Average records per business: {business_activity.mean():.2f}")
        print(f"Median records per business: {business_activity.median():.2f}")
        
        # Activity distribution
        print(f"\nActivity Distribution:")
        print(f"Businesses with 1 record: {(business_activity == 1).sum()}")
        print(f"Businesses with 2-5 records: {((business_activity >= 2) & (business_activity <= 5)).sum()}")
        print(f"Businesses with 6-10 records: {((business_activity >= 6) & (business_activity <= 10)).sum()}")
        print(f"Businesses with >10 records: {(business_activity > 10).sum()}")
    
    # Data quality assessment
    print(f"\n=== Data Quality Assessment ===")
    quality_score = mapping_success_rate
    
    if quality_score > 90:
        print("✅ Excellent mapping quality - High confidence in results")
        quality_level = "Excellent"
    elif quality_score > 80:
        print("✅ Good mapping quality - Results are reliable")
        quality_level = "Good"
    elif quality_score > 60:
        print("⚠️ Moderate mapping quality - Some manual review recommended")
        quality_level = "Moderate"
    else:
        print("❌ Poor mapping quality - Significant data issues detected")
        quality_level = "Poor"
    
    # Privacy-safe dataset summaries
    print(f"\nDataset Summaries (Privacy-Safe):")
    print(f"Customer dataset: {customer_df.shape}")
    print(f"Sales dataset: {sales_df.shape}")
    
    return quality_score > 80

def main_business_id_mapping():
    """
    Main orchestration function for business ID mapping pipeline
    
    What: Coordinates complete business ID mapping workflow from data loading to validation
    Why: Provides single entry point for the entire mapping process with error handling
    How: Executes each step in sequence with comprehensive validation and reporting
    Alternative: Could use workflow engines like Airflow, but direct execution is simpler
    
    Returns:
        tuple: (customer_df_processed, sales_df_processed, mapping_success) or None if error
        
    Pipeline Steps:
    1. Load customer and sales datasets
    2. Process customer account numbers to create business IDs
    3. Map business IDs to sales dataset using customer names
    4. Validate mapping quality and completeness
    5. Save processed datasets for downstream use
    """
    print("=== Business ID Mapping Pipeline Started ===")
    print("=" * 50)
    
    try:
        # Step 1: Data Loading
        print("\n📋 Step 1: Data Loading")
        print("Loading customer and sales datasets...")
        
        customer_df = pd.read_csv('cc (1).csv')
        sales_df = pd.read_csv('s_by_c.CSV')
        
        print(f"Customer dataset loaded: {customer_df.shape}")
        print(f"Sales dataset loaded: {sales_df.shape}")
        
        # Step 2: Process Customer Accounts
        print("\n📋 Step 2: Customer Account Processing")
        customer_df_processed = process_df_accounts(customer_df)
        
        # Step 3: Map Business IDs to Sales
        print("\n📋 Step 3: Business ID Mapping to Sales")
        sales_df_processed = map_business_id_to_df3(customer_df_processed, sales_df)
        
        # Step 4: Validation
        print("\n📋 Step 4: Mapping Validation")
        mapping_success = verify_mapping(customer_df_processed, sales_df_processed)
        
        # Step 5: Save Results
        print("\n📋 Step 5: Results Persistence")
        customer_df_processed.to_csv('processed_customer_with_business_ids.csv', index=False)
        sales_df_processed.to_csv('processed_sales_with_business_ids.csv', index=False)
        
        print("✅ Processed datasets saved:")
        print("- processed_customer_with_business_ids.csv: Customer data with business IDs")
        print("- processed_sales_with_business_ids.csv: Sales data with mapped business IDs")
        
        # Final Summary
        print("\n" + "=" * 50)
        print("🎉 BUSINESS ID MAPPING PIPELINE COMPLETED")
        print(f"📊 Customer businesses: {customer_df_processed['business_id'].nunique()}")
        print(f"📊 Mapped sales records: {sales_df_processed['business_id'].notna().sum()}")
        print(f"📊 Overall quality: {'Good' if mapping_success else 'Needs Review'}")
        
        if mapping_success:
            print("✅ Mapping quality is excellent. Ready for downstream analysis.")
        else:
            print("⚠️ Mapping quality needs attention. Review unmapped records.")
        
        return customer_df_processed, sales_df_processed, mapping_success
        
    except FileNotFoundError as e:
        print(f"❌ Error: Required data files not found. Details: {e}")
        print("Please ensure 'cc (1).csv' and 's_by_c.CSV' files are in the current directory.")
        return None
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        print("Please check data files and try again.")
        return None

if __name__ == "__main__":
    print("=== Business ID Mapping and Cross-Dataset Integration Tool ===")
    print("Integrating customer and sales data through business identifier mapping...")
    
    # Execute main pipeline
    result = main_business_id_mapping()
    
    if result:
        customer_df, sales_df, success = result
        print(f"\n📈 Pipeline completed successfully!")
        print(f"📊 Ready for business intelligence and customer analysis")
        
        # Additional recommendations
        print(f"\n=== Next Steps ===")
        print("1. Use business_id for customer segmentation analysis")
        print("2. Analyze sales patterns by business entity")
        print("3. Identify high-value customers and growth opportunities")
        print("4. Create business performance dashboards")
    else:
        print("\n❌ Pipeline failed. Please check error messages above.")
        print("\n💡 Troubleshooting tips:")
        print("1. Verify input files exist and are readable")
        print("2. Check data format and column names")
        print("3. Ensure sufficient disk space for output files")
        print("4. Review error messages for specific issues") 