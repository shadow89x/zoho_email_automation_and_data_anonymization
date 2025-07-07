"""
Customer Name Matching Analysis Tool

What: Comprehensive tool for analyzing customer name consistency between multiple datasets
Why: Ensures data quality and identifies integration issues before downstream processing
How: Compares cleaned customer names using set operations and provides detailed statistics
Alternative: Could use fuzzy matching for partial matches, but exact matching provides clearer baseline assessment

Package Selection Rationale:
- pandas: Industry standard for data manipulation and analysis
- re: Built-in regex library for pattern matching and text cleaning
- No external dependencies: Keeps tool lightweight and easy to deploy

Analysis Features:
- Exact name matching between datasets
- Privacy-safe reporting (only statistics, no actual names)
- Quality assessment with clear recommendations
- Detailed breakdown of matching patterns
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
        
    Examples:
        extract_business_info("1341A") -> ("1341", "A", "Accessory")
        extract_business_info("1513") -> ("1513", "", "Lens")
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

def clean_customer_name(customer_name):
    """
    Clean customer name by removing account number suffixes
    
    What: Removes trailing account numbers (e.g., '#1341') from customer names
    Why: Enables matching and deduplication by standardizing names
    How: Uses regex to remove trailing patterns
    Alternative: For more complex patterns, Named Entity Recognition could be used, but regex is fast and interpretable
    
    Args:
        customer_name (str): Raw customer name that may include account numbers
        
    Returns:
        str: Cleaned customer name without account number suffixes
        
    Examples:
        clean_customer_name("1001 OPTICAL #1341") -> "1001 OPTICAL"
        clean_customer_name("ABC VISION") -> "ABC VISION"
    """
    if pd.isna(customer_name):
        return None
    
    # Remove "#number" pattern from end of names
    # Pattern: \s* (optional whitespace) + # + \d+ (digits) + [A-Za-z]* (optional letters) + $ (end of string)
    clean_name = re.sub(r'\s*#\d+[A-Za-z]*$', '', str(customer_name))
    return clean_name.strip()

def check_customer_matching():
    """
    Check for matching customer names between two datasets
    
    What: Compares cleaned customer names between two CSVs and reports overlap and differences
    Why: Ensures consistency and completeness of customer records across sources
    How: Loads data, cleans names, computes set intersections/differences, and prints only shape/columns for privacy
    Alternative: Could use fuzzy matching for partial matches, but exact match is transparent and fast for initial checks
    
    Returns:
        dict: Comprehensive matching analysis results including ratios and quality assessment
        
    Analysis Components:
    - Unique customer name counts in each dataset
    - Exact matches between datasets
    - Dataset-specific customer names
    - Matching ratios and quality assessment
    """
    print("=== Loading CSV files for customer matching analysis ===")
    
    try:
        # Load customer and sales data
        customer_df = pd.read_csv('cc (1).csv')
        sales_df = pd.read_csv('s_by_c.CSV')
    except FileNotFoundError as e:
        print(f"Error: Required CSV files not found. Details: {e}")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
    # Privacy-safe data structure display
    print(f"Customer dataset: {customer_df.shape}, columns: {len(customer_df.columns)}")
    print(f"Sales dataset: {sales_df.shape}, columns: {len(sales_df.columns)}")
    
    # Add clean customer names for matching
    customer_df['customer_name_clean'] = customer_df['Customer'].apply(clean_customer_name)
    sales_df['customer_name_clean'] = sales_df['Name'].apply(clean_customer_name)
    
    # Extract unique customer names from both datasets
    customer_names = set(customer_df['customer_name_clean'].dropna().unique())
    sales_names = set(sales_df['customer_name_clean'].dropna().unique())
    
    print(f"\n=== Customer Name Statistics ===")
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
    
    # Privacy-safe sample display (anonymized)
    print(f"\n=== Sample Analysis (Privacy-Safe) ===")
    print(f"Top matching names count: {min(20, len(matching_customers))}")
    print(f"Sample customer-only names count: {min(10, len(customer_only_names))}")
    print(f"Sample sales-only names count: {min(10, len(sales_only_names))}")
    
    # Record-level analysis
    print(f"\n=== Record-Level Analysis ===")
    
    # Count records with matching customer names
    customer_matching_records = customer_df[customer_df['customer_name_clean'].isin(matching_customers)].shape[0]
    sales_matching_records = sales_df[sales_df['customer_name_clean'].isin(matching_customers)].shape[0]
    
    print(f"Customer records with matching names: {customer_matching_records}")
    print(f"Sales records with matching names: {sales_matching_records}")
    
    # Calculate record-level matching ratios
    customer_record_ratio = customer_matching_records / len(customer_df) * 100 if len(customer_df) > 0 else 0
    sales_record_ratio = sales_matching_records / len(sales_df) * 100 if len(sales_df) > 0 else 0
    
    print(f"Customer dataset record matching ratio: {customer_record_ratio:.2f}%")
    print(f"Sales dataset record matching ratio: {sales_record_ratio:.2f}%")
    
    # Quality assessment
    print(f"\n=== Data Quality Assessment ===")
    avg_matching_ratio = (customer_matching_ratio + sales_matching_ratio) / 2
    
    if avg_matching_ratio > 80:
        print("✅ Excellent data consistency - High overlap between datasets")
        quality_level = "Excellent"
    elif avg_matching_ratio > 60:
        print("⚠️ Good data consistency - Moderate overlap with some gaps")
        quality_level = "Good"
    elif avg_matching_ratio > 40:
        print("⚠️ Moderate data consistency - Significant gaps between datasets")
        quality_level = "Moderate"
    else:
        print("❌ Poor data consistency - Major integration issues detected")
        quality_level = "Poor"
    
    return {
        'matching_customers': matching_customers,
        'customer_only_names': customer_only_names,
        'sales_only_names': sales_only_names,
        'customer_matching_ratio': customer_matching_ratio,
        'sales_matching_ratio': sales_matching_ratio,
        'customer_record_ratio': customer_record_ratio,
        'sales_record_ratio': sales_record_ratio,
        'avg_matching_ratio': avg_matching_ratio,
        'quality_level': quality_level
    }

def analyze_matching_details():
    """
    Analyze details of matching customer names between datasets
    
    What: For each matched customer name, counts records in both datasets and ranks by total count
    Why: Identifies most frequent/important customers and potential data quality issues
    How: Loads data, cleans names, aggregates counts, and prints only shape/columns for privacy
    Alternative: Could visualize with plots or use clustering for deeper analysis
    
    Returns:
        list: Detailed analysis of matching customers ranked by record count
        
    Analysis Features:
    - Record counts per customer in both datasets
    - Ranking by total activity (record count)
    - Privacy-safe reporting without exposing actual names
    """
    print("=== Analyzing detailed customer matching patterns ===")
    
    try:
        # Load CSV files
        customer_df = pd.read_csv('cc (1).csv')
        sales_df = pd.read_csv('s_by_c.CSV')
    except FileNotFoundError as e:
        print(f"Error: Required CSV files not found. Details: {e}")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
    # Add clean customer names
    customer_df['customer_name_clean'] = customer_df['Customer'].apply(clean_customer_name)
    sales_df['customer_name_clean'] = sales_df['Name'].apply(clean_customer_name)
    
    # Find matching customer names
    customer_names = set(customer_df['customer_name_clean'].dropna().unique())
    sales_names = set(sales_df['customer_name_clean'].dropna().unique())
    matching_customers = customer_names.intersection(sales_names)
    
    print("=== Detailed Customer Matching Analysis ===")
    
    # Analyze record counts for each matching customer
    matching_analysis = []
    
    for customer in sorted(matching_customers):
        customer_count = len(customer_df[customer_df['customer_name_clean'] == customer])
        sales_count = len(sales_df[sales_df['customer_name_clean'] == customer])
        total_count = customer_count + sales_count
        
        matching_analysis.append({
            'customer_name': customer,
            'customer_records': customer_count,
            'sales_records': sales_count,
            'total_records': total_count
        })
    
    # Sort by total record count (most active customers first)
    matching_analysis.sort(key=lambda x: x['total_records'], reverse=True)
    
    # Privacy-safe reporting
    print("=== Top Customer Activity Analysis (Privacy-Safe) ===")
    print(f"{'Rank':<4} {'Customer Dataset':<15} {'Sales Dataset':<15} {'Total Records':<15}")
    print("-" * 60)
    
    for i, analysis in enumerate(matching_analysis[:20], 1):
        print(f"{i:<4} {analysis['customer_records']:<15} {analysis['sales_records']:<15} {analysis['total_records']:<15}")
    
    # Summary statistics
    if matching_analysis:
        total_customers = len(matching_analysis)
        avg_customer_records = sum(a['customer_records'] for a in matching_analysis) / total_customers
        avg_sales_records = sum(a['sales_records'] for a in matching_analysis) / total_customers
        
        print(f"\n=== Activity Summary ===")
        print(f"Total matching customers: {total_customers}")
        print(f"Average customer dataset records per customer: {avg_customer_records:.2f}")
        print(f"Average sales dataset records per customer: {avg_sales_records:.2f}")
        print(f"Most active customer total records: {matching_analysis[0]['total_records']}")
        print(f"Least active customer total records: {matching_analysis[-1]['total_records']}")
    
    return matching_analysis

if __name__ == "__main__":
    print("=== Customer Matching Analysis Tool ===")
    print("Analyzing customer name consistency between datasets...")
    
    # Run basic matching analysis
    print("\n" + "="*60)
    print("PHASE 1: Basic Matching Analysis")
    print("="*60)
    
    results = check_customer_matching()
    
    if results is None:
        print("Error: Basic analysis failed. Please check data files and try again.")
        exit(1)
    
    # Run detailed analysis
    print("\n" + "="*60)
    print("PHASE 2: Detailed Activity Analysis")
    print("="*60)
    
    detailed_analysis = analyze_matching_details()
    
    if detailed_analysis is None:
        print("Error: Detailed analysis failed.")
        exit(1)
    
    # Final summary report
    print("\n" + "="*60)
    print("FINAL SUMMARY REPORT")
    print("="*60)
    
    print(f"Matching customer names: {len(results['matching_customers'])}")
    print(f"Customer dataset matching ratio: {results['customer_matching_ratio']:.2f}%")
    print(f"Sales dataset matching ratio: {results['sales_matching_ratio']:.2f}%")
    print(f"Customer dataset record matching ratio: {results['customer_record_ratio']:.2f}%")
    print(f"Sales dataset record matching ratio: {results['sales_record_ratio']:.2f}%")
    print(f"Overall data quality: {results['quality_level']}")
    
    # Recommendations
    print(f"\n=== Recommendations ===")
    if results['quality_level'] == "Excellent":
        print("✅ Data quality is excellent. Proceed with confidence.")
    elif results['quality_level'] == "Good":
        print("⚠️ Data quality is good. Consider investigating gaps for completeness.")
    elif results['quality_level'] == "Moderate":
        print("⚠️ Data quality needs attention. Review data integration processes.")
    else:
        print("❌ Data quality is poor. Significant data cleaning and integration work needed.")
    
    print("\nAnalysis complete. Results are privacy-safe and ready for sharing.") 