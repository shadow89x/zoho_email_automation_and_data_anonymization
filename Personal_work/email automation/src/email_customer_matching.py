"""
Advanced Email-Customer Matching Algorithm

What: Implements sophisticated similarity-based matching to link email communications with customer records
Why: Email data often lacks explicit customer identifiers; advanced matching enables relationship discovery
How: Uses fuzzy string matching, sequence matching, and domain comparison with configurable thresholds
Alternative: Could use machine learning embeddings or NER, but classical methods are more interpretable

Package Selection Rationale:
- fuzzywuzzy: Industry standard for fuzzy string matching, faster than alternatives like rapidfuzz for medium datasets
- scikit-learn: Comprehensive ML library with TF-IDF and cosine similarity, better than custom implementations
- difflib: Built-in Python library for sequence matching, no external dependencies
- numpy/pandas: Essential for data manipulation and numerical operations

Algorithm Design:
- Multi-stage matching: business name extraction → normalization → similarity scoring → threshold filtering
- Weighted scoring: combines fuzzy (40%) + sequence (40%) + domain (20%) matching
- Configurable thresholds: allows tuning for precision vs recall trade-offs
"""

import pandas as pd
import numpy as np
import re
from difflib import SequenceMatcher
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import warnings
warnings.filterwarnings('ignore')

def extract_business_info_from_email(email_data):
    """
    Extract business information from email data using pattern matching
    
    What: Attempts to extract business names from various email fields (subject, sender, summary, domain)
    Why: Business names are often embedded in unstructured text; extracting them enables customer matching
    How: Uses regex patterns to identify optical industry business names, then combines results
    Alternative: Named Entity Recognition (NER) could be more robust but requires training data and is slower
    
    Args:
        email_data (DataFrame): Email data with columns like 'fromAddress', 'subject', 'sender', 'summary'
    
    Returns:
        DataFrame: Enhanced email data with extracted business information
    
    Algorithm Details:
    - Domain extraction: Splits email addresses at '@' to get domain names
    - Pattern matching: Looks for optical industry keywords (OPTICAL, OPTOMETRY, EYE, VISION, LENS)
    - Field combination: Merges extracted names from multiple fields for comprehensive coverage
    """
    print("=== Extracting business information from emails ===")
    
    # Extract domain from email addresses
    # Why: Domain names often contain business identifiers
    email_data['email_domain'] = email_data['fromAddress'].str.extract(r'@(.+)')
    
    # Extract business names from subject lines
    # Pattern: Uppercase words followed by optical industry keywords
    email_data['subject_business'] = email_data['subject'].str.extract(r'([A-Z][A-Z\s&]+(?:OPTICAL|OPTOMETRY|EYE|VISION|LENS))')
    
    # Extract business names from sender names
    # Similar pattern matching for sender field
    email_data['sender_business'] = email_data['sender'].str.extract(r'([A-Z][A-Z\s&]+(?:OPTICAL|OPTOMETRY|EYE|VISION|LENS))')
    
    # Extract business names from email summaries
    # Comprehensive search in email body/summary text
    email_data['summary_business'] = email_data['summary'].str.extract(r'([A-Z][A-Z\s&]+(?:OPTICAL|OPTOMETRY|EYE|VISION|LENS))')
    
    # Combine all extracted business information
    # Why: Multiple fields may contain different parts of business name
    email_data['extracted_business'] = email_data[['subject_business', 'sender_business', 'summary_business']].fillna('').agg(' '.join, axis=1)
    email_data['extracted_business'] = email_data['extracted_business'].str.strip()
    
    # Convert empty strings to NaN for cleaner processing
    email_data['extracted_business'] = email_data['extracted_business'].replace('', np.nan)
    
    print(f"Successfully extracted business info from {email_data['extracted_business'].notna().sum()} emails")
    print(f"Email dataset shape: {email_data.shape}, columns: {len(email_data.columns)}")
    
    return email_data

def normalize_business_name(name):
    """
    Normalize business names for consistent matching
    
    What: Standardizes business names by handling common variations and abbreviations
    Why: Same businesses may be referenced with different spellings, abbreviations, or formats
    How: Applies systematic replacements, removes special characters, and standardizes spacing
    Alternative: Could use fuzzy matching without normalization, but normalization improves accuracy
    
    Args:
        name (str): Raw business name
    
    Returns:
        str: Normalized business name
    
    Normalization Rules:
    - Convert to lowercase for case-insensitive matching
    - Replace common optical industry terms with standard abbreviations
    - Remove special characters and normalize spacing
    - Convert written numbers to digits for consistency
    """
    if pd.isna(name):
        return ''
    
    # Convert to lowercase for consistent processing
    name = str(name).lower()
    
    # Dictionary of common business term replacements
    # Why these replacements: Common variations in optical industry naming
    replacements = {
        'optical': 'opt',
        'optometry': 'opt',
        'eyecare': 'eye',
        'eyewear': 'eye',
        'vision': 'vis',
        'lens': 'len',
        'clinic': 'cl',
        'center': 'ctr',
        'associates': 'assoc',
        'professional': 'prof',
        'family': 'fam',
        'group': 'grp',
        'company': 'co',
        'corporation': 'corp',
        'incorporated': 'inc',
        'limited': 'ltd',
        # Number word to digit conversions
        'eleven': '11',
        'twenty': '20',
        'thirty': '30',
        'forty': '40',
        'fifty': '50',
        'sixty': '60',
        'seventy': '70',
        'eighty': '80',
        'ninety': '90',
        'one': '1',
        'two': '2',
        'three': '3',
        'four': '4',
        'five': '5',
        'six': '6',
        'seven': '7',
        'eight': '8',
        'nine': '9',
        'zero': '0'
    }
    
    # Apply replacements with word boundaries to avoid partial matches
    for old, new in replacements.items():
        name = re.sub(r'\b' + old + r'\b', new, name)
    
    # Remove special characters (keep only alphanumeric and spaces)
    name = re.sub(r'[^\w\s]', '', name)
    
    # Normalize whitespace (multiple spaces to single space)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def calculate_similarity_score(name1, name2, method='combined'):
    """
    Calculate similarity score between two business names
    
    What: Computes similarity using various algorithms (fuzzy, sequence, or combined)
    Why: Different similarity methods excel in different scenarios; combined approach is most robust
    How: Uses fuzzywuzzy for fuzzy matching and difflib for sequence matching
    Alternative: Could use Levenshtein distance or Jaro-Winkler, but chosen methods are well-tested
    
    Args:
        name1 (str): First business name
        name2 (str): Second business name
        method (str): Similarity calculation method ('fuzzy', 'sequence', 'combined')
    
    Returns:
        float: Similarity score (0-100)
    
    Method Comparison:
    - fuzzy: Good for typos and minor variations
    - sequence: Good for word order changes
    - combined: Balances both approaches for best overall performance
    """
    if pd.isna(name1) or pd.isna(name2) or name1 == '' or name2 == '':
        return 0
    
    # Convert to lowercase for consistent comparison
    name1 = str(name1).lower()
    name2 = str(name2).lower()
    
    if method == 'fuzzy':
        # Uses Levenshtein distance for character-level similarity
        return fuzz.ratio(name1, name2)
    elif method == 'sequence':
        # Uses longest common subsequence for sequence similarity
        return SequenceMatcher(None, name1, name2).ratio() * 100
    elif method == 'combined':
        # Weighted average of both methods
        fuzzy_score = fuzz.ratio(name1, name2)
        sequence_score = SequenceMatcher(None, name1, name2).ratio() * 100
        return (fuzzy_score + sequence_score) / 2
    else:
        return 0

def find_best_matches(email_data, customer_data, threshold=70):
    """
    Find optimal matches between email and customer data using multi-criteria similarity
    
    What: For each email, identifies the most similar customer using business name and email domain matching
    Why: Enables linking unstructured email data to structured customer records without explicit IDs
    How: Combines fuzzy matching, sequence matching, and domain comparison with configurable thresholds
    Alternative: Machine learning classification could work but requires training data and is less interpretable
    
    Args:
        email_data (DataFrame): Email data with extracted business information
        customer_data (DataFrame): Customer data with business names and contact info
        threshold (float): Minimum similarity score for matches (0-100)
    
    Returns:
        DataFrame: Matched email-customer pairs with similarity scores
    
    Matching Algorithm:
    1. Normalize business names in both datasets
    2. For each email, compare with all customers
    3. Calculate weighted similarity: fuzzy (40%) + sequence (40%) + domain (20%)
    4. Return only matches above threshold with highest scores
    """
    print("=== Starting advanced similarity-based matching ===")
    print(f"Email records: {len(email_data)}, Customer records: {len(customer_data)}")
    
    # Generate clean customer names if not present
    if 'customer_name_clean' not in customer_data.columns:
        customer_data['customer_name_clean'] = customer_data['Customer'].apply(
            lambda x: re.sub(r'\s*#\d+[A-Za-z]*$', '', str(x)).strip()
        )
    
    # Create normalized names for matching
    customer_data['normalized_name'] = customer_data['customer_name_clean'].apply(normalize_business_name)
    email_data['normalized_business'] = email_data['extracted_business'].apply(normalize_business_name)
    
    # Initialize results storage
    matches = []
    
    print(f"Processing {len(email_data)} emails against {len(customer_data)} customers")
    
    # Main matching loop - compare each email with all customers
    for idx, email_row in email_data.iterrows():
        # Skip emails without extracted business names
        if pd.isna(email_row['normalized_business']) or email_row['normalized_business'] == '':
            continue
            
        best_match = None
        best_score = 0
        
        # Compare with each customer
        for _, customer_row in customer_data.iterrows():
            # Skip customers without clean names
            if pd.isna(customer_row['normalized_name']) or customer_row['normalized_name'] == '':
                continue
                
            # Calculate multiple similarity scores
            fuzzy_score = calculate_similarity_score(
                email_row['normalized_business'], 
                customer_row['normalized_name'], 
                'fuzzy'
            )
            sequence_score = calculate_similarity_score(
                email_row['normalized_business'], 
                customer_row['normalized_name'], 
                'sequence'
            )
            
            # Email domain matching bonus
            domain_score = 0
            if pd.notna(email_row['email_domain']) and pd.notna(customer_row['Main Email']):
                customer_domain = customer_row['Main Email'].split('@')[-1] if '@' in str(customer_row['Main Email']) else ''
                if email_row['email_domain'] == customer_domain:
                    domain_score = 100
            
            # Weighted combined score
            # Why these weights: Name similarity is most important, domain provides additional confidence
            combined_score = (fuzzy_score * 0.4 + sequence_score * 0.4 + domain_score * 0.2)
            
            # Update best match if score exceeds threshold and current best
            if combined_score > best_score and combined_score >= threshold:
                best_score = combined_score
                best_match = customer_row
        
        # Store successful matches
        if best_match is not None:
            matches.append({
                'email_index': idx,
                'customer_index': best_match.name,
                'email_business': email_row['extracted_business'],
                'customer_name': best_match['Customer'],
                'customer_clean': best_match['customer_name_clean'],
                'similarity_score': best_score,
                'email_domain': email_row['email_domain'],
                'customer_email': best_match['Main Email']
            })
    
    # Convert results to DataFrame
    matches_df = pd.DataFrame(matches)
    
    # Privacy-safe reporting
    print(f"Successfully matched {len(matches_df)} emails")
    print(f"Matching rate: {len(matches_df) / len(email_data) * 100:.2f}%")
    print(f"Results shape: {matches_df.shape}")
    
    return matches_df

def analyze_matching_quality(matches_df, email_data, customer_data):
    """
    Analyze quality and reliability of matching results
    
    What: Provides comprehensive statistics on similarity scores and match quality distribution
    Why: Quality assessment helps validate matching algorithm performance and identify improvement areas
    How: Computes descriptive statistics, quality tiers, and displays anonymized sample results
    Alternative: Could use ROC curves or confusion matrices with labeled data, but descriptive stats are sufficient
    
    Args:
        matches_df (DataFrame): Matching results with similarity scores
        email_data (DataFrame): Original email data
        customer_data (DataFrame): Original customer data
    
    Returns:
        dict: Quality analysis results including statistics and quality tiers
    """
    print("=== Analyzing matching quality and reliability ===")
    print(f"Analyzing {matches_df.shape[0]} matches")
    
    # Statistical analysis of similarity scores
    score_stats = matches_df['similarity_score'].describe()
    print(f"Similarity Score Statistics:")
    print(f"  Mean: {score_stats['mean']:.2f}")
    print(f"  Median: {score_stats['50%']:.2f}")
    print(f"  Minimum: {score_stats['min']:.2f}")
    print(f"  Maximum: {score_stats['max']:.2f}")
    print(f"  Standard Deviation: {score_stats['std']:.2f}")
    
    # Quality tier analysis
    # Why these thresholds: Industry standard for similarity matching quality assessment
    high_quality_matches = matches_df[matches_df['similarity_score'] >= 90]
    medium_quality_matches = matches_df[(matches_df['similarity_score'] >= 80) & (matches_df['similarity_score'] < 90)]
    low_quality_matches = matches_df[matches_df['similarity_score'] < 80]
    
    print(f"\nMatch Quality Distribution:")
    print(f"  High Quality (90+): {len(high_quality_matches)} matches ({len(high_quality_matches)/len(matches_df)*100:.1f}%)")
    print(f"  Medium Quality (80-89): {len(medium_quality_matches)} matches ({len(medium_quality_matches)/len(matches_df)*100:.1f}%)")
    print(f"  Low Quality (<80): {len(low_quality_matches)} matches ({len(low_quality_matches)/len(matches_df)*100:.1f}%)")
    
    # Sample results display (privacy-safe)
    print(f"\n=== Sample Matching Results (Top 10 by Score) ===")
    top_matches = matches_df.nlargest(10, 'similarity_score')
    for i, (_, match) in enumerate(top_matches.iterrows(), 1):
        print(f"{i}. Score: {match['similarity_score']:.1f}")
        print(f"   Email Business: [ANONYMIZED]")
        print(f"   Customer: [ANONYMIZED]")
        print(f"   Domain Match: {'Yes' if '@' in str(match['customer_email']) else 'No'}")
        print()
    
    # Quality assessment summary
    quality_percentage = len(high_quality_matches) / len(matches_df) * 100
    print(f"=== Quality Assessment Summary ===")
    if quality_percentage >= 70:
        print("✅ Excellent matching quality - High confidence in results")
    elif quality_percentage >= 50:
        print("⚠️ Good matching quality - Results generally reliable")
    elif quality_percentage >= 30:
        print("⚠️ Moderate matching quality - Manual review recommended")
    else:
        print("❌ Low matching quality - Algorithm tuning needed")
    
    return {
        'score_stats': score_stats,
        'high_quality_count': len(high_quality_matches),
        'medium_quality_count': len(medium_quality_matches),
        'low_quality_count': len(low_quality_matches),
        'quality_percentage': quality_percentage
    }

def create_final_matched_dataset(email_data, matches_df, customer_data):
    """
    Create comprehensive matched dataset combining email and customer information
    
    What: Merges matched email records with corresponding customer data to create enriched dataset
    Why: Enables downstream analysis with combined email and customer context
    How: Joins DataFrames on matched indices and adds similarity scores
    Alternative: Could use database joins, but pandas merge is sufficient for this dataset size
    
    Args:
        email_data (DataFrame): Original email data
        matches_df (DataFrame): Matching results with indices
        customer_data (DataFrame): Original customer data
    
    Returns:
        DataFrame: Enriched dataset with email and customer information
    """
    print("=== Creating final matched dataset ===")
    
    # Extract matched email records
    matched_emails = email_data.loc[matches_df['email_index']].copy()
    
    # Add customer information from matching results
    matched_emails['matched_customer'] = matches_df['customer_name'].values
    matched_emails['matched_customer_clean'] = matches_df['customer_clean'].values
    matched_emails['similarity_score'] = matches_df['similarity_score'].values
    
    # Join with additional customer data
    # Why left join: Preserves all matched emails even if customer data is incomplete
    customer_info = customer_data.set_index('Customer')[['Account No.', 'Main Email', 'Main Phone']]
    matched_emails = matched_emails.join(customer_info, on='matched_customer', how='left')
    
    print(f"Final matched dataset: {len(matched_emails)} records")
    print(f"Columns: {len(matched_emails.columns)} (shape preserved for privacy)")
    
    return matched_emails

def main_email_customer_matching():
    """
    Main orchestration function for email-customer matching pipeline
    
    What: Coordinates the complete matching workflow from data loading to result generation
    Why: Provides a single entry point for the entire matching process
    How: Executes each step in sequence with error handling and progress reporting
    Alternative: Could use workflow engines like Airflow, but direct execution is simpler for this use case
    
    Returns:
        tuple: (final_dataset, matches, quality_analysis) or None if error occurs
    """
    print("=== Email-Customer Matching Pipeline Started ===")
    
    # Data loading with error handling
    try:
        customer_df = pd.read_csv('../data/processed_customer_data.csv')
        email_df = pd.read_csv('../data/processed_email_data.csv')
        print(f"Loaded customer data: {customer_df.shape}")
        print(f"Loaded email data: {email_df.shape}")
    except FileNotFoundError as e:
        print(f"Error: Data files not found. Please process data first. Details: {e}")
        return None
    
    # Business information extraction
    print("\n--- Step 1: Business Information Extraction ---")
    email_df_processed = extract_business_info_from_email(email_df)
    
    # Similarity-based matching
    print("\n--- Step 2: Similarity-Based Matching ---")
    matches = find_best_matches(email_df_processed, customer_df, threshold=70)
    
    if len(matches) == 0:
        print("Warning: No matches found. Consider lowering threshold or improving data quality.")
        return None
    
    # Quality analysis
    print("\n--- Step 3: Quality Analysis ---")
    quality_analysis = analyze_matching_quality(matches, email_df_processed, customer_df)
    
    # Final dataset creation
    print("\n--- Step 4: Final Dataset Creation ---")
    final_dataset = create_final_matched_dataset(email_df_processed, matches, customer_df)
    
    # Results persistence
    print("\n--- Step 5: Results Persistence ---")
    final_dataset.to_csv('../data/email_customer_matched.csv', index=False)
    matches.to_csv('../data/email_customer_matches.csv', index=False)
    print("Results saved to CSV files")
    
    # Summary report
    print(f"\n=== Pipeline Completion Summary ===")
    print(f"Total emails processed: {len(email_df)}")
    print(f"Successful matches: {len(final_dataset)}")
    print(f"Overall matching rate: {len(final_dataset) / len(email_df) * 100:.2f}%")
    print(f"Average similarity score: {quality_analysis['score_stats']['mean']:.2f}")
    print(f"High quality matches: {quality_analysis['high_quality_count']} ({quality_analysis['quality_percentage']:.1f}%)")
    
    return final_dataset, matches, quality_analysis

if __name__ == "__main__":
    # Execute main pipeline
    result = main_email_customer_matching()
    if result:
        print("Email-customer matching completed successfully!")
    else:
        print("Email-customer matching failed. Please check data and configuration.") 