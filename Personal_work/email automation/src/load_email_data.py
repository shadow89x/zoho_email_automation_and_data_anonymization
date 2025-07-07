import pandas as pd
import json

# Load zoho_emails.json file as pandas DataFrame
print("Reading JSON file...")

# Read JSON file
with open('zoho_emails.json', 'r', encoding='utf-8') as file:
    email_data = json.load(file)

print(f"JSON data type: {type(email_data)}")
print(f"Total number of emails: {len(email_data)}")

# Convert JSON data to DataFrame
email_df = pd.json_normalize(email_data)
print(f"DataFrame shape: {email_df.shape}")
print(f"Columns: {list(email_df.columns)}")

print("\n=== Email DataFrame Information ===")
print(f"email_df: {email_df.shape} - Columns: {list(email_df.columns)}")

print("\n=== First 5 rows of email_df ===")
print(email_df.head())

print("\n=== Basic Information of email_df ===")
print(email_df.info())

print("\n=== Data Types in email_df ===")
print(email_df.dtypes)

print("\n=== Missing Values in email_df ===")
print(email_df.isnull().sum())

# Check sample data for key columns
print("\n=== Sample Data for Key Columns ===")
if 'fromAddress' in email_df.columns:
    print("Sample fromAddress:")
    print(email_df['fromAddress'].head())
    
if 'toAddress' in email_df.columns:
    print("\nSample toAddress:")
    print(email_df['toAddress'].head())
    
if 'sender' in email_df.columns:
    print("\nSample sender:")
    print(email_df['sender'].head())
    
if 'receivedTime' in email_df.columns:
    print("\nSample receivedTime:")
    print(email_df['receivedTime'].head())

# Check number of unique senders
if 'fromAddress' in email_df.columns:
    unique_senders = email_df['fromAddress'].nunique()
    print(f"\nNumber of unique senders: {unique_senders}")

# Check number of unique recipients
if 'toAddress' in email_df.columns:
    unique_recipients = email_df['toAddress'].nunique()
    print(f"Number of unique recipients: {unique_recipients}") 