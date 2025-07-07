import pandas as pd
import json
import datetime

# Load zoho_emails.json file as pandas DataFrame
print("Reading JSON file...")

# Read JSON file
with open('zoho_emails.json', 'r', encoding='utf-8') as file:
    email_data = json.load(file)

# Convert JSON data to DataFrame
email_df = pd.json_normalize(email_data)

print(f"Original data: {email_df.shape}")

# Split sentDateInGMT into date and time
print("\n=== Starting date and time separation ===")

# Convert millisecond timestamp to date
email_df['sentDate'] = pd.to_datetime(email_df['sentDateInGMT'].astype(float), unit='ms')

# Split date and time
email_df['sent_date'] = email_df['sentDate'].dt.date
email_df['sent_time'] = email_df['sentDate'].dt.time

# Split receivedTime in the same way
email_df['receivedDate'] = pd.to_datetime(email_df['receivedTime'].astype(float), unit='ms')
email_df['received_date'] = email_df['receivedDate'].dt.date
email_df['received_time'] = email_df['receivedDate'].dt.time

print("=== Date and time separation results ===")
print("Converted columns:")
print(f"- sent_date: {email_df['sent_date'].dtype}")
print(f"- sent_time: {email_df['sent_time'].dtype}")
print(f"- received_date: {email_df['received_date'].dtype}")
print(f"- received_time: {email_df['received_time'].dtype}")

print("\n=== Sample of separated date and time ===")
print("Sent date/time:")
print(email_df[['sentDateInGMT', 'sent_date', 'sent_time']].head())

print("\nReceived date/time:")
print(email_df[['receivedTime', 'received_date', 'received_time']].head())

# Check email count by date
print("\n=== Email count by date ===")
daily_emails = email_df.groupby('sent_date').size()
print(f"Total number of dates: {len(daily_emails)}")
print(f"Average daily emails: {daily_emails.mean():.1f}")
print(f"Maximum daily emails: {daily_emails.max()}")

print("\n=== Email count for last 10 days ===")
recent_emails = daily_emails.sort_index(ascending=False).head(10)
print(recent_emails)

# Check email count by hour
print("\n=== Email count by hour ===")
hourly_emails = email_df.groupby(email_df['sent_time'].apply(lambda x: x.hour)).size()
print("Email count by hour (0-23):")
print(hourly_emails)

# Hour with most emails
most_active_hour = hourly_emails.idxmax()
print(f"\nHour with most emails: {most_active_hour} ({hourly_emails.max()} emails)")

# Check email count by weekday
print("\n=== Email count by weekday ===")
weekday_emails = email_df.groupby(email_df['sentDate'].dt.day_name()).size()
print(weekday_emails)

# Check email count by month
print("\n=== Email count by month ===")
monthly_emails = email_df.groupby(email_df['sentDate'].dt.to_period('M')).size()
print("Last 12 months:")
print(monthly_emails.tail(12))

# Check email count by year
print("\n=== Email count by year ===")
yearly_emails = email_df.groupby(email_df['sentDate'].dt.year).size()
print(yearly_emails)

# Save converted DataFrame
print("\n=== Saving converted data ===")
email_df.to_csv('emails_split_datetime.csv', index=False, encoding='utf-8')
print("Converted data saved to 'emails_split_datetime.csv' file.")

# Key statistics summary
print("\n=== Data summary ===")
print(f"Total emails: {len(email_df)}")
print(f"Unique senders: {email_df['fromAddress'].nunique()}")
print(f"Emails with attachments: {email_df['hasAttachment'].value_counts()}")
print(f"Unread emails: {(email_df['status'] == '0').sum()}")
print(f"Read emails: {(email_df['status'] == '1').sum()}")

# Check date range
print(f"\n=== Date range ===")
print(f"Oldest email: {email_df['sent_date'].min()}")
print(f"Most recent email: {email_df['sent_date'].max()}")
print(f"Total period: {email_df['sent_date'].max() - email_df['sent_date'].min()}") 