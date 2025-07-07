import pandas as pd
import json
import datetime

# zoho_emails.json 파일을 pandas DataFrame으로 불러오기
print("JSON 파일을 읽는 중...")

# JSON 파일 읽기
with open('zoho_emails.json', 'r', encoding='utf-8') as file:
    email_data = json.load(file)

# JSON 데이터를 DataFrame으로 변환
email_df = pd.json_normalize(email_data)

print(f"원본 데이터: {email_df.shape}")

# sentDateInGMT를 날짜 형식으로 변환
print("\n=== 날짜 변환 시작 ===")

# 밀리초 타임스탬프를 날짜로 변환
email_df['sentDate'] = pd.to_datetime(email_df['sentDateInGMT'].astype(float), unit='ms')

# receivedTime도 날짜로 변환 (밀리초 단위)
email_df['receivedDate'] = pd.to_datetime(email_df['receivedTime'].astype(float), unit='ms')

print("=== 날짜 변환 결과 ===")
print("변환된 컬럼들:")
print(f"- sentDate: {email_df['sentDate'].dtype}")
print(f"- receivedDate: {email_df['receivedDate'].dtype}")

print("\n=== 변환된 날짜 샘플 ===")
print("sentDate 샘플:")
print(email_df[['sentDateInGMT', 'sentDate']].head())

print("\nreceivedDate 샘플:")
print(email_df[['receivedTime', 'receivedDate']].head())

print("\n=== 날짜 범위 ===")
print(f"가장 오래된 이메일: {email_df['sentDate'].min()}")
print(f"가장 최근 이메일: {email_df['sentDate'].max()}")
print(f"총 기간: {email_df['sentDate'].max() - email_df['sentDate'].min()}")

# 날짜별 이메일 개수 확인
print("\n=== 날짜별 이메일 개수 ===")
daily_emails = email_df.groupby(email_df['sentDate'].dt.date).size()
print(f"총 날짜 수: {len(daily_emails)}")
print(f"평균 일일 이메일 수: {daily_emails.mean():.1f}")
print(f"최대 일일 이메일 수: {daily_emails.max()}")

print("\n=== 최근 10일간 이메일 개수 ===")
recent_emails = daily_emails.sort_index(ascending=False).head(10)
print(recent_emails)

# 월별 이메일 개수 확인
print("\n=== 월별 이메일 개수 ===")
monthly_emails = email_df.groupby(email_df['sentDate'].dt.to_period('M')).size()
print("최근 12개월:")
print(monthly_emails.tail(12))

# 연도별 이메일 개수 확인
print("\n=== 연도별 이메일 개수 ===")
yearly_emails = email_df.groupby(email_df['sentDate'].dt.year).size()
print(yearly_emails)

# 요일별 이메일 개수 확인
print("\n=== 요일별 이메일 개수 ===")
weekday_emails = email_df.groupby(email_df['sentDate'].dt.day_name()).size()
print(weekday_emails)

# 시간대별 이메일 개수 확인
print("\n=== 시간대별 이메일 개수 ===")
hourly_emails = email_df.groupby(email_df['sentDate'].dt.hour).size()
print("시간대별 이메일 개수 (0-23시):")
print(hourly_emails)

# 변환된 DataFrame 저장 (선택사항)
print("\n=== 변환된 데이터 저장 ===")
email_df.to_csv('emails_with_dates.csv', index=False, encoding='utf-8')
print("변환된 데이터가 'emails_with_dates.csv' 파일로 저장되었습니다.")

# 주요 통계 요약
print("\n=== 데이터 요약 ===")
print(f"총 이메일 수: {len(email_df)}")
print(f"고유한 발신자 수: {email_df['fromAddress'].nunique()}")
print(f"첨부파일이 있는 이메일: {email_df['hasAttachment'].value_counts()}")
print(f"읽지 않은 이메일: {(email_df['status'] == '0').sum()}")
print(f"읽은 이메일: {(email_df['status'] == '1').sum()}") 