import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL 연결 설정
engine = create_engine("postgresql://admin:password123@localhost:5432/optical_db")

# CSV 파일 경로 (필요 시 절대경로로 변경)
df_path = "data/cc.csv"
df2_path = "data/fi.csv"
df3_path = "data/s_by_c.csv"
email_df_path = "data/email_df.csv"

# CSV 파일 로드
df = pd.read_csv(df_path)
df2 = pd.read_csv(df2_path)
df3 = pd.read_csv(df3_path)
email_df = pd.read_csv(email_df_path)

# 불필요한 인덱스 컬럼 제거
for dataframe in [df, df2, df3, email_df]:
    if 'Unnamed: 0' in dataframe.columns:
        dataframe.drop(columns=['Unnamed: 0'], inplace=True)

# PostgreSQL로 테이블 삽입
df.to_sql("customers", engine, if_exists="append", index=False)
df2.to_sql("items", engine, if_exists="append", index=False)
df3.to_sql("orders", engine, if_exists="append", index=False)
email_df.to_sql("emails", engine, if_exists="append", index=False)

print("✅ 데이터베이스에 CSV 데이터 삽입 완료")