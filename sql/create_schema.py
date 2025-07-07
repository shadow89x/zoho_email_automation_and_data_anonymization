from sqlalchemy import create_engine

# 연결 문자열 확인 (DB명도 정확하게!)
engine = create_engine("postgresql://admin:password123@localhost:5432/optical_db")

# 정확한 schema.sql 경로
with engine.connect() as conn:
    with open("sql/schema.sql", "r", encoding="utf-8") as f:
        schema_sql = f.read()
        conn.exec_driver_sql(schema_sql)  # 핵심: raw SQL 실행

print("✅ 테이블 생성 완료")
